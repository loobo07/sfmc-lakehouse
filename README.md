# sfmc-lakehouse

Salesforce Marketing Cloud **Engagement** campaign data → **Amazon S3 Tables** (managed Iceberg), in us-east-1.

## Status: parked 2026-10-26

The code is complete and nothing is deployed. To pick it up again, do these in order:

1. Confirm the SFMC product is Engagement. If it's Account Engagement (Pardot) or core
   Salesforce, use Glue zero-ETL instead of this job.
2. Get an AWS identity that can create Glue, S3 Tables, S3, IAM, SNS and EventBridge resources.
   The local `ghd_test` user can't.
3. Follow [Setup](#setup), review `terraform plan`, then apply.
4. Work through [Verification](#verification). Timing a full day at production volume decides
   whether this approach can keep up or you need SFMC exports instead.

Deferred minor review items:
- Column names aren't quoted in the generated SQL.
- Nothing detects two source fields that map to the same column name.
- Rows with an unparseable `event_date` are dropped silently.
- Dimension upserts rewrite every row on every run.
- `s3tables:*` is broader than the job needs.
- The failure alert doesn't fire for STOPPED or ERROR runs.
- Tables the job creates block `terraform destroy`.

An AWS Glue 5.0 job reads SFMC entities through Glue's native `salesforcemarketingcloud`
connector and MERGEs them into Iceberg tables in the `sfmc-analytics` table bucket
(namespace `sfmc`) via the S3 Tables Iceberg REST endpoint. Event tables load an
overlapping incremental window (`LOOKBACK_DAYS`, default 2), and dimension tables are
upserted in full. Reruns are idempotent.

| Table | Source entity | Merge key | Mode |
|---|---|---|---|
| `sent_event`, `open_event`, `bounce_event`, `unsub_event`, `not_sent_event` | `*Event` | `send_id, subscriber_key, event_date, event_type` | insert-only, partitioned by `day(event_date)` |
| `click_event` | `ClickEvent` | the same plus `url` | insert-only, partitioned by `day(event_date)` |
| `send`, `subscriber`, `list`, `email` | `Send`, `Subscriber`, `List`, `Email` | `id` | upsert |

> Glue zero-ETL doesn't support Marketing Cloud *Engagement*. If your source turns out to
> be **Account Engagement (Pardot)** or core Salesforce, use Glue zero-ETL → S3 Tables instead
> of this job. The connector also has no Complaint entity. If you need complaints, export
> `_Complaint` from SFMC to S3.

## Layout

```
glue/sfmc_to_s3tables.py   Glue job (pure helpers at module level, Spark code below)
tests/                     pytest for the pure helpers
infra/                     Terraform: table bucket, namespace, artifacts bucket, IAM, Glue job, trigger, alerts
```

## Setup

### 1. Manual prerequisites
1. **Confirm the SFMC product** is Marketing Cloud Engagement (see the note above).
2. **In SFMC**, create an Installed Package with a server-to-server API integration that has read
   access to Email, Tracking Events, Subscribers, Lists and Data Extensions. Note the client
   ID/secret and the SOAP base URL (`https://<subdomain>.soap.marketingcloudapis.com/`).
3. **Create the Glue connection** in the AWS console: us-east-1 → Glue → Data connections →
   Salesforce Marketing Cloud, using OAuth2 client credentials. The console stores the client
   ID/secret in Secrets Manager. Note the connection name and the secret's ARN.

### 2. Deploy
```sh
cd infra
cp terraform.tfvars.example terraform.tfvars   # fill in; gitignored
terraform init
terraform plan -out=tfplan                     # review before applying
terraform apply tfplan
```
After the first apply:
- Confirm the SNS email subscription from the message AWS sends.
- In the S3 console, enable the table bucket's **AWS analytics services integration** so
  Athena can query `s3tablescatalog/sfmc-analytics`.

### 3. First run and backfill
Start with a one-day run, then check it against the verification steps below. SFMC keeps
about 6 months of tracking data. The scheduled job is sized for a 2-day window
(2 × G.1X, 4 connector partitions, 120-minute timeout), so the 180-day backfill needs more
capacity. Override the sizing for that run and load one entity at a time:

```sh
aws glue start-job-run --job-name sfmc-to-s3tables \
  --timeout 1440 --worker-type G.1X --number-of-workers 10 \
  --arguments '{"--ENTITIES":"sent_event","--LOOKBACK_DAYS":"180","--NUM_PARTITIONS":"60"}'
# Repeat for open_event, click_event, bounce_event, unsub_event, not_sent_event,
# and then for the dimension tables (send,subscriber,list,email), which are always full reads.
```
After that, the `sfmc-to-s3tables-daily` trigger runs the job daily at 06:00 UTC
(`aws glue get-trigger --name sfmc-to-s3tables-daily` should show `ACTIVATED`). Failed
and timed-out runs send an email through SNS.

## Verification
- Load one historical day, then compare `count(*)` per event table against SFMC Tracking/Data
  View counts for that day.
- Rerun the same day. Counts must not change (idempotent MERGE).
- Query from Athena through the `s3tablescatalog/sfmc-analytics` catalog.
- Time a full day at production volume. If SOAP throughput can't keep up, fall back to SFMC
  Automation Studio exports to S3.

### Risks to check on the first run
- That the connector's `LOWER_BOUND`/`UPPER_BOUND` really limit the read. The job also filters
  by date after reading, as a safety net.
- The SOAP field names the connector returns. The merge keys assume `SendID`,
  `SubscriberKey`, `EventDate`, `EventType` and `URL`.
- The Iceberg REST catalog settings on Glue 5.0 (`--conf` in `infra/main.tf`).
- The time zone of `EventDate`. SFMC SOAP returns Central Time by default. If the values carry
  no UTC offset, the window bounds are shifted by about 6 hours. The 2-day lookback absorbs this
  for daily loads, but day-level count comparisons will be off.
- The connector `ENTITY_NAME` strings (`SentEvent`, …). The docs only show display names.
- Whether the connection's secret uses a customer-managed KMS key. If it does, grant the Glue
  role `kms:Decrypt` on that key.

## Development
```sh
uv sync
uv run pytest
```
