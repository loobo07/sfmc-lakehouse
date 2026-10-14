# sfmc-lakehouse

Salesforce Marketing Cloud **Engagement** campaign data → **Amazon S3 Tables** (managed Iceberg), in us-east-1.

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
```sh
# One-off backfill: SFMC keeps about 6 months of tracking data
aws glue start-job-run --job-name sfmc-to-s3tables --arguments '{"--LOOKBACK_DAYS":"180"}'
```
After that, the `sfmc-to-s3tables-daily` trigger runs the job daily at 06:00 UTC. Failed and
timed-out runs send an email through SNS.

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

## Development
```sh
uv sync
uv run pytest
```
