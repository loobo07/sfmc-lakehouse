data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  job_name   = "sfmc-to-s3tables"
  catalog    = "s3tables" # must match CATALOG in glue/sfmc_to_s3tables.py

  # Glue accepts one --conf argument; further settings are chained inside its value.
  iceberg_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.${local.catalog}=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.${local.catalog}.type=rest",
    "spark.sql.catalog.${local.catalog}.uri=https://s3tables.${var.region}.amazonaws.com/iceberg",
    "spark.sql.catalog.${local.catalog}.warehouse=${aws_s3tables_table_bucket.sfmc.arn}",
    "spark.sql.catalog.${local.catalog}.rest.sigv4-enabled=true",
    "spark.sql.catalog.${local.catalog}.rest.signing-name=s3tables",
    "spark.sql.catalog.${local.catalog}.rest.signing-region=${var.region}",
    "spark.sql.catalog.${local.catalog}.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.${local.catalog}.rest-metrics-reporting-enabled=false",
  ])
}

# --------------------------------------------------------------------------- S3 Tables

resource "aws_s3tables_table_bucket" "sfmc" {
  name = "sfmc-analytics"
}

resource "aws_s3tables_namespace" "sfmc" {
  namespace        = "sfmc"
  table_bucket_arn = aws_s3tables_table_bucket.sfmc.arn
}

# --------------------------------------------------------------------------- Job artifacts

resource "aws_s3_bucket" "artifacts" {
  bucket = "sfmc-lakehouse-artifacts-${local.account_id}-${var.region}"
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_object" "job_script" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue/sfmc_to_s3tables.py"
  source = "${path.module}/../glue/sfmc_to_s3tables.py"
  etag   = filemd5("${path.module}/../glue/sfmc_to_s3tables.py")
}

# --------------------------------------------------------------------------- IAM

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${local.job_name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_job" {
  statement {
    sid       = "S3TablesBucket"
    actions   = ["s3tables:*"]
    resources = [aws_s3tables_table_bucket.sfmc.arn, "${aws_s3tables_table_bucket.sfmc.arn}/*"]
  }

  statement {
    sid       = "ReadJobArtifacts"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
  }

  statement {
    sid     = "SfmcConnection"
    actions = ["glue:GetConnection", "glue:GetConnections"]
    resources = [
      "arn:${local.partition}:glue:${var.region}:${local.account_id}:catalog",
      "arn:${local.partition}:glue:${var.region}:${local.account_id}:connection/${var.sfmc_connection_name}",
    ]
  }

  statement {
    sid       = "SfmcSecret"
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [var.sfmc_secret_arn]
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "sfmc-to-s3tables"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

# --------------------------------------------------------------------------- Glue job

resource "aws_glue_job" "sfmc" {
  name              = local.job_name
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 120
  max_retries       = 0
  connections       = [var.sfmc_connection_name]

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.job_script.key}"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-language"      = "python"
    "--datalake-formats"  = "iceberg"
    "--conf"              = local.iceberg_conf
    "--CONNECTION_NAME"   = var.sfmc_connection_name
    "--SOAP_INSTANCE_URL" = var.soap_instance_url
    "--ENTITIES"          = var.entities
    "--LOOKBACK_DAYS"     = "2"
    "--NUM_PARTITIONS"    = "4"
  }

  depends_on = [aws_iam_role_policy.glue_job, aws_iam_role_policy_attachment.glue_service]
}

resource "aws_glue_trigger" "daily" {
  name     = "${local.job_name}-daily"
  type     = "SCHEDULED"
  schedule = var.schedule

  actions {
    job_name = aws_glue_job.sfmc.name
  }
}

# --------------------------------------------------------------------------- Alerting

resource "aws_sns_topic" "alerts" {
  name = "${local.job_name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_event_rule" "job_failed" {
  name        = "${local.job_name}-failed"
  description = "Glue job ${local.job_name} FAILED or TIMEOUT"

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.sfmc.name]
      state   = ["FAILED", "TIMEOUT"]
    }
  })
}

resource "aws_cloudwatch_event_target" "sns" {
  rule = aws_cloudwatch_event_rule.job_failed.name
  arn  = aws_sns_topic.alerts.arn
}

data "aws_iam_policy_document" "alerts_topic" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.job_failed.arn]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts_topic.json
}
