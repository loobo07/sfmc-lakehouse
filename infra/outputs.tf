output "table_bucket_arn" {
  value = aws_s3tables_table_bucket.sfmc.arn
}

output "glue_job_name" {
  value = aws_glue_job.sfmc.name
}

output "glue_job_role_arn" {
  value = aws_iam_role.glue_job.arn
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
