variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "us-east-1"
}

variable "sfmc_connection_name" {
  description = "Name of the Glue SALESFORCEMARKETINGCLOUD connection created in the console."
  type        = string
}

variable "sfmc_secret_arn" {
  description = "ARN of the Secrets Manager secret the Glue connection uses for the SFMC client ID/secret."
  type        = string
}

variable "soap_instance_url" {
  description = "SFMC SOAP base URL, e.g. https://<subdomain>.soap.marketingcloudapis.com/."
  type        = string
}

variable "alert_email" {
  description = "Email address subscribed to Glue job failure alerts."
  type        = string
}

variable "entities" {
  description = "Comma-separated target tables the job loads (keys of ENTITIES in the job)."
  type        = string
  default     = "sent_event,open_event,click_event,bounce_event,unsub_event,not_sent_event,send,subscriber,list,email"
}

variable "schedule" {
  description = "Glue trigger schedule (UTC)."
  type        = string
  default     = "cron(0 6 * * ? *)"
}
