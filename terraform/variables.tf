
variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "eu-west-2"
}

variable "project_name" {
  type        = string
  description = "Project name"
  default     = "supplychain360"
}

variable "instance_type" {
  type        = string
  description = "ec2 instance type"
  default     = "t3.micro"

}

variable "bucket_name" {
  type        = string
  description = "This is s3 bucket name"
  default     = "supplychain360-data-lake"
  sensitive   = true
}

variable "public_key_path" {
  type        = string
  description = "Path to SSH public key file"
}

variable "snowflake_iam_user_arn" {
  description = "Snowflake IAM User ARN from DESC INTEGRATION"
  type        = string
}

variable "snowflake_external_id" {
  description = "Snowflake External ID from DESC INTEGRATION"
  type        = string
}
