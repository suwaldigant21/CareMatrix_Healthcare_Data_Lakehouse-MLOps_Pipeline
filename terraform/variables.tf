variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "project_name" {
  type    = string
  default = "carematrix"
}

variable "bucket_suffix" {
  type        = string
  default     = "demo"
  description = "Globally-unique suffix for S3 resource names. Replace with your own suffix (e.g. <you>-<env>-001) before applying."
}