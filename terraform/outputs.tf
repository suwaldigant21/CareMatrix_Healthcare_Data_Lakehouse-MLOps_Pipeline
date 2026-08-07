output "s3_bucket_name" {
  value = aws_s3_bucket.lakehouse_bucket.id
}

output "glue_database_name" {
  value = aws_glue_catalog_database.carematrix_db.name
}

output "pipeline_access_key_id" {
  value     = aws_iam_access_key.pipeline_user_key.id
  sensitive = true
}

output "pipeline_secret_access_key" {
  value     = aws_iam_access_key.pipeline_user_key.secret
  sensitive = true
}