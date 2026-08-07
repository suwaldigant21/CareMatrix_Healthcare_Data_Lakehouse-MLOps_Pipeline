# Scoped IAM User for n8n/dbt pipeline access
resource "aws_iam_user" "pipeline_user" {
  name = "${var.project_name}-pipeline-user-${var.environment}"
}

# Policy allowing strictly required S3 operations
resource "aws_iam_policy" "s3_ingest_policy" {
  name        = "${var.project_name}-s3-ingest-policy"
  description = "Least privilege policy for CareMatrix ingestion pipeline"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.lakehouse_bucket.arn,
          "${aws_s3_bucket.lakehouse_bucket.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "attach_ingest" {
  user       = aws_iam_user.pipeline_user.name
  policy_arn = aws_iam_policy.s3_ingest_policy.arn
}

resource "aws_iam_access_key" "pipeline_user_key" {
  user = aws_iam_user.pipeline_user.name
}