# Core Data Lake Bucket
resource "aws_s3_bucket" "lakehouse_bucket" {
  bucket        = "${var.project_name}-lakehouse-${var.environment}-${var.bucket_suffix}"
  force_destroy = true # Allows clean tear-down during testing
}

# 1. HIPAA Compliance: Enable Server-Side Encryption at Rest
resource "aws_s3_bucket_server_side_encryption_configuration" "s3_encryption" {
  bucket = aws_s3_bucket.lakehouse_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# 2. Block all Public Access
resource "aws_s3_bucket_public_access_block" "public_block" {
  bucket                  = aws_s3_bucket.lakehouse_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 3. HIPAA Compliance: Enforce In-Transit Encryption (Require HTTPS/TLS)
resource "aws_s3_bucket_policy" "enforce_tls" {
  bucket = aws_s3_bucket.lakehouse_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnforceTLSRequestsOnly"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.lakehouse_bucket.arn,
          "${aws_s3_bucket.lakehouse_bucket.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

# Create Bronze, Silver, Gold Prefixes
resource "aws_s3_object" "bronze_prefix" {
  bucket = aws_s3_bucket.lakehouse_bucket.id
  key    = "bronze/"
}

resource "aws_s3_object" "silver_prefix" {
  bucket = aws_s3_bucket.lakehouse_bucket.id
  key    = "silver/"
}

resource "aws_s3_object" "gold_prefix" {
  bucket = aws_s3_bucket.lakehouse_bucket.id
  key    = "gold/"
}

# Central Glue Data Catalog Database
resource "aws_glue_catalog_database" "carematrix_db" {
  name        = "${var.project_name}_${var.environment}_db"
  description = "Glue Catalog Database for CareMatrix Healthcare Analytics"
}