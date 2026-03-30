
# ----------------------------
# IAM ROLE (Trust Policy)
# ----------------------------
resource "aws_iam_role" "snowflake_s3_role" {
  name = "snowflake_s3_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          AWS = var.snowflake_iam_user_arn
        },
        Action = "sts:AssumeRole",
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.snowflake_external_id
          }
        }
      }
    ]
  })
}

# ----------------------------
# S3 ACCESS POLICY
# ----------------------------
resource "aws_iam_policy" "snowflake_s3_policy" {
  name = "snowflake_s3_access_policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowReadObjects",
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ],
        Resource = "arn:aws:s3:::${var.bucket_name}/raw/*"
      },
      {
        Sid      = "AllowListBucket",
        Effect   = "Allow",
        Action   = "s3:ListBucket",
        Resource = "arn:aws:s3:::${var.bucket_name}"
      }
    ]
  })
}

# ----------------------------
# ATTACH POLICY TO ROLE
# ----------------------------
resource "aws_iam_role_policy_attachment" "attach_policy" {
  role       = aws_iam_role.snowflake_s3_role.name
  policy_arn = aws_iam_policy.snowflake_s3_policy.arn
}