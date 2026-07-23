resource "aws_s3_bucket" "migration_pipeline_bucket" {
    bucket = "waffen-migration-pipeline-bucket"
}

# Create our folders which will contain: raw, and DQ passed/rejected data
resource "aws_s3_object" "bucket_folders" {
    bucket = aws_s3_bucket.migration_pipeline_bucket.id

    for_each = toset(local.s3_bucket_folders)
    key = each.value
    content_type = "application/x-directory"
}

# S3 bucket notification for Lambda (on "raw/"); temporarily disabled
resource "aws_s3_bucket_notification" "bucket_notification" {
    bucket = aws_s3_bucket.migration_pipeline_bucket.id
}
