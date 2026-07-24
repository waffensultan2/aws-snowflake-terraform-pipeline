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

resource "aws_lambda_permission" "allow_s3" {
    statement_id = "AllowExecutionFromS3"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.pipeline_lambda.function_name
    principal = "s3.amazonaws.com"
    source_arn = aws_s3_bucket.migration_pipeline_bucket.arn
}

# S3 bucket notification for Lambda (on "raw/"); temporarily disabled
resource "aws_s3_bucket_notification" "bucket_notification" {
    bucket = aws_s3_bucket.migration_pipeline_bucket.id

    lambda_function {
        lambda_function_arn = aws_lambda_function.pipeline_lambda.arn
        events = ["s3:ObjectCreated:*"]
        filter_prefix = "raw/"
    }

    depends_on = [aws_lambda_permission.allow_s3]
}
