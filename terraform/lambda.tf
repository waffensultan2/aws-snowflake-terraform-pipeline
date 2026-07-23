# Define the Lambda Execution Role and its Assume Role Trust Policy
resource "aws_iam_role" "pipeline_lambda_role" {
    name = "waffen-migration-pipeline-lambda-s3-read-write-role"

    assume_role_policy = jsonencode({
        Version: "2012-10-17"
        Statement = [
            {
                Action = "sts:AssumeRole"
                Effect = "Allow"
                Principal = {
                    Service = "lambda.amazonaws.com"
                }
            }
        ]
    })
}

# Define the Read/Write S3 Permissions Policy Document
data "aws_iam_policy_document" "lambda_s3_rw" {
    # Object-level actions (Read and write permissions)
    statement {
        effect = "Allow"
        actions = [
            "s3:GetObject",
            "s3:PutObject",
        ]
        resources = ["${aws_s3_bucket.migration_pipeline_bucket.arn}/*"]
    }
}

# Inline attach the S3 Policy Document directly to lambda role
resource "aws_iam_role_policy" "lambda_s3_attachment" {
    name = "waffen-lambda-s3-read-write-policy"
    role = aws_iam_role.pipeline_lambda_role.id
    policy = data.aws_iam_policy_document.lambda_s3_rw.json
}

# Allows us to see logs in CloudWatch Logs
resource "aws_iam_role_policy_attachment" "lambda_logs" {
    role = aws_iam_role.pipeline_lambda_role.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# The Lambda function itself
resource "aws_lambda_function" "pipeline_lambda" {
    function_name = "waffen-pipeline-lambda"

    filename = "../lambda/lambda.zip"
    source_code_hash = filebase64sha256("../lambda/lambda.zip")

    handler = "lambda_function.lambda_handler"
    runtime = "python3.9"

    role = aws_iam_role.pipeline_lambda_role.arn
    timeout = 30
}
