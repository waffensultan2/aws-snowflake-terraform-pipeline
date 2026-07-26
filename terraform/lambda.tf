# Reference the S3 read/write role that we created manually
data "aws_iam_role" "pipeline_lambda_role" {
  name = "new-waffen-migration-pipeline-lambda-s3-read-write-role"
}

# The Lambda function itself
resource "aws_lambda_function" "pipeline_lambda" {
  function_name = "waffen-pipeline-lambda"

  filename         = "../lambda/lambda.zip"
  source_code_hash = filebase64sha256("../lambda/lambda.zip")

  handler = "lambda_function.lambda_handler"
  runtime = "python3.9"

  role    = data.aws_iam_role.pipeline_lambda_role.arn
  timeout = 30
}
