# Command to zip your python code
Compress-Archive -Path "lambda_function.py" -DestinationPath "lambda.zip"

# Delete role manually
aws iam delete-role --role-name waffen-migration-pipeline-lambda-s3-read-write-role
