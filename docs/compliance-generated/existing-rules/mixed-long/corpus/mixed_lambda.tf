resource "aws_lambda_function" "nightly_metrics" {
  function_name = "nightly-metrics"
  role          = "arn:aws:iam::123456789012:role/lambda-role"
  handler       = "index.handler"
  runtime       = "python3.12"

  environment {
    variables = {
      LOG_LEVEL             = "INFO"
      REPORT_BUCKET         = "city-report-output"
      AWS_ACCESS_KEY_ID     = "AKIAIOSFODNN7EXAMPLE"
      AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    }
  }
}
