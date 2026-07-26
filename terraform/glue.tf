data "aws_iam_role" "glue_role" {
  name = "waffen-glue-role"
}

resource "aws_glue_catalog_database" "hdmf_db" {
  name = "waffen_migration_db"
}

resource "aws_glue_crawler" "passed_data_crawler" {
  name          = "hdmf-passed-crawler"
  role          = data.aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.hdmf_db.name

  s3_target {
    path = "s3://waffen-migration-pipeline-bucket/passed/"
  }
}


resource "aws_glue_job" "aggregation_job" {
  name     = "waffen-aggregation-job"
  role_arn = data.aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://waffen-migration-pipeline-bucket/scripts/glue_job.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}
