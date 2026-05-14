import json
from unittest.mock import Mock, patch

from src.jobs.count_and_save_in_csv import run_job


@patch("src.jobs.count_and_save_in_csv.boto3.client")
@patch("src.jobs.count_and_save_in_csv.getResolvedOptions")
@patch("src.jobs.count_and_save_in_csv.SparkContext")
@patch("src.jobs.count_and_save_in_csv.GlueContext")  # 👈 IMPORTANT: patch direct import in module
def test_run_job_success(
    mock_glue,
    mock_spark,
    mock_args,
    mock_boto
):

    # -------------------------
    # Args Glue
    # -------------------------
    mock_args.return_value = {
        "CONFIG_PATH": "s3://my-bucket/config.json"
    }

    # -------------------------
    # S3 mock
    # -------------------------
    s3 = Mock()
    mock_boto.return_value = s3

    s3.get_object.return_value = {
        "Body": Mock(
            read=Mock(
                return_value=json.dumps({
                    "OUTPUT_BUCKET_NAME": "output-bucket"
                }).encode("utf-8")
            )
        )
    }

    # -------------------------
    # Spark mock
    # -------------------------
    df = Mock()
    writer = Mock()

    df.coalesce.return_value = df
    df.write = writer

    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.csv.return_value = None

    spark = Mock()
    spark.createDataFrame.return_value = df

    glue_instance = Mock()
    glue_instance.spark_session = spark

    mock_glue.return_value = glue_instance
    mock_spark.return_value = Mock()

    # -------------------------
    # RUN
    # -------------------------
    run_job()

    # -------------------------
    # ASSERTIONS
    # -------------------------
    s3.get_object.assert_called_once()
    spark.createDataFrame.assert_called_once()
    writer.csv.assert_called_once()

    args, _ = writer.csv.call_args
    assert "s3://output-bucket/output/" in args[0]
