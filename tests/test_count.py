import sys
import json
import types
from unittest.mock import Mock, patch


# -------------------------------------------------------
# MOCK modules AWS Glue / Spark
# -------------------------------------------------------
sys.modules["pyspark"] = types.ModuleType("pyspark")
sys.modules["pyspark.context"] = types.ModuleType("pyspark.context")
sys.modules["awsglue"] = types.ModuleType("awsglue")
sys.modules["awsglue.context"] = types.ModuleType("awsglue.context")
sys.modules["awsglue.utils"] = types.ModuleType("awsglue.utils")


# -------------------------------------------------------
# IMPORT JOB
# -------------------------------------------------------
from src.jobs.count_and_save_in_csv import run_job


# -------------------------------------------------------
# TEST
# -------------------------------------------------------
@patch("src.jobs.count_and_save_in_csv.boto3.client")
@patch("awsglue.utils.getResolvedOptions")
@patch("pyspark.context.SparkContext")
@patch("awsglue.context.GlueContext")
def test_run_job_success(
    mock_glue_context,
    mock_spark_context,
    mock_get_args,
    mock_boto
):

    # -----------------------------
    # Glue args
    # -----------------------------
    mock_get_args.return_value = {
        "CONFIG_PATH": "s3://my-bucket/config.json"
    }

    # -----------------------------
    # Mock S3
    # -----------------------------
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

    # -----------------------------
    # Mock Spark DF
    # -----------------------------
    df_mock = Mock()
    writer_mock = Mock()

    df_mock.coalesce.return_value = df_mock
    df_mock.write = writer_mock

    writer_mock.mode.return_value = writer_mock
    writer_mock.option.return_value = writer_mock
    writer_mock.csv.return_value = None

    spark_mock = Mock()
    spark_mock.createDataFrame.return_value = df_mock

    mock_glue_context.return_value.spark_session = spark_mock
    mock_spark_context.return_value = Mock()

    # -----------------------------
    # RUN
    # -----------------------------
    run_job()

    # -----------------------------
    # ASSERTIONS
    # -----------------------------
    s3.get_object.assert_called_once()
    spark_mock.createDataFrame.assert_called_once()
    writer_mock.csv.assert_called_once()

    args, _ = writer_mock.csv.call_args
    assert "s3://output-bucket/output/" in args[0]
