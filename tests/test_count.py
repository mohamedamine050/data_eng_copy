import sys
import json
import types
from unittest.mock import Mock, patch

# -------------------------------------------------------
# Mock AWS Glue / PySpark (pour exécution locale)
# -------------------------------------------------------
sys.modules["pyspark"] = types.ModuleType("pyspark")
sys.modules["pyspark.context"] = types.ModuleType("pyspark.context")
sys.modules["awsglue"] = types.ModuleType("awsglue")
sys.modules["awsglue.context"] = types.ModuleType("awsglue.context")
sys.modules["awsglue.utils"] = types.ModuleType("awsglue.utils")


# -------------------------------------------------------
# Import du job
# -------------------------------------------------------
from src.jobs.count_and_save_in_csv import run_job


# -------------------------------------------------------
# TEST
# -------------------------------------------------------
@patch("src.jobs.count_and_save_in_csv.boto3.client")
@patch("src.jobs.count_and_save_in_csv.getResolvedOptions")
@patch("src.jobs.count_and_save_in_csv.SparkContext")
@patch("src.jobs.count_and_save_in_csv.GlueContext")
def test_run_job_success(
    mock_glue,
    mock_sc,
    mock_args,
    mock_boto
):

    # -----------------------------
    # Glue args
    # -----------------------------
    mock_args.return_value = {
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
    # Mock Spark DataFrame
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

    mock_glue.return_value.spark_session = spark_mock
    mock_sc.return_value = Mock()

    # -----------------------------
    # EXECUTION
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
