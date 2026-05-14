import sys
import json
import types
from unittest.mock import Mock, patch


# -----------------------------
# Fake modules (important)
# -----------------------------
sys.modules["pyspark"] = types.ModuleType("pyspark")
sys.modules["pyspark.context"] = types.ModuleType("pyspark.context")
sys.modules["awsglue"] = types.ModuleType("awsglue")
sys.modules["awsglue.context"] = types.ModuleType("awsglue.context")
sys.modules["awsglue.utils"] = types.ModuleType("awsglue.utils")


from src.jobs.count_and_save_in_csv import run_job


# -----------------------------
# PATCH IMPORTANT (CORRECT)
# -----------------------------
@patch("src.jobs.count_and_save_in_csv.boto3.client")
@patch("awsglue.utils.getResolvedOptions")
@patch("pyspark.context.SparkContext")
@patch("awsglue.context.GlueContext")   # 👈 OK ici MAIS ON VA MOCK LA CLASSE
def test_run_job_success(
    mock_glue_class,
    mock_spark,
    mock_args,
    mock_boto
):

    # -----------------------------
    # Args Glue
    # -----------------------------
    mock_args.return_value = {
        "CONFIG_PATH": "s3://my-bucket/config.json"
    }

    # -----------------------------
    # S3 mock
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
    # Spark mock
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

    # ⚠️ GlueContext instance mock
    glue_instance = Mock()
    glue_instance.spark_session = spark_mock

    mock_glue_class.return_value = glue_instance
    mock_spark.return_value = Mock()

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
