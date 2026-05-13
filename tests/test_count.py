import sys
import types
from unittest.mock import MagicMock, patch
import importlib


def test_glue_job_saves_csv():

    # =========================
    # 1. MOCK AWS GLUE
    # =========================
    mock_utils = types.ModuleType("awsglue.utils")
    mock_utils.getResolvedOptions = MagicMock(return_value={
        "output_path": "s3://fake-bucket/output"
    })

    sys.modules["awsglue.utils"] = mock_utils
    sys.modules["awsglue.context"] = types.ModuleType("awsglue.context")
    sys.modules["awsglue"] = types.ModuleType("awsglue")

    # =========================
    # 2. MOCK PYSPARK CONTEXT + CLASS
    # =========================
    mock_pyspark_context = types.ModuleType("pyspark.context")
    mock_pyspark_context.SparkContext = MagicMock()

    sys.modules["pyspark"] = types.ModuleType("pyspark")
    sys.modules["pyspark.context"] = mock_pyspark_context

    # =========================
    # 3. ARGS
    # =========================
    sys.argv = ["job.py", "--output_path", "s3://fake-bucket/output"]

    # =========================
    # 4. MOCK SPARK
    # =========================
    mock_df = MagicMock()

    mock_writer = MagicMock()
    mock_df.write = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.csv.return_value = None

    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df

    # =========================
    # 5. MOCK GLUE CONTEXT
    # =========================
    mock_glue_context = MagicMock()
    mock_glue_context.spark_session = mock_spark

    # =========================
    # 6. TEST
    # =========================
    with patch("awsglue.context.GlueContext", return_value=mock_glue_context):

        import src.jobs.count_and_save_in_csv as job
        importlib.reload(job)

        job.main()

        mock_spark.createDataFrame.assert_called_once()
        mock_writer.csv.assert_called_once_with("s3://fake-bucket/output")
