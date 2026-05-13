import sys
import types
from unittest.mock import MagicMock, patch
import importlib


def test_glue_job_saves_csv():

    # =========================
    # 1. MOCK MODULES + FUNCTIONS
    # =========================
    mock_utils = types.ModuleType("awsglue.utils")
    mock_utils.getResolvedOptions = MagicMock(return_value={
        "output_path": "s3://fake-bucket/output"
    })

    sys.modules["awsglue.utils"] = mock_utils
    sys.modules["awsglue.context"] = types.ModuleType("awsglue.context")
    sys.modules["awsglue"] = types.ModuleType("awsglue")

    sys.modules["pyspark.context"] = types.ModuleType("pyspark.context")
    sys.modules["pyspark"] = types.ModuleType("pyspark")

    # =========================
    # 2. ARGS
    # =========================
    sys.argv = ["job.py", "--output_path", "s3://fake-bucket/output"]

    # =========================
    # 3. MOCK SPARK
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
    # 4. MOCK GLUE CONTEXT
    # =========================
    mock_glue_context = MagicMock()
    mock_glue_context.spark_session = mock_spark

    # =========================
    # 5. PATCH AWS CLASSES ONLY
    # =========================
    with patch("pyspark.context.SparkContext"), \
         patch("awsglue.context.GlueContext", return_value=mock_glue_context):

        import src.jobs.count_and_save_in_csv as job
        importlib.reload(job)

        job.main()

        # =========================
        # 6. ASSERTIONS
        # =========================
        mock_spark.createDataFrame.assert_called_once()
        mock_writer.csv.assert_called_once_with("s3://fake-bucket/output")
