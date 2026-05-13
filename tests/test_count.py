import sys
import types
from unittest.mock import MagicMock, patch
import importlib


def test_glue_job_saves_csv():

    # =========================
    # 1. MOCK MODULES AWS GLUE
    # =========================
    sys.modules["awsglue"] = types.ModuleType("awsglue")
    sys.modules["awsglue.context"] = types.ModuleType("awsglue.context")
    sys.modules["awsglue.utils"] = types.ModuleType("awsglue.utils")
    sys.modules["pyspark"] = types.ModuleType("pyspark")
    sys.modules["pyspark.context"] = types.ModuleType("pyspark.context")

    # =========================
    # 2. MOCK ARGUMENTS GLUE
    # =========================
    sys.argv = ["job.py", "--output_path", "s3://fake-bucket/output"]

    mock_get_resolved = MagicMock(return_value={
        "output_path": "s3://fake-bucket/output"
    })

    # =========================
    # 3. MOCK SPARK DATAFRAME
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
    # 5. PATCH + IMPORT JOB
    # =========================
    with patch("awsglue.utils.getResolvedOptions", mock_get_resolved), \
         patch("pyspark.context.SparkContext"), \
         patch("awsglue.context.GlueContext", return_value=mock_glue_context):

        import src.jobs.count_and_save_in_csv as job
        importlib.reload(job)

        # Si ton code n'est pas dans main(), adapte ici
        # sinon wrap dans fonction main()
        job.main()

        # =========================
        # 6. ASSERTIONS
        # =========================

        # DataFrame créé avec bonnes données
        mock_spark.createDataFrame.assert_called_once()

        # Vérifie écriture CSV vers S3
        mock_writer.csv.assert_called_once_with("s3://fake-bucket/output")
