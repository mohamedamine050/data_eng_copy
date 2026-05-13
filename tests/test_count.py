import sys
import types
import importlib
from unittest.mock import MagicMock, patch


def test_glue_job_saves_csv():

    # =========================
    # 1. MOCK awsglue.utils
    # =========================
    mock_utils = types.ModuleType("awsglue.utils")
    mock_utils.getResolvedOptions = MagicMock(return_value={
        "output_path": "s3://fake-bucket/output"
    })

    # =========================
    # 2. MOCK awsglue.context
    # =========================
    mock_spark = MagicMock()

    mock_writer = MagicMock()
    mock_writer.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.csv.return_value = None

    mock_df = MagicMock()
    mock_df.write = mock_writer
    mock_spark.createDataFrame.return_value = mock_df

    mock_glue_context_instance = MagicMock()
    mock_glue_context_instance.spark_session = mock_spark

    MockGlueContext = MagicMock(return_value=mock_glue_context_instance)

    mock_glue_context_module = types.ModuleType("awsglue.context")
    mock_glue_context_module.GlueContext = MockGlueContext

    # =========================
    # 3. MOCK awsglue
    # =========================
    sys.modules["awsglue"] = types.ModuleType("awsglue")
    sys.modules["awsglue.utils"] = mock_utils
    sys.modules["awsglue.context"] = mock_glue_context_module

    # =========================
    # 4. MOCK pyspark
    # =========================
    mock_pyspark_context = types.ModuleType("pyspark.context")
    mock_pyspark_context.SparkContext = MagicMock(return_value=MagicMock())

    sys.modules["pyspark"] = types.ModuleType("pyspark")
    sys.modules["pyspark.context"] = mock_pyspark_context

    # =========================
    # 5. ARGS
    # =========================
    sys.argv = ["job.py", "--output_path", "s3://fake-bucket/output"]

    # =========================
    # 6. IMPORT + RELOAD  (le code s'exécute ici au niveau module)
    # =========================
    import src.jobs.count_and_save_in_csv as job
    importlib.reload(job)

    # =========================
    # 7. ASSERTIONS
    # =========================
    # Un DataFrame a bien été créé avec 20 lignes (1 → 20)
    mock_spark.createDataFrame.assert_called_once()
    args_call, _ = mock_spark.createDataFrame.call_args
    data_arg = args_call[0]
    assert data_arg == [(i,) for i in range(1, 21)], (
        f"Expected 20 rows (1→20), got: {data_arg}"
    )

    # Le CSV a bien été écrit vers le bon chemin S3
    mock_writer.mode.assert_called_once_with("overwrite")
    mock_writer.option.assert_called_once_with("header", "true")
    mock_writer.csv.assert_called_once_with("s3://fake-bucket/output")
