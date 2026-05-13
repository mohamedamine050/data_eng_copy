import sys
import types
import importlib
from unittest.mock import MagicMock


def test_glue_job_saves_single_csv():

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

    # Chaîne d'écriture : coalesce(1).write.mode().option().csv()
    mock_writer = MagicMock()
    mock_writer.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.csv.return_value = None

    mock_df = MagicMock()
    mock_df.coalesce.return_value = mock_df   # coalesce(1) retourne le même df
    mock_df.write = mock_writer

    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df

    mock_glue_context_instance = MagicMock()
    mock_glue_context_instance.spark_session = mock_spark

    mock_glue_context_module = types.ModuleType("awsglue.context")
    mock_glue_context_module.GlueContext = MagicMock(return_value=mock_glue_context_instance)

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
    # 6. IMPORT UNIQUE (pas de reload)
    # =========================
    sys.modules.pop("src.jobs.count_and_save_in_csv", None)
    import src.jobs.count_and_save_in_csv  # noqa: F401

    # =========================
    # 7. ASSERTIONS
    # =========================

    # ✅ DataFrame créé avec les 20 bonnes lignes
    mock_spark.createDataFrame.assert_called_once()
    args_call, _ = mock_spark.createDataFrame.call_args
    assert args_call[0] == [(i,) for i in range(1, 21)], "Les données 1→20 sont incorrectes"
    assert args_call[1] == ["number"], "Le nom de colonne doit être 'number'"

    # ✅ coalesce(1) appelé → un seul fichier CSV
    mock_df.coalesce.assert_called_once_with(1)

    # ✅ Options d'écriture correctes
    mock_writer.mode.assert_called_once_with("overwrite")
    mock_writer.option.assert_called_once_with("header", "true")
    mock_writer.csv.assert_called_once_with("s3://fake-bucket/output")
