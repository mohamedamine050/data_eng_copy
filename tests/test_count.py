import sys
from unittest.mock import patch, MagicMock, PropertyMock
import pytest

# --- Simulation des modules AWS Glue pour l'environnement local ---
mock_glue = MagicMock()
sys.modules["awsglue"] = mock_glue
sys.modules["awsglue.context"] = MagicMock()
sys.modules["awsglue.utils"] = MagicMock()
# ------------------------------------------------------------------

from src.jobs.count_and_save_in_csv import run_job

@pytest.fixture(scope="session")
def spark():
    """Crée une session Spark locale pour les tests."""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .master("local[1]") \
        .appName("testing") \
        .getOrCreate()

# On patche directement les modules sources car les imports sont internes à la fonction
@patch("awsglue.utils.getResolvedOptions")
@patch("boto3.client")
@patch("awsglue.context.GlueContext")
@patch("pyspark.context.SparkContext")
def test_run_job_logic(mock_sc, mock_glue_context, mock_boto, mock_args, spark):
    # 1. Mock des arguments Glue
    mock_args.return_value = {'CONFIG_PATH': 's3://fake-bucket/config.json'}

    # 2. Mock de S3
    mock_s3_client = MagicMock()
    mock_boto.return_value = mock_s3_client
    config_content = '{"OUTPUT_BUCKET_NAME": "test-output-bucket"}'
    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: config_content.encode("utf-8"))
    }

    # 3. Injection de la session Spark de test
    mock_glue_context.return_value.spark_session = spark

    # 4. Mock du DataFrame.write pour ne pas écrire réellement de CSV
    # On utilise patch.object sur la classe DataFrame de pyspark
    from pyspark.sql import DataFrame
    with patch.object(DataFrame, "write", new_callable=PropertyMock) as mock_write_prop:
        mock_writer = MagicMock()
        mock_write_prop.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        # --- EXÉCUTION ---
        run_job()

        # --- VÉRIFICATIONS ---
        # Vérification de l'appel S3
        mock_s3_client.get_object.assert_called_with(Bucket="fake-bucket", Key="config.json")
        
        # Vérification de la destination du CSV
        expected_path = "s3://test-output-bucket/output/"
        mock_writer.csv.assert_called_with(expected_path)
