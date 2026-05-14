import sys
from unittest.mock import patch, MagicMock
import pytest

# --- Simulation des modules AWS Glue ---
mock_glue = MagicMock()
sys.modules["awsglue"] = mock_glue
sys.modules["awsglue.context"] = MagicMock()
sys.modules["awsglue.utils"] = MagicMock()

from src.jobs.count_and_save_in_csv import run_job

@pytest.fixture(scope="session")
def spark():
    """Crée une session Spark locale pour les tests."""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .master("local[1]") \
        .appName("testing") \
        .getOrCreate()

@patch("awsglue.utils.getResolvedOptions")
@patch("boto3.client")
@patch("awsglue.context.GlueContext")
@patch("pyspark.context.SparkContext")
def test_run_job_logic(mock_sc, mock_glue_context, mock_boto, mock_args, spark):
    # 1. Mock des arguments Glue
    mock_args.return_value = {'CONFIG_PATH': 's3://fake-bucket/config.json'}

    # 2. Mock de S3 (lecture config)
    mock_s3_client = MagicMock()
    mock_boto.return_value = mock_s3_client
    config_content = '{"OUTPUT_BUCKET_NAME": "test-output-bucket"}'
    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: config_content.encode("utf-8"))
    }

    # 3. Injection de la session Spark
    mock_glue_context.return_value.spark_session = spark

    # 4. Mocker la création du DataFrame pour intercepter l'écriture
    # On patche 'createDataFrame' pour qu'il renvoie un objet Mock au lieu d'un vrai DF
    mock_df = MagicMock()
    
    # On s'assure que df.coalesce(1) renvoie aussi le mock
    mock_df.coalesce.return_value = mock_df
    
    # On simule la chaîne d'écriture : df.write.mode().option().csv()
    mock_writer = mock_df.write.mode.return_value.option.return_value

    with patch.object(spark, "createDataFrame", return_value=mock_df):
        # --- EXÉCUTION ---
        run_job()

        # --- VÉRIFICATIONS ---
        # Vérifie que la config S3 a été lue
        mock_s3_client.get_object.assert_called_with(Bucket="fake-bucket", Key="config.json")
        
        # Vérifie que le CSV a été "écrit" (via le mock) au bon endroit
        expected_path = "s3://test-output-bucket/output/"
        mock_writer.csv.assert_called_with(expected_path)

    print("Test réussi et écriture S3 neutralisée !")
