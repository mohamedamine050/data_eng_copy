import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
import sys

# On importe la fonction à tester
# Note : assure-toi que ton dossier 'src' est dans le PYTHONPATH
from src.jobs.count_and_save_in_csv import run_job

@pytest.fixture(scope="session")
def spark():
    """Cree une session Spark locale pour les tests."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("testing") \
        .get_create()

@patch("src.jobs.count_and_save_in_csv.getResolvedOptions")
@patch("src.jobs.count_and_save_in_csv.boto3.client")
@patch("src.jobs.count_and_save_in_csv.GlueContext")
@patch("src.jobs.count_and_save_in_csv.SparkContext")
def test_run_job_logic(mock_sc, mock_glue_context, mock_boto, mock_args, spark):
    # 1. Mock des arguments Glue
    mock_args.return_value = {'CONFIG_PATH': 's3://fake-bucket/config.json'}

    # 2. Mock de la réponse S3 (le fichier de config JSON)
    mock_s3_client = MagicMock()
    mock_boto.return_value = mock_s3_client
    
    config_content = '{"OUTPUT_BUCKET_NAME": "my-test-output"}'
    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: config_content.encode("utf-8"))
    }

    # 3. Mock de Glue pour retourner notre session Spark locale
    mock_glue_context.return_value.spark_session = spark

    # 4. Execution du job
    # On mock aussi le 'write' pour éviter d'écrire réellement sur le disque/S3 pendant le test
    with patch.object(spark.DataFrame, "write", new_callable=MagicMock) as mock_write:
        run_job()
        
        # Vérifications
        # Est-ce que S3 a été appelé avec le bon bucket ?
        mock_s3_client.get_object.assert_called_with(Bucket="fake-bucket", Key="config.json")
        
        # Est-ce que l'écriture a été tentée au bon endroit ?
        expected_path = "s3://my-test-output/output/"
        mock_write.mode.assert_called_with("overwrite")
        mock_write.mode().option.assert_called_with("header", "true")
        mock_write.mode().option().csv.assert_called_with(expected_path)

    print("Test validé avec succès !")
