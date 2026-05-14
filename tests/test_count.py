import sys
from unittest.mock import patch, MagicMock
import pytest

# --- HACK : Simulation des modules AWS Glue inexistants localement ---
# On doit faire ça AVANT d'importer le code source qui les utilise
mock_glue = MagicMock()
sys.modules["awsglue"] = mock_glue
sys.modules["awsglue.context"] = MagicMock()
sys.modules["awsglue.utils"] = MagicMock()
# --------------------------------------------------------------------

from src.jobs.count_and_save_in_csv import run_job

@pytest.fixture(scope="session")
def spark():
    """Crée une session Spark locale pour les tests."""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .master("local[1]") \
        .appName("testing") \
        .getOrCreate()  # Correction de get_create -> getOrCreate

@patch("src.jobs.count_and_save_in_csv.getResolvedOptions")
@patch("src.jobs.count_and_save_in_csv.boto3.client")
@patch("src.jobs.count_and_save_in_csv.GlueContext")
@patch("src.jobs.count_and_save_in_csv.SparkContext")
def test_run_job_logic(mock_sc, mock_glue_context, mock_boto, mock_args, spark):
    # 1. Configuration des arguments Glue
    mock_args.return_value = {'CONFIG_PATH': 's3://fake-bucket/config.json'}

    # 2. Simulation de la réponse S3
    mock_s3_client = MagicMock()
    mock_boto.return_value = mock_s3_client
    
    config_content = '{"OUTPUT_BUCKET_NAME": "test-output-bucket"}'
    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: config_content.encode("utf-8"))
    }

    # 3. Injection de la session Spark locale dans le mock Glue
    mock_glue_context.return_value.spark_session = spark

    # 4. Mock de l'écriture pour ne pas créer de fichiers CSV réels
    # On intercepte l'appel à .write sur n'importe quel DataFrame
    with patch("pyspark.sql.DataFrame.write", new_callable=PropertyMock) as mock_write:
        # On définit une chaîne de mocks pour df.write.mode().option().csv()
        mock_writer = MagicMock()
        mock_write.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        # --- EXECUTION ---
        run_job()

        # --- VERIFICATIONS ---
        # Vérifie si le bon bucket a été appelé
        mock_s3_client.get_object.assert_called_with(Bucket="fake-bucket", Key="config.json")
        
        # Vérifie si le chemin de sortie CSV est correct
        expected_path = "s3://test-output-bucket/output/"
        mock_writer.csv.assert_called_with(expected_path)

# Petit helper pour mocker les propriétés Spark
from unittest.mock import PropertyMock
