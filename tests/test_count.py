import unittest
from unittest.mock import patch, MagicMock, mock_open
import sys
import json

# Simulation des modules AWS Glue absents en local
mock_glue = MagicMock()
sys.modules['awsglue'] = mock_glue
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['awsglue.job'] = MagicMock()

# Import des fonctions à tester après le mocking
from glue_script import get_s3_config

class TestGlueJob(unittest.TestCase):

    @patch('boto3.client')
    def test_get_s3_config(self, mock_boto_client):
        """Vérifie que la lecture du JSON S3 fonctionne correctement."""
        # Setup du mock S3
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        mock_data = {"OUTPUT_BUCKET_NAME": "test-bucket"}
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(mock_data).encode('utf-8')
        mock_s3.get_object.return_value = {"Body": mock_body}

        # Appel de la fonction
        config = get_s3_config(mock_s3, "s3://fake-bucket/config.json")

        # Assertions
        self.assertEqual(config["OUTPUT_BUCKET_NAME"], "test-bucket")
        mock_s3.get_object.assert_called_with(Bucket="fake-bucket", Key="config.json")

    @patch('glue_script.SparkContext')
    @patch('glue_script.GlueContext')
    @patch('glue_script.Job')
    @patch('glue_script.getResolvedOptions')
    @patch('glue_script.boto3.client')
    def test_run_job_flow(self, mock_boto, mock_get_args, mock_job, mock_glue_ctx, mock_sc):
        """Vérifie le flux complet du job (initialisation et appels)."""
        
        # 1. Mock des arguments
        mock_get_args.return_value = {'JOB_NAME': 'test_job', 'CONFIG_PATH': 's3://b/c.json'}
        
        # 2. Mock de la config S3
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps({"OUTPUT_BUCKET_NAME": "out"}).encode('utf-8')
        mock_s3.get_object.return_value = {"Body": mock_body}

        # 3. Mock Spark Session
        mock_spark = mock_glue_ctx.return_value.spark_session
        
        # Import local pour éviter les effets de bord
        from glue_script import run_job
        
        # Exécution
        run_job()

        # Vérifications
        mock_job.return_value.init.assert_called()
        mock_job.return_value.commit.assert_called()
        mock_spark.createDataFrame.assert_called()
        print("\n✅ Test du flux complet réussi.")

if __name__ == '__main__':
    unittest.main()
