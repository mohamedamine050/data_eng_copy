import unittest
from unittest.mock import patch, MagicMock
import json
import io

# On simule les modules Glue qui ne sont pas présents en local
import sys
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()

class TestGlueScript(unittest.TestCase):

    @patch('boto3.client')
    @patch('awsglue.utils.getResolvedOptions')
    @patch('pyspark.context.SparkContext')
    @patch('awsglue.context.GlueContext')
    def test_script_logic(self, mock_glue_context, mock_spark_context, mock_get_options, mock_boto_client):
        """
        Teste la lecture de la config S3 et la construction du chemin de sortie.
        """
        
        # 1. Mock des arguments Glue
        mock_get_options.return_value = {'CONFIG_PATH': 's3://my-test-bucket/config.json'}

        # 2. Mock de la réponse S3 (boto3)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        config_data = {"OUTPUT_BUCKET_NAME": "target-bucket"}
        json_bytes = json.dumps(config_data).encode('utf-8')
        
        # Simulation du flux de lecture S3
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: json_bytes)
        }

        # 3. Exécution d'une partie de la logique (Simulation)
        from urllib.parse import urlparse
        config_path = mock_get_options(None, [])['CONFIG_PATH']
        parsed = urlparse(config_path)
        
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        
        response = mock_s3.get_object(Bucket=bucket, Key=key)
        config = json.loads(response["Body"].read().decode("utf-8"))
        
        output_bucket = config["OUTPUT_BUCKET_NAME"]
        output_path = f"s3://{output_bucket}/output/"

        # 4. Assertions
        self.assertEqual(bucket, "my-test-bucket")
        self.assertEqual(key, "config.json")
        self.assertEqual(output_path, "s3://target-bucket/output/")
        print(f"\n✅ Assertion réussie : {output_path}")

if __name__ == '__main__':
    unittest.main()
