import unittest
from unittest.mock import MagicMock, patch
import json
from pyspark.sql import SparkSession

# On simule les modules AWS Glue qui ne sont pas présents localement
import sys
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()

class TestGlueJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Création d'une session Spark locale pour les tests
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("GlueUnitTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @patch('boto3.client')
    @patch('awsglue.utils.getResolvedOptions')
    def test_config_parsing_and_write(self, mock_get_options, mock_boto):
        # 1. Mock des arguments Glue
        mock_get_options.return_value = {'CONFIG_PATH': 's3://my-bucket/config.json'}

        # 2. Mock de la réponse S3 (Boto3)
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        
        config_data = {"OUTPUT_BUCKET_NAME": "test-output-bucket"}
        body_mock = MagicMock()
        body_mock.read.return_value = json.dumps(config_data).encode('utf-8')
        mock_s3.get_object.return_value = {'Body': body_mock}

        # 3. Logique de test (Simulation du flux du script)
        # Simulation extraction config
        bucket = "my-bucket"
        key = "config.json"
        response = mock_s3.get_object(Bucket=bucket, Key=key)
        config = json.loads(response["Body"].read().decode("utf-8"))
        
        output_path = f"s3://{config['OUTPUT_BUCKET_NAME']}/output/"
        
        # Validation de la construction du chemin
        self.assertEqual(output_path, "s3://test-output-bucket/output/")

        # 4. Test de la création du DataFrame Spark
        data = [(i,) for i in range(1, 21)]
        df = self.spark.createDataFrame(data, ["number"])
        
        self.assertEqual(df.count(), 20)
        self.assertEqual(len(df.columns), 1)

    def test_data_logic(self):
        # Test spécifique sur la transformation de données
        data = [(i,) for i in range(1, 21)]
        df = self.spark.createDataFrame(data, ["number"])
        
        # Vérifier que le max est bien 20
        max_val = df.agg({"number": "max"}).collect()[0][0]
        self.assertEqual(max_val, 20)

if __name__ == '__main__':
    unittest.main()
