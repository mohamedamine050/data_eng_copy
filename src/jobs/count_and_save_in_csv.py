import sys
import json
import boto3
from urllib.parse import urlparse

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

def get_s3_config(s3_client, config_path):
    """Récupère le JSON de configuration depuis S3."""
    parsed = urlparse(config_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))

def run_job():
    # Initialisation des arguments (CONFIG_PATH est requis)
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CONFIG_PATH'])
    
    # Setup Glue & Spark
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    try:
        # Configuration S3
        s3 = boto3.client("s3")
        config = get_s3_config(s3, args['CONFIG_PATH'])
        
        output_bucket = config["OUTPUT_BUCKET_NAME"]
        output_path = f"s3://{output_bucket}/output/"

        # Logique de données
        data = [(i,) for i in range(1, 21)]
        df = spark.createDataFrame(data, ["number"])

        # Écriture (un seul fichier CSV)
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)

        print(f"✅ Succès : Données écrites vers {output_path}")

    except Exception as e:
        print(f"❌ Erreur lors de l'exécution : {str(e)}")
        raise e
    finally:
        job.commit()

if __name__ == "__main__":
    run_job()
