import sys
import json
import boto3
from urllib.parse import urlparse

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# -----------------------------
# Args Glue
# -----------------------------
args = getResolvedOptions(sys.argv, ['CONFIG_PATH'])
config_path = args['CONFIG_PATH']

# -----------------------------
# Parse S3 path
# -----------------------------
parsed = urlparse(config_path)
bucket = parsed.netloc
key = parsed.path.lstrip('/')

# -----------------------------
# Read config.json from S3
# -----------------------------
s3 = boto3.client("s3")
response = s3.get_object(Bucket=bucket, Key=key)

config = json.loads(response["Body"].read().decode("utf-8"))

output_bucket = config["OUTPUT_BUCKET_NAME"]
output_path = f"s3://{output_bucket}/output/"

print(f"Output path from config: {output_path}")

# -----------------------------
# Spark / Glue init
# -----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -----------------------------
# Data
# -----------------------------
data = [(i,) for i in range(1, 21)]
df = spark.createDataFrame(data, ["number"])
df.show()

# -----------------------------
# Write to S3 (single CSV)
# -----------------------------
df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"Data written to: {output_path}")
