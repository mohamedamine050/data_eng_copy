def run_job():
    import sys
    import json
    import boto3
    from urllib.parse import urlparse

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ['CONFIG_PATH'])
    config_path = args['CONFIG_PATH']

    parsed = urlparse(config_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    config = json.loads(response["Body"].read().decode("utf-8"))

    output_bucket = config["OUTPUT_BUCKET_NAME"]
    output_path = f"s3://{output_bucket}/output/"

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    data = [(i,) for i in range(1, 21)]
    df = spark.createDataFrame(data, ["number"])

    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

    print(f"Data written to: {output_path}")


if __name__ == "__main__":
    run_job()
