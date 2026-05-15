import sys
import json
import logging
import requests
import boto3
from urllib.parse import urlparse

from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as _sum, avg, round as _round
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType
)

# ── Configuration Initiale ─────────────────────────────────────────────────────
API_URL = "https://restcountries.com/v3.1/all"

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

COUNTRIES_SCHEMA = StructType([
    StructField("name",        StringType(),  True),
    StructField("region",      StringType(),  True),
    StructField("subregion",   StringType(),  True),
    StructField("population",  LongType(),    True),
    StructField("area_km2",    DoubleType(),  True),
    StructField("capital",     StringType(),  True),
    StructField("independent", BooleanType(), True),
])

# ══════════════════════════════════════════════════════════════════════════════
# LOGIQUE ETL
# ══════════════════════════════════════════════════════════════════════════════

def fetch_and_parse() -> list:
    """Récupère et aplatit les données de l'API."""
    log.info(f"Appel de l'API: {API_URL}")
    res = requests.get(API_URL, timeout=30)
    res.raise_for_status()
    data = res.json()
    
    records = []
    for c in data:
        capitals = c.get("capital", [])
        records.append({
            "name":        c.get("name", {}).get("common"),
            "region":      c.get("region"),
            "subregion":   c.get("subregion"),
            "population":  int(c.get("population")) if c.get("population") is not None else 0,
            "area_km2":    float(c.get("area")) if c.get("area") is not None else 0.0,
            "capital":     capitals[0] if capitals else None,
            "independent": bool(c.get("independent")),
        })
    return records

def process_data(spark: SparkSession, records: list) -> tuple[DataFrame, DataFrame]:
    """Nettoyage et Agrégation via Spark."""
    df = spark.createDataFrame(records, schema=COUNTRIES_SCHEMA)
    
    # Nettoyage
    df_clean = df.filter(
        (col("region").isNotNull()) & (col("population") > 0) & (col("area_km2") > 0)
    )
    
    # Agrégation
    df_agg = (
        df_clean.groupBy("region")
        .agg(
            count("name").alias("nb_countries"),
            _sum("population").alias("total_population"),
            _round(avg("area_km2"), 2).alias("avg_area_km2")
        )
        .orderBy(col("total_population").desc())
    )
    
    return df_clean, df_agg

# ══════════════════════════════════════════════════════════════════════════════
# EXECUTION (AWS GLUE)
# ══════════════════════════════════════════════════════════════════════════════

# 1. Récupération des arguments (Le job attend --CONFIG_PATH)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CONFIG_PATH'])

# 2. Lecture de la config sur S3 via boto3
parsed = urlparse(args['CONFIG_PATH'])
s3 = boto3.client("s3")
response = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip('/'))
config = json.loads(response["Body"].read().decode("utf-8"))

output_bucket = config["OUTPUT_BUCKET_NAME"]
output_path = f"s3://{output_bucket}/output"

# 3. Initialisation du contexte Glue/Spark
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

try:
    # 4. Pipeline
    raw_records = fetch_and_parse()
    df_detail, df_summary = process_data(spark, raw_records)

    # 5. Écriture S3 (Format CSV)
    log.info(f"Écriture des résultats vers {output_path}")
    
    df_detail.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}/detail")
    df_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}/summary")

    job.commit()
    log.info("Job terminé avec succès.")

except Exception as e:
    log.error(f"Erreur lors de l'exécution : {e}")
    raise
