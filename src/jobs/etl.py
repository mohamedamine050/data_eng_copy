"""
AWS Glue ETL Job — REST Countries API
======================================
Extract  : REST Countries API (https://restcountries.com/v3.1/all)
Transform : nettoyage des nulls + agrégation par région
Load     : CSV (S3 ou local)
"""

import sys
import logging
import requests

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as _sum, avg, round as _round
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType,
)

# ── Glue imports (optionnels, absents en local) ────────────────────────────────
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    IS_GLUE = True
except ImportError:
    IS_GLUE = False

# ── Config ─────────────────────────────────────────────────────────────────────
API_URL = "https://restcountries.com/v3.1/all"
DEFAULT_OUTPUT = "output/countries"          # remplacé par args["output_path"] sur Glue

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ── Schéma Spark ───────────────────────────────────────────────────────────────
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
# EXTRACT
# ══════════════════════════════════════════════════════════════════════════════

def fetch_countries(url: str = API_URL, timeout: int = 30) -> list:
    """Appel HTTP vers l'API REST Countries."""
    log.info(f"Fetching data from {url}")
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    log.info(f"Fetched {len(data)} countries")
    return data


def parse_countries(raw_data: list) -> list[dict]:
    """Aplatit le JSON imbriqué en liste de dicts plats."""
    records = []
    for country in raw_data:
        try:
            capital_list = country.get("capital") or []
            records.append({
                "name":        country.get("name", {}).get("common"),
                "region":      country.get("region"),
                "subregion":   country.get("subregion"),
                "population":  int(pop) if (pop := country.get("population")) is not None else None,
                "area_km2":    float(area) if (area := country.get("area")) is not None else None,
                "capital":     capital_list[0] if capital_list else None,
                "independent": bool(country.get("independent")),
            })
        except Exception as exc:
            log.warning(f"Skipping malformed record: {exc}")
    log.info(f"Parsed {len(records)} records")
    return records


# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════

def to_spark_df(spark: SparkSession, records: list[dict]) -> DataFrame:
    """Convertit la liste de dicts en DataFrame Spark typé."""
    return spark.createDataFrame(records, schema=COUNTRIES_SCHEMA)


def filter_and_clean(df: DataFrame) -> DataFrame:
    """
    Nettoyage :
      - Supprime les lignes sans région, population ou superficie
      - Supprime les pays avec population ou surface <= 0
    """
    before = df.count()
    df_clean = df.filter(
        col("region").isNotNull()     &
        col("population").isNotNull() &
        col("area_km2").isNotNull()   &
        (col("region")     != "")     &
        (col("population") > 0)       &
        (col("area_km2")   > 0)
    )
    after = df_clean.count()
    log.info(f"Cleaning: {before} → {after} rows (dropped {before - after})")
    return df_clean


def aggregate_by_region(df: DataFrame) -> DataFrame:
    """
    Agrégation par région :
      - Nombre de pays
      - Population totale
      - Population moyenne
      - Superficie moyenne (km²)
    """
    return (
        df.groupBy("region")
          .agg(
              count("name")           .alias("nb_countries"),
              _sum("population")      .alias("total_population"),
              _round(avg("population"), 0).alias("avg_population"),
              _round(avg("area_km2"),  2) .alias("avg_area_km2"),
          )
          .orderBy(col("total_population").desc())
    )


# ══════════════════════════════════════════════════════════════════════════════
# LOAD
# ══════════════════════════════════════════════════════════════════════════════

def save_csv(df: DataFrame, path: str, label: str) -> None:
    """Écrit un DataFrame en CSV (1 fichier, avec header)."""
    full_path = f"{path}/{label}"
    log.info(f"Saving '{label}' → {full_path}")
    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", True)
          .option("sep", ",")
          .csv(full_path)
    )
    log.info(f"Saved '{label}' successfully")


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

def run_etl(spark: SparkSession, output_path: str) -> tuple[DataFrame, DataFrame]:
    """Pipeline complet Extract → Transform → Load."""

    # 1. Extract
    raw_data = fetch_countries(API_URL)
    records  = parse_countries(raw_data)

    # 2. Transform
    df_raw   = to_spark_df(spark, records)
    df_clean = filter_and_clean(df_raw)
    df_agg   = aggregate_by_region(df_clean)

    df_clean.show(5, truncate=False)
    df_agg  .show(truncate=False)

    # 3. Load
    save_csv(df_clean, output_path, "detail")
    save_csv(df_agg,   output_path, "aggregated_by_region")

    return df_clean, df_agg


# ══════════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":

    if IS_GLUE:
        # ── Mode AWS Glue ──────────────────────────────────────────────────────
        args         = getResolvedOptions(sys.argv, ["JOB_NAME", "output_path"])
        sc           = SparkContext()
        glue_context = GlueContext(sc)
        spark        = glue_context.spark_session
        job          = Job(glue_context)
        job.init(args["JOB_NAME"], args)
        output_path  = args["output_path"]        # ex: s3://mon-bucket/output/

    else:
        # ── Mode local (dev / debug) ───────────────────────────────────────────
        spark = (
            SparkSession.builder
            .appName("ETL_Countries_Local")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        output_path = DEFAULT_OUTPUT

    run_etl(spark, output_path)

    if IS_GLUE:
        job.commit()
