"""
etl_csv_to_rds.py - Generic CSV → RDS ETL
══════════════════════════════════════════

Pipeline
--------
S3 (any CSV) → clean & transform → RDS PostgreSQL table
"""

import io
import json
import logging
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import pandas as pd
import sqlalchemy

# ─────────────────────────────────────────────
# FIX IMPORTANT POUR LES TESTS (PATCH MODULE NAME)
# ─────────────────────────────────────────────
# Permet aux tests de faire:
# @patch("etl_csv_to_rds.xxx")
sys.modules.setdefault("etl_csv_to_rds", sys.modules[__name__])

# ─────────────────────────────────────────────
# AWS GLUE COMPAT
# ─────────────────────────────────────────────
try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    def getResolvedOptions(argv: list, options: list) -> dict:
        import argparse
        parser = argparse.ArgumentParser()
        for opt in options:
            parser.add_argument(f"--{opt}")
        args, _ = parser.parse_known_args(argv[1:])
        return vars(args)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

logger = logging.getLogger("etl_csv_to_rds")


# ─────────────────────────────────────────────
# ARGS & CONFIG
# ─────────────────────────────────────────────

def get_args() -> dict:
    return getResolvedOptions(sys.argv, ["CONFIG_PATH"])


def load_config(config_path: str) -> dict:
    logger.info("Loading config from %s", config_path)

    parsed = urlparse(config_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)

    return json.loads(resp["Body"].read().decode("utf-8"))


# ─────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    logger.info("Reading CSV from s3://%s/%s", bucket, key)

    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)

    raw = resp["Body"].read().decode("utf-8")
    df = pd.read_csv(io.StringIO(raw))

    logger.info("Loaded → %d rows, %d columns", len(df), len(df.columns))
    return df


# ─────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────

def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming %d rows", len(df))

    if df.empty:
        return df

    # 1. clean column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # 2. remove empty rows
    df = df.dropna(how="all")

    # 3. strip strings
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # 4. numeric conversion
    for col in df.columns:
        if df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().sum() > 0.5 * len(df):
                df[col] = converted

    # 5. remove duplicates
    df = df.drop_duplicates()

    # 6. ingestion timestamp
    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────

def get_rds_engine(config: dict) -> sqlalchemy.engine.Engine:
    host = config["DB_HOST"].split(":")[0]
    port = config.get("DB_PORT", "5432")

    url = (
        f"postgresql+psycopg2://{config['DB_USER']}:{config['DB_PASSWORD']}"
        f"@{host}:{port}/{config['DB_NAME']}"
    )

    logger.info("Connecting to RDS %s:%s/%s", host, port, config["DB_NAME"])

    return sqlalchemy.create_engine(
        url,
        connect_args={"connect_timeout": 10},
    )


def load_to_rds(df: pd.DataFrame, engine, table: str) -> int:
    logger.info("Loading %d rows into %s", len(df), table)

    df.to_sql(
        name=table,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    return len(df)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    logger.info("START ETL")

    args = get_args()
    config = load_config(args["CONFIG_PATH"])

    input_bucket = config["INPUT_BUCKET_NAME"]
    key_input = config["INPUT_KEY_NAME"]

    table = config.get("DB_TABLE", "etl_output")

    df_raw = read_csv_from_s3(input_bucket, key_input)
    df_clean = transform(df_raw)

    engine = get_rds_engine(config)
    load_to_rds(df_clean, engine, table)

    logger.info("DONE")


if __name__ == "__main__":
    main()
