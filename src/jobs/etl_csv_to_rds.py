"""
etl_csv_to_rds.py - Generic CSV → RDS ETL
══════════════════════════════════════════

Pipeline
--------
S3 (any CSV) → clean & transform → RDS PostgreSQL table

Config JSON stored in S3 (loaded via --CONFIG_PATH arg):
---------------------------------------------------------
{
  "INPUT_BUCKET_NAME":  "my-input-bucket",
  "INPUT_KEY_NAME":     "raw/data.csv",
  "DB_HOST":            "xxx.rds.amazonaws.com",
  "DB_PORT":            "5432",
  "DB_NAME":            "mydb",
  "DB_USER":            "admin",
  "DB_PASSWORD":        "secret",
  "DB_TABLE":           "products_clean"
}
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
    key    = parsed.path.lstrip("/")
    s3     = boto3.client("s3")
    resp   = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


# ─────────────────────────────────────────────
# EXTRACT — lire le CSV depuis S3
# ─────────────────────────────────────────────

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    logger.info("Reading CSV from s3://%s/%s", bucket, key)
    s3   = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)
    raw  = resp["Body"].read().decode("utf-8")
    df   = pd.read_csv(io.StringIO(raw))
    logger.info("CSV loaded → %d rows, %d columns : %s", len(df), len(df.columns), list(df.columns))
    return df


# ─────────────────────────────────────────────
# TRANSFORM — nettoyage générique
# ─────────────────────────────────────────────

def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming %d rows", len(df))

    # ── 0. DataFrame vide → retour immédiat
    if df.empty:
        return df

    # ── 1. Noms de colonnes : strip + lowercase + espaces → underscore
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # ── 2. Supprimer les lignes entièrement vides
    df = df.dropna(how="all")

    # ── 3. Strip sur toutes les colonnes string
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # ── 4. Convertir les colonnes numériques automatiquement
    for col in df.columns:
        if df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            # on ne remplace que si la conversion a marché pour >50% des valeurs
            if converted.notna().sum() > 0.5 * len(df):
                df[col] = converted

    # ── 5. Supprimer les doublons
    before = len(df)
    df = df.drop_duplicates()
    logger.info("Duplicates removed: %d → %d rows", before, len(df))

    # ── 6. Timestamp d'ingestion
    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────
# LOAD — écrire dans RDS PostgreSQL
# ─────────────────────────────────────────────

def get_rds_engine(config: dict) -> sqlalchemy.engine.Engine:
    host     = config["DB_HOST"].split(":")[0]   # retire le port s'il est collé
    port     = config.get("DB_PORT", "5432")
    db_name  = config["DB_NAME"]
    user     = config["DB_USER"]
    password = config["DB_PASSWORD"]

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    logger.info("Connecting to RDS → %s:%s/%s", host, port, db_name)

    engine = sqlalchemy.create_engine(
        url,
        connect_args={"connect_timeout": 10},
    )
    return engine


def load_to_rds(df: pd.DataFrame, engine: sqlalchemy.engine.Engine, table: str) -> int:
    """
    Écrit le DataFrame dans la table RDS.
    if_exists="append" → ajoute sans écraser les données existantes.
    Remplacer par "replace" pour recréer la table à chaque run.
    """
    logger.info("Loading %d rows into table '%s'", len(df), table)

    df.to_sql(
        name=table,
        con=engine,
        if_exists="append",   # "replace" pour écraser, "append" pour ajouter
        index=False,
        method="multi",       # insertion par batch (plus rapide)
        chunksize=500,
    )

    logger.info("Load complete → table '%s'", table)
    return len(df)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    logger.info("START ETL")

    args   = get_args()
    config = load_config(args["CONFIG_PATH"])

    # ── Config S3 input ──────────────────────
    input_bucket = config["INPUT_BUCKET_NAME"]
    key_input    = config["INPUT_KEY_NAME"]

    # ── Config RDS ───────────────────────────
    db_table = config.get("DB_TABLE", "etl_output")

    # ── Extract ──────────────────────────────
    df_raw = read_csv_from_s3(input_bucket, key_input)

    # ── Transform ────────────────────────────
    df_clean = transform(df_raw)

    logger.info("Columns in final DataFrame: %s", list(df_clean.columns))
    logger.info("Rows: %d", len(df_clean))

    # ── Load → RDS ───────────────────────────
    engine    = get_rds_engine(config)
    rows_written = load_to_rds(df_clean, engine, db_table)

    logger.info("DONE → %d rows written to RDS table '%s'", rows_written, db_table)


if __name__ == "__main__":
    main()
