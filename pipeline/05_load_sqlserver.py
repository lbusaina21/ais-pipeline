"""
pipeline/05_load_sqlserver.py
Load semua hasil analisis ke SQL Server BPS on-prem.
"""

import sys
sys.path.append("/home/onyxia/work/ais-pipeline")

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from sqlalchemy import create_engine, text

from config import (
    OUT_TRAFFIC_ALL, OUT_TRAFFIC_INBOUND, OUT_TRAFFIC_OUTBOUND,
    OUT_VESSEL_HORMUZ, OUT_VESSEL_NO_HORMUZ, OUT_ARRIVAL_RECAP,
    OUT_TIME_TRAVEL,
    SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD,
)

if not SQL_USERNAME or not SQL_PASSWORD:
    raise EnvironmentError(
        "BPS_SQLSERVER_USER dan BPS_SQLSERVER_PASS harus diset "
        "di Onyxia Vault Secrets."
    )

CONN_STRING = (
    f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}"
    f"@{SQL_SERVER}/{SQL_DATABASE}"
    f"?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
)

# Mapping: path S3 → tabel SQL Server
TABLES = {
    OUT_TRAFFIC_ALL:      "ais.port_traffic_all",
    OUT_TRAFFIC_INBOUND:  "ais.port_traffic_inbound",
    OUT_TRAFFIC_OUTBOUND: "ais.port_traffic_outbound",
    OUT_VESSEL_HORMUZ:    "ais.vessel_through_hormuz",
    OUT_VESSEL_NO_HORMUZ: "ais.vessel_not_through_hormuz",
    OUT_ARRIVAL_RECAP:    "ais.arrival_recap_hormuz",
    OUT_TIME_TRAVEL:      "ais.time_travel_hormuz",
}

# ── Helper: baca Parquet dari S3 ─────────────────────────────────────────────

def read_parquet_s3(s3_path: str) -> pd.DataFrame:
    path   = s3_path.replace("s3a://", "")
    bucket = path.split("/")[0]
    key    = "/".join(path.split("/")[1:])
    s3     = boto3.client("s3")
    obj    = s3.get_object(Bucket=bucket, Key=key)
    df     = pd.read_parquet(BytesIO(obj["Body"].read()))
    print(f"  Dibaca: {s3_path} ({len(df):,} baris)")
    return df

# ── Koneksi SQL Server ────────────────────────────────────────────────────────

print(f"Koneksi ke SQL Server: {SQL_SERVER}/{SQL_DATABASE}")
engine = create_engine(CONN_STRING, fast_executemany=True)

with engine.connect() as conn:
    conn.execute(text("SELECT 1"))
    print("Koneksi SQL Server berhasil.")

# ── Load tiap tabel ───────────────────────────────────────────────────────────

for s3_path, table_name in TABLES.items():
    schema, tabel = table_name.split(".")
    print(f"\nMemuat → [{schema}].[{tabel}]...")

    try:
        df = read_parquet_s3(s3_path)
        df["loaded_at"] = datetime.now()

        # Truncate tabel dulu, lalu insert ulang dari awal
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            conn.commit()

        df.to_sql(
            name=tabel, schema=schema, con=engine,
            if_exists="append", index=False,
            chunksize=1000, method="multi",
        )
        print(f"  Berhasil: {len(df):,} baris → {table_name}")

    except Exception as e:
        print(f"  GAGAL memuat {s3_path}: {e}")
        raise

print("\nSemua tabel berhasil dimuat ke SQL Server BPS.")