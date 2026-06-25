"""
pipeline/05_load_sqlserver.py
Load hasil analisis ke SQL Server BPS on-prem:
  - port_traffic  → tabel ais.port_traffic
  - port_call     → tabel ais.port_call
  - time_travel   → tabel ais.time_travel_hormuz

Koneksi SQL Server via SQLAlchemy + pyodbc.
Credentials dari environment variable (disimpan di Onyxia Vault/Secrets).
"""

import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from io import BytesIO

# ── Konfigurasi ───────────────────────────────────────────────────────────────

START_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_START_DATE",
                   (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d"))
)
END_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_END_DATE",
                   (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))
)

working_dir = os.environ["AWS_WORKING_DIRECTORY_PATH"]
SAVE_PATH   = f"s3a://{working_dir}/iran_usa_conflict/hasil/"

start_str = START_DATE.strftime("%d%b%Y").lower()
end_str   = END_DATE.strftime("%d%b%Y").lower()

# SQL Server BPS — credentials dari Onyxia Vault (env vars)
SQL_SERVER   = os.environ.get("BPS_SQLSERVER_HOST", "NOVA.ms.bps.go.id")
SQL_DATABASE = os.environ.get("BPS_SQLSERVER_DB",   "sd_web_scraping")
SQL_USERNAME = os.environ.get("BPS_SQLSERVER_USER")
SQL_PASSWORD = os.environ.get("BPS_SQLSERVER_PASS")

if not SQL_USERNAME or not SQL_PASSWORD:
    raise EnvironmentError(
        "BPS_SQLSERVER_USER dan BPS_SQLSERVER_PASS harus diset "
        "di Onyxia Vault Secrets."
    )

CONN_STRING = (
    f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}"
    f"@{SQL_SERVER}/{SQL_DATABASE}"
    f"?driver=ODBC+Driver+17+for+SQL+Server"
    f"&TrustServerCertificate=yes"
)

# Mapping: file Parquet → tabel SQL Server
TABLES = {
    f"port_traffic_{start_str}_{end_str}.parquet": "ais.port_traffic",
    f"port_call_{start_str}_{end_str}.parquet":    "ais.port_call",
    f"time_travel_hormuz_{start_str}_{end_str}.parquet": "ais.time_travel_hormuz",
}

# ── Helper: baca Parquet dari S3 via boto3 ────────────────────────────────────

def read_parquet_from_s3(filename: str) -> pd.DataFrame:
    """
    Baca file Parquet dari S3 personal menggunakan boto3
    (credentials dari environment variable yang di-inject Onyxia).
    """
    bucket = working_dir.split("/")[0]
    prefix = "/".join(working_dir.split("/")[1:])
    key    = f"{prefix}ais-indonesia/hasil/{filename}"

    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = BytesIO(obj["Body"].read())
    df  = pd.read_parquet(buf)
    print(f"  Dibaca: s3://{bucket}/{key} ({len(df):,} baris)")
    return df

# ── Tambahkan metadata kolom ──────────────────────────────────────────────────

def tambah_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df["pipeline_start_date"] = START_DATE.date()
    df["pipeline_end_date"]   = END_DATE.date()
    df["loaded_at"]           = datetime.now()
    return df

# ── Load ke SQL Server ────────────────────────────────────────────────────────

print(f"Koneksi ke SQL Server: {SQL_SERVER}/{SQL_DATABASE}")
engine = create_engine(CONN_STRING, fast_executemany=True)

# Test koneksi
with engine.connect() as conn:
    conn.execute(text("SELECT 1"))
    print("Koneksi SQL Server berhasil.")

for filename, table_name in TABLES.items():
    schema, tabel = table_name.split(".")
    print(f"\nMemuat {filename} → [{schema}].[{tabel}]...")

    try:
        df = read_parquet_from_s3(filename)
        df = tambah_metadata(df)

        # Hapus data periode yang sama dulu (upsert by date range)
        with engine.connect() as conn:
            conn.execute(text(f"""
                DELETE FROM {table_name}
                WHERE pipeline_start_date = :start
                  AND pipeline_end_date   = :end
            """), {"start": START_DATE.date(), "end": END_DATE.date()})
            conn.commit()

        # Insert baru
        df.to_sql(
            name=tabel,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
        print(f"  Berhasil: {len(df):,} baris dimuat ke {table_name}")

    except Exception as e:
        print(f"  GAGAL memuat {filename}: {e}")
        raise

print("\nSemua tabel berhasil dimuat ke SQL Server BPS.")
