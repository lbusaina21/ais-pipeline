"""
pipeline/01_preprocess.py
Preprocessing data AIS mentah:
  1. Baca dari S3 personal (landing zone)
  2. Deduplikasi (distinct)
  3. Join IHS ship register (af.match_ais_ihs)
  4. Manual fix vessel_type untuk kapal tertentu
  5. Simpan data clean ke S3 personal
"""

import os
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from ais import functions as af

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
SAVE_PATH   = f"s3a://{working_dir}/iran_usa_conflict/"

start_str = START_DATE.strftime("%d%b%Y").lower()
end_str   = END_DATE.strftime("%d%b%Y").lower()
IN_PATH   = f"{SAVE_PATH}/raw/data-ais-indonesia-{start_str}-{end_str}.parquet"
OUT_PATH  = f"{SAVE_PATH}/clean/data-ais-indonesia-clean-{start_str}-{end_str}.parquet"

# Manual fix: kapal yang vessel_type-nya salah di register
VESSEL_TYPE_FIXES = [
    ("PERTAMINA GAS DAHLIA",  "Gas Tanker",              "Lpg Tanker"),
    ("PERTAMINA PRIDE",       "Oil And Chemical Tanker",  "Crude Oil Tanker"),
    ("MARINA AMAN",           "Oil And Chemical Tanker",  "Crude Oil Tanker"),
    ("SC OCEAN LXI",          "Oil And Chemical Tanker",  "Crude Oil Tanker"),
    ("MBS BULELENG",          "Bulk Carrier",             None),
    ("SUCCESS FORTUNE XL",    "Oil And Chemical Tanker",  "Crude Oil Tanker"),
]

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

# ── Step 1: Baca data mentah ──────────────────────────────────────────────────

print(f"Membaca data dari {IN_PATH}...")
data_ais = spark.read.parquet(IN_PATH)
print(f"Baris sebelum dedup: {data_ais.count():,}")

# ── Step 2: Deduplikasi ───────────────────────────────────────────────────────

data_ais = data_ais.distinct()
print(f"Baris setelah dedup: {data_ais.count():,}")

# ── Step 3: Join IHS ship register ───────────────────────────────────────────

print("Membaca IHS ship register...")
ihs = af.read_ihs_table(spark, "ShipData.CSV")

print("Matching AIS dengan IHS register...")
data_ais = af.match_ais_ihs(data_ais, ihs, return_all=True)
print("Match selesai.")

# ── Step 4: Manual fix vessel_type ───────────────────────────────────────────

print("Applying manual vessel_type fixes...")
for vessel_name, type_main, type_sub in VESSEL_TYPE_FIXES:
    data_ais = data_ais.withColumn(
        "vessel_type_main",
        F.when(F.col("vessel_name") == vessel_name, F.lit(type_main))
         .otherwise(F.col("vessel_type_main"))
    )
    if type_sub:
        data_ais = data_ais.withColumn(
            "vessel_type_sub",
            F.when(F.col("vessel_name") == vessel_name, F.lit(type_sub))
             .otherwise(F.col("vessel_type_sub"))
        )

# ── Step 5: Simpan data clean ─────────────────────────────────────────────────

print(f"Menyimpan ke {OUT_PATH}...")
(
    data_ais
    .write
    .mode("overwrite")
    .parquet(OUT_PATH)
)
print(f"Preprocessing selesai. Total baris: {data_ais.count():,}")
