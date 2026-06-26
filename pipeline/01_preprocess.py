"""
pipeline/01_preprocess.py
Preprocessing data AIS mentah:
  1. Baca dari S3 personal (landing zone)
  2. Deduplikasi (distinct)
  3. Join IHS ship register (af.match_ais_ihs)
  4. Filter hanya matched (ihs_matchtype != 9)
  5. Manual fix vessel_type untuk kapal tertentu
  6. Filter record < 10 per MMSI
  7. Filter SOG > 3 berjumlah < 20 per MMSI
  8. Pilih kolom yang sesuai → simpan sebagai data detail periode ini
  9. Concat dengan data periode sebelumnya → simpan sebagai data akumulatif
"""

import os
from datetime import datetime, timedelta
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from ais import functions as af

# ── Konfigurasi ───────────────────────────────────────────────────────────────

END_ACCUM_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_END_DATE",
                   (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"))
)
START_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_START_DATE",
                   (datetime.now() - timedelta(days=9)).strftime("%Y-%m-%d"))
)
END_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_END_DATE",
                   (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"))
)

working_dir = os.environ["AWS_WORKING_DIRECTORY_PATH"]
SAVE_PATH   = f"s3a://{working_dir}iran_usa_conflict/"

end_accum_str   = END_ACCUM_DATE.strftime("%d%b%Y").lower()
start_str = START_DATE.strftime("%d%b%Y").lower()
end_str   = END_DATE.strftime("%d%b%Y").lower()

IN_PATH      = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-{start_str}-{end_str}.parquet"

# Output periode ini (detail)
OUT_DETAIL   = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-detail-{start_str}-{end_str}.parquet"

# Output akumulatif (concat dengan periode sebelumnya)
# Nama file akumulatif menggunakan start dari env var PIPELINE_ACCUM_START
# Default: sama dengan start periode ini (pertama kali jalan)
ACCUM_START  = os.environ.get("PIPELINE_ACCUM_START", start_str)
OUT_ACCUM    = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-detail-{ACCUM_START}-{end_str}.parquet"

# Path data akumulatif sebelumnya (None kalau pertama kali jalan)
PREV_ACCUM   = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-detail-{ACCUM_START}-{end_accum_str}.parquet"

# Manual fix: kapal yang vessel_type-nya salah di register
VESSEL_TYPE_FIXES = [
    ("PERTAMINA GAS DAHLIA",  "Gas Tanker",             "Lpg Tanker"),
    ("PERTAMINA PRIDE",       "Oil And Chemical Tanker", "Crude Oil Tanker"),
    ("MARINA AMAN",           "Oil And Chemical Tanker", "Crude Oil Tanker"),
    ("SC OCEAN LXI",          "Oil And Chemical Tanker", "Crude Oil Tanker"),
    ("MBS BULELENG",          "Bulk Carrier",            None),
    ("SUCCESS FORTUNE XL",    "Oil And Chemical Tanker", "Crude Oil Tanker"),
]

# Kolom output final
SELECT_COLS = [
    "mmsi", "imo", "vessel_name",
    "vessel_type", "vessel_type_main", "vessel_type_sub",
    "flag_country",
    "RegisteredOwnerCountryofDomicile",
    "GroupBeneficialOwnerCountryOfRegistration",
    "GroupBeneficialOwnerCountryofDomicile",
    "length", "LengthOverallLOA", "width", "BreadthExtreme", "Depth", "draught",
    "GrossTonnage", "NetTonnage", "Deadweight", "LightDisplacementTonnage",
    "nav_status", "heading", "sog",
    "dt_pos_utc", "latitude", "longitude", "H3_int_index_8",
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

# Cek dan rename kolom duplikat di IHS supaya tidak konflik
ais_cols_lower = set([c.lower() for c in data_ais.columns])
ihs_cols_lower = set([c.lower() for c in ihs.columns])
common_cols    = ais_cols_lower.intersection(ihs_cols_lower)
print(f"Kolom duplikat: {common_cols}")

ihs_renamed = reduce(
    lambda df, c: df.withColumnRenamed(c, f"ihs_{c}"),
    common_cols,
    ihs,
)

print("Matching AIS dengan IHS register...")
matched = af.match_ais_ihs(data_ais, ihs_renamed, return_all=True)
print("Match selesai.")

# ── Step 4: Filter hanya matched (buang ihs_matchtype == 9) ──────────────────

matched_only = matched.filter(matched.ihs_matchtype != 9)
print(f"Baris setelah filter matched: {matched_only.count():,}")

# ── Step 5: Manual fix vessel_type ───────────────────────────────────────────

print("Applying manual vessel_type fixes...")
for vessel_name, type_main, type_sub in VESSEL_TYPE_FIXES:
    matched_only = matched_only.withColumn(
        "vessel_type_main",
        F.when(F.col("vessel_name") == vessel_name, F.lit(type_main))
         .otherwise(F.col("vessel_type_main"))
    )
    if type_sub:
        matched_only = matched_only.withColumn(
            "vessel_type_sub",
            F.when(F.col("vessel_name") == vessel_name, F.lit(type_sub))
             .otherwise(F.col("vessel_type_sub"))
        )

# ── Step 6: Filter record < 10 per MMSI ──────────────────────────────────────

print("Filter MMSI dengan record < 10...")
record_counts    = matched_only.groupBy("mmsi").agg(count("*").alias("record_count"))
mmsi_less_than_10 = record_counts.filter(col("record_count") < 10).select("mmsi")
mmsi_with_record_great_10 = matched_only.join(mmsi_less_than_10, on="mmsi", how="left_anti")
print(f"Baris setelah filter record < 10: {mmsi_with_record_great_10.count():,}")

# ── Step 7: Filter SOG > 3 berjumlah < 20 per MMSI ──────────────────────────

print("Filter MMSI dengan SOG > 3 berjumlah < 20...")
filtered_data  = mmsi_with_record_great_10.filter(col("sog") > 3)
grouped_data   = filtered_data.groupBy("mmsi").agg(count("*").alias("record_count"))
filtered_mmsi  = grouped_data.filter(col("record_count") < 20).select("mmsi")
data_ais_clean = mmsi_with_record_great_10.join(filtered_mmsi, "mmsi", "left_anti")
print(f"Baris setelah filter SOG: {data_ais_clean.count():,}")

# ── Step 8: Pilih kolom → simpan data detail periode ini ─────────────────────

print("Memilih kolom output...")
data_ais_detail = data_ais_clean.select(SELECT_COLS)

print(f"Menyimpan detail periode ini ke {OUT_DETAIL}...")
data_ais_detail.write.mode("overwrite").parquet(OUT_DETAIL)
print(f"Tersimpan: {data_ais_detail.count():,} baris")

# ── Step 9: Concat dengan periode sebelumnya → simpan akumulatif ─────────────

if PREV_ACCUM:
    print(f"Membaca data akumulatif sebelumnya dari {PREV_ACCUM}...")
    try:
        prev_df   = spark.read.parquet(PREV_ACCUM)
        accum_df  = prev_df.union(data_ais_detail)
        print(f"Total setelah concat: {accum_df.count():,} baris")
    except Exception as e:
        print(f"Tidak bisa baca data sebelumnya ({e}), pakai data periode ini saja.")
        accum_df = data_ais_detail
else:
    print("Tidak ada data akumulatif sebelumnya, pakai data periode ini saja.")
    accum_df = data_ais_detail

print(f"Menyimpan data akumulatif ke {OUT_ACCUM}...")
accum_df.write.mode("overwrite").parquet(OUT_ACCUM)
print(f"Preprocessing selesai. Total akumulatif: {accum_df.count():,} baris")