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
  8. Pilih kolom → simpan data detail periode ini
  9. Concat dengan periode sebelumnya → simpan data akumulatif
"""

import sys
sys.path.append("/home/onyxia/work/ais-pipeline")

from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from ais import functions as af

from config import (
    IN_RAW, OUT_DETAIL, OUT_ACCUM, PREV_ACCUM,
    VESSEL_TYPE_FIXES, SELECT_COLS,
)

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

# ── Step 1: Baca data mentah ──────────────────────────────────────────────────

print(f"Membaca data dari {IN_RAW}...")
data_ais = spark.read.parquet(IN_RAW)
print(f"Baris sebelum dedup: {data_ais.count():,}")

# ── Step 2: Deduplikasi ───────────────────────────────────────────────────────

data_ais = data_ais.distinct()
print(f"Baris setelah dedup: {data_ais.count():,}")

# ── Step 3: Join IHS ship register ───────────────────────────────────────────

print("Membaca IHS ship register...")
ihs = af.read_ihs_table(spark, "ShipData.CSV")

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

# ── Step 4: Filter hanya matched ─────────────────────────────────────────────

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
record_counts     = matched_only.groupBy("mmsi").agg(count("*").alias("record_count"))
mmsi_less_than_10 = record_counts.filter(col("record_count") < 10).select("mmsi")
mmsi_rec_gt_10    = matched_only.join(mmsi_less_than_10, on="mmsi", how="left_anti")
print(f"Baris setelah filter record < 10: {mmsi_rec_gt_10.count():,}")

# ── Step 7: Filter SOG > 3 berjumlah < 20 per MMSI ──────────────────────────

print("Filter MMSI dengan SOG > 3 berjumlah < 20...")
filtered_data  = mmsi_rec_gt_10.filter(col("sog") > 3)
grouped_data   = filtered_data.groupBy("mmsi").agg(count("*").alias("record_count"))
filtered_mmsi  = grouped_data.filter(col("record_count") < 20).select("mmsi")
data_ais_clean = mmsi_rec_gt_10.join(filtered_mmsi, "mmsi", "left_anti")
print(f"Baris setelah filter SOG: {data_ais_clean.count():,}")

# ── Step 8: Pilih kolom → simpan detail periode ini ──────────────────────────

data_ais_detail = data_ais_clean.select(SELECT_COLS)

print(f"Menyimpan detail periode ini ke {OUT_DETAIL}...")
data_ais_detail.write.mode("overwrite").parquet(OUT_DETAIL)
print(f"Tersimpan: {data_ais_detail.count():,} baris")

# ── Step 9: Concat dengan periode sebelumnya → simpan akumulatif ─────────────

if PREV_ACCUM:
    print(f"Membaca data akumulatif sebelumnya dari {PREV_ACCUM}...")
    try:
        prev_df  = spark.read.parquet(PREV_ACCUM)
        accum_df = prev_df.union(data_ais_detail)
        print(f"Total setelah concat: {accum_df.count():,} baris")
    except Exception as e:
        print(f"Tidak bisa baca data sebelumnya ({e}), pakai data periode ini saja.")
        accum_df = data_ais_detail
else:
    print("Tidak ada data akumulatif sebelumnya, pakai data periode ini saja.")
    accum_df = data_ais_detail

print(f"Menyimpan akumulatif ke {OUT_ACCUM}...")
accum_df.write.mode("overwrite").parquet(OUT_ACCUM)
print(f"Preprocessing selesai. Total akumulatif: {accum_df.count():,} baris")
