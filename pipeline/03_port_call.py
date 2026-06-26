"""
pipeline/03_port_call.py
Analisis Port Call Indonesia:
  1. Baca data AIS clean (akumulatif)
  2. Load AOI port via read_aoi('Manual') dari ais_aoi_integrated
  3. Hitung boundary (port call detection) via boundary()
  4. Tag kapal yang lewat Selat Hormuz
  5. Join boundary dengan Hormuz flag
  6. List vessel through/not through Hormuz
  7. Arrival recap with Hormuz tag
  8. Simpan semua hasil ke S3 personal
"""

import sys
sys.path.append("/home/onyxia/work/ais-pipeline")

import h3
import pandas as pd
from shapely.geometry import Polygon
from shapely.geometry import mapping
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, broadcast

from ais_aoi_integrated.utils import (
    read_aoi,
    boundary,
    recap_boundary_with_hormuz,
    list_vessel_through_hormuz,
    list_vessel_not_through_hormuz,
)

from config import REF_POLYGON_HORMUZ, OUT_ACCUM, OUT_HORMUZ_TAG, MISSING_COLS, OUT_VESSEL_HORMUZ, OUT_VESSEL_NO_HORMUZ, OUT_ARRIVAL_RECAP

POLYGON_HORMUZ = Polygon(REF_POLYGON_HORMUZ)

def polygon_to_h3(geometry, resolution=8):
    geo_json = mapping(geometry)
    h3_set   = h3.polyfill_geojson(geo_json, resolution)
    return [int(h, 16) for h in h3_set]

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

# ── Step 1: Baca AIS clean akumulatif ────────────────────────────────────────

print(f"Membaca {OUT_ACCUM}...")
df_ais = spark.read.parquet(OUT_ACCUM)

# ── Step 2: Boundary — port call detection via ais_aoi_integrated ─────────────

print("Menghitung boundary (port call detection)...")
boundary_df = boundary(
    ais=df_ais,
    aoi='Manual',
    resolution=8,
    buffer_radius_m=10000,
    spark=spark,
)
print("Boundary selesai.")

# ── Step 3: Tag kapal yang lewat Selat Hormuz ─────────────────────────────────

print("Tagging kapal yang melewati Selat Hormuz...")
h3_set_hormuz = {h3.string_to_h3(x) for x in polygon_to_h3(POLYGON_HORMUZ, 8)}

h3_hormuz_df = (
    spark.createDataFrame([(int(x),) for x in h3_set_hormuz], ["H3_int_index_8"])
    .withColumn("is_hormuz", F.lit(True))
)

df_ais_hormuz_tag = (
    df_ais
    .join(broadcast(h3_hormuz_df), "H3_int_index_8", "left")
    .withColumn("is_hormuz", F.coalesce(col("is_hormuz"), F.lit(False)))
)

# Simpan tagging ke S3
print(f"Menyimpan tagging Hormuz ke {OUT_HORMUZ_TAG}...")
df_ais_hormuz_tag.write.mode("overwrite").parquet(OUT_HORMUZ_TAG)

# ── Step 4: Buat Hormuz flag per MMSI ────────────────────────────────────────

df_mmsi_first_hormuz = (
    df_ais_hormuz_tag
    .filter(col("is_hormuz") == True)
    .groupBy("mmsi", "vessel_type")
    .agg(
        F.min("dt_pos_utc").alias("first_detected_hormuz"),
        F.max("dt_pos_utc").alias("last_detected_hormuz"),
    )
)

df_hormuz_flag = (
    df_ais_hormuz_tag
    .groupBy("mmsi")
    .agg(F.max("is_hormuz").alias("is_through_hormuz"))
)

# ── Step 5: Join boundary dengan Hormuz flag ──────────────────────────────────

print("Join boundary dengan Hormuz flag...")
boundary_with_flag = (
    boundary_df
    .join(df_hormuz_flag, on="mmsi", how="left")
    .fillna({"is_through_hormuz": 0})
    .join(
        df_mmsi_first_hormuz.select("mmsi", "first_detected_hormuz", "last_detected_hormuz"),
        on="mmsi",
        how="left",
    )
)

# ── Step 6: Dedup AIS untuk enrich vessel list ────────────────────────────────

df_ais_dedup = (
    df_ais.select(["mmsi"] + MISSING_COLS)
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("mmsi").orderBy(col("dt_pos_utc").desc())
    ))
    .filter(col("rn") == 1)
    .drop("rn")
)

# ── Step 7a: List vessel through Hormuz ──────────────────────────────────────

print("List vessel through Hormuz...")
vessel_through = (
    list_vessel_through_hormuz(
        ais_activity=boundary_with_flag,
        activity='arrival',
        # month_obs=8,
        # year_obs=2026,
        port_time_limit=30,
        spark=spark,
    )
    .join(df_ais_dedup, on="mmsi", how="left")
    .drop("dt_pos_utc")
)
print(f"Vessel through Hormuz: {vessel_through.count():,}")

# ── Step 7b: List vessel not through Hormuz ───────────────────────────────────

print("List vessel not through Hormuz...")
vessel_not_through = (
    list_vessel_not_through_hormuz(
        ais_activity=boundary_with_flag,
        activity='arrival',
        # month_obs=8,
        # year_obs=2026,
        port_time_limit=30,
        spark=spark,
    )
    .join(df_ais_dedup, on="mmsi", how="left")
    .drop("dt_pos_utc")
)
print(f"Vessel not through Hormuz: {vessel_not_through.count():,}")

# ── Step 8: Arrival recap with Hormuz tag ────────────────────────────────────

print("Arrival recap with Hormuz tag...")
arrival_recap = recap_boundary_with_hormuz(
    ais_activity=boundary_with_flag,
    activity='arrival',
    # month_obs=8,
    # year_obs=2026,
    port_time_limit=30,
    spark=spark,
)

# Format tanggal
arrival_recap_pd = arrival_recap.toPandas()
for date_col in ["date", "first_detected_hormuz", "last_detected_hormuz"]:
    if date_col in arrival_recap_pd.columns:
        arrival_recap_pd[date_col] = (
            pd.to_datetime(arrival_recap_pd[date_col]).dt.strftime('%Y-%m-%d')
        )

arrival_recap_spark = spark.createDataFrame(arrival_recap_pd)

# ── Step 9: Simpan semua hasil ────────────────────────────────────────────────

print("Menyimpan hasil...")
vessel_through.write.mode("overwrite").parquet(OUT_VESSEL_HORMUZ)
vessel_not_through.write.mode("overwrite").parquet(OUT_VESSEL_NO_HORMUZ)
arrival_recap_spark.write.mode("overwrite").parquet(OUT_ARRIVAL_RECAP)

print(f"  → {OUT_VESSEL_HORMUZ}")
print(f"  → {OUT_VESSEL_NO_HORMUZ}")
print(f"  → {OUT_ARRIVAL_RECAP}")
print("Port call selesai.")
