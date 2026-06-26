"""
pipeline/04_time_travel.py
Analisis Time Travel — Selat Hormuz ke Indonesia:
  1. Baca data AIS clean (akumulatif)
  2. Load EEZ Indonesia + polyfill H3 res-8
  3. Polyfill H3 Selat Hormuz res-8
  4. Tag tiap sinyal: is_hormuz, is_indo
  5. Drop kolom tidak perlu
  6. Detect exit Hormuz & entry Indonesia per kapal
  7. Hitung travel time (exit Hormuz → entry Indo)
  8. Join metadata kapal → simpan hasil
"""

import sys
sys.path.append("/home/onyxia/work/ais-pipeline")

import h3
import geopandas as gpd
from shapely.geometry import Polygon, mapping
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lag, broadcast,
    min as spark_min,
    max as spark_max,
)

from config import OUT_ACCUM, REF_POLYGON_HORMUZ, REF_EEZ_LAND, OUT_TIME_TRAVEL

POLYGON_HORMUZ = Polygon(REF_POLYGON_HORMUZ)

COLS_DROP_1 = [
    "H3_int_index_8", "nav_status",
    "GroupBeneficialOwnerCountryofDomicile",
    "GroupBeneficialOwnerCountryOfRegistration",
    "RegisteredOwnerCountryofDomicile",
]
COLS_DROP_2 = ["latitude", "longitude", "sog", "vessel_name"]
COLS_DROP_3 = ["imo"]

def polygon_to_h3(polygon, res=8):
    return h3.polyfill(
        polygon.__geo_interface__,
        res=res,
        geo_json_conformant=True,
    )

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

# ── Step 1: Baca AIS clean akumulatif ────────────────────────────────────────

print(f"Membaca {OUT_ACCUM}...")
df_ais = spark.read.parquet(OUT_ACCUM)

# ── Step 2: Polyfill H3 EEZ Indonesia ────────────────────────────────────────

print("Membaca EEZ Indonesia...")
land_eez  = gpd.read_file(REF_EEZ_LAND)
indo_eez  = land_eez[land_eez["TERRITORY1"] == "Indonesia"]
indo_geom = indo_eez.unary_union

print("Polyfill H3 res=8 EEZ Indonesia...")
h3_set_indo = {h3.string_to_h3(x) for x in polygon_to_h3(indo_geom, 8)}
print(f"Indo   res=8: {len(h3_set_indo):,} cells")

# ── Step 3: Polyfill H3 Selat Hormuz ─────────────────────────────────────────

h3_set_hormuz = {h3.string_to_h3(x) for x in polygon_to_h3(POLYGON_HORMUZ, 8)}
print(f"Hormuz res=8: {len(h3_set_hormuz):,} cells")

# ── Step 4: Tag sinyal is_hormuz & is_indo ───────────────────────────────────

print("Tagging sinyal...")
h3_indo_df = (
    spark.createDataFrame([(int(x),) for x in h3_set_indo], ["H3_int_index_8"])
    .withColumn("is_indo", F.lit(True))
)
h3_hormuz_df = (
    spark.createDataFrame([(int(x),) for x in h3_set_hormuz], ["H3_int_index_8"])
    .withColumn("is_hormuz", F.lit(True))
)

df = (
    df_ais
    .join(broadcast(h3_hormuz_df), "H3_int_index_8", "left")
    .join(broadcast(h3_indo_df),   "H3_int_index_8", "left")
    .withColumn("is_hormuz", F.coalesce(col("is_hormuz"), F.lit(False)))
    .withColumn("is_indo",   F.coalesce(col("is_indo"),   F.lit(False)))
)

# ── Step 5: Drop kolom tidak perlu ───────────────────────────────────────────

df = df.drop(*COLS_DROP_1).drop(*COLS_DROP_2).drop(*COLS_DROP_3)

# ── Step 6: Detect exit Hormuz & entry Indonesia ──────────────────────────────

print("Detect exit Hormuz & entry Indonesia...")
w = Window.partitionBy("mmsi").orderBy("dt_pos_utc")

df_detect = (
    df
    .withColumn(
        "hormuz_exit",
        (lag("is_hormuz").over(w) == True) & (col("is_hormuz") == False)
    )
    .withColumn(
        "indo_entry",
        (lag("is_indo").over(w) == False) & (col("is_indo") == True)
    )
)

# Entry Indo: ambil timestamp pertama masuk Indonesia
indo_entry_time = (
    df_detect
    .filter("indo_entry")
    .groupBy("mmsi")
    .agg(spark_min("dt_pos_utc").alias("t_indo"))
)

# Exit Hormuz: ambil exit terakhir sebelum masuk Indo
df_join = df_detect.join(indo_entry_time, "mmsi")

hormuz_exit_time = (
    df_join
    .filter(col("hormuz_exit") & (col("dt_pos_utc") < col("t_indo")))
    .groupBy("mmsi")
    .agg(spark_max("dt_pos_utc").alias("t_hormuz"))
)

# ── Step 7: Hitung travel time ────────────────────────────────────────────────

print("Hitung travel time...")
result = (
    hormuz_exit_time
    .join(indo_entry_time, "mmsi")
    .filter(col("t_indo") > col("t_hormuz"))
    .withColumn(
        "travel_time_hours",
        (col("t_indo").cast("long") - col("t_hormuz").cast("long")) / 3600
    )
)

# ── Step 8: Join metadata kapal → simpan ─────────────────────────────────────

vessel_meta = (
    df_ais
    .select("mmsi", "vessel_type", "flag_country", "draught")
    .dropDuplicates(["mmsi"])
)

df_result = (
    result
    .join(vessel_meta, "mmsi")
    .select(
        "mmsi", "vessel_type", "flag_country", "draught",
        "t_hormuz", "t_indo", "travel_time_hours",
    )
)

print(f"Total kapal dengan time travel terdeteksi: {df_result.count():,}")

print(f"Menyimpan ke {OUT_TIME_TRAVEL}...")
df_result.write.mode("overwrite").parquet(OUT_TIME_TRAVEL)
print("Time travel selesai.")
