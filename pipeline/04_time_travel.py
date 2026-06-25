"""
pipeline/04_time_travel.py
Analisis Time Travel — kapal Indonesia yang melewati Selat Hormuz:
  1. Baca data AIS clean
  2. Filter kapal yang melewati polygon Selat Hormuz (via H3)
  3. Hitung waktu perjalanan (time travel) per kapal
  4. Simpan hasil ke S3 personal
"""

import os
from datetime import datetime, timedelta

import h3
import geopandas as gpd
from shapely.geometry import Polygon
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
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
IN_PATH   = f"{SAVE_PATH}/clean/data-ais-indonesia-clean-{start_str}-{end_str}.parquet"
OUT_PATH  = f"{SAVE_PATH}/hasil/time_travel_hormuz_{start_str}_{end_str}.parquet"

H3_RESOLUTION = 8

# Polygon Selat Hormuz
POLYGON_HORMUZ = Polygon([
    [54.7, 24.9], [54.7, 26.0], [54.9, 26.6],
    [55.5, 27.0], [56.5, 27.2], [56.5, 26.5],
    [56.1, 26.1], [55.7, 25.7], [54.7, 24.9],
])

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

# ── Step 1: H3 polyfill Selat Hormuz ─────────────────────────────────────────

print("Polyfill H3 Selat Hormuz...")
gdf_hormuz = gpd.GeoDataFrame(
    {"name": ["Hormuz"]}, geometry=[POLYGON_HORMUZ], crs="EPSG:4326"
)

h3_hormuz_int = []
for _, row in gdf_hormuz.iterrows():
    geom  = row.geometry.__geo_interface__
    cells = h3.polyfill(geom, H3_RESOLUTION, geo_json_conformant=True)
    h3_hormuz_int.extend([h3.string_to_h3(c) for c in cells])

h3_hormuz_int = list(set(h3_hormuz_int))
print(f"H3 cells Hormuz: {len(h3_hormuz_int)}")

# ── Step 2: Baca AIS clean ────────────────────────────────────────────────────

print(f"Membaca {IN_PATH}...")
df_ais = spark.read.parquet(IN_PATH)

# ── Step 3: Filter kapal yang lewat Hormuz ────────────────────────────────────

print("Filter kapal yang melewati Selat Hormuz...")
df_through_hormuz = af.apply_small_filter(
    spark, df_ais, h3_hormuz_int, "H3_int_index_8"
)
mmsi_through_hormuz = (
    df_through_hormuz.select("mmsi").distinct().collect()
)
mmsi_list = [row["mmsi"] for row in mmsi_through_hormuz]
print(f"Kapal yang melewati Hormuz: {len(mmsi_list):,}")

# ── Step 4: Ambil seluruh track kapal tersebut ────────────────────────────────

print("Ambil seluruh track kapal via broadcast join...")
df_filtered = af.apply_small_filter(spark, df_ais, mmsi_list, "mmsi")

# ── Step 5: Assign route berdasarkan area ─────────────────────────────────────

# Buat label area: "Hormuz" atau None
hormuz_df = spark.createDataFrame(
    [{"boundary_h3": c, "area_name": "Selat Hormuz"} for c in h3_hormuz_int]
)

df_labeled = df_filtered.join(
    F.broadcast(hormuz_df),
    df_filtered["H3_int_index_8"] == hormuz_df["boundary_h3"],
    how="left",
).withColumn(
    "polygon_name",
    F.coalesce(F.col("area_name"), F.lit("Laut Lepas"))
)

df_routed = af.assign_route(
    df_labeled,
    ship_unique_identifier_cols=["mmsi"],
    route_order_by_cols=["dt_pos_utc"],
)

# ── Step 6: Hitung time travel ────────────────────────────────────────────────

print("Hitung waktu perjalanan...")
df_agg = af.agg_route(
    df_routed,
    group_by_cols=["mmsi", "route_group", "polygon_name", "vessel_type_main"],
    order_by_cols=["dt_pos_utc"],
    num_agg_cols=["sog"],
    fl_agg_cols=["dt_pos_utc", "vessel_name", "destination"],
    checker=False,
)

# Durasi tiap segmen dalam jam
df_agg = df_agg.withColumn(
    "durasi_jam",
    (
        F.unix_timestamp("departure_dt_pos_utc") -
        F.unix_timestamp("arrival_dt_pos_utc")
    ) / 3600,
)

# Filter hanya segmen di Hormuz
df_hormuz_only = df_agg.filter(F.col("polygon_name") == "Selat Hormuz")
print(f"Total event lewat Hormuz: {df_hormuz_only.count():,}")

# ── Step 7: Simpan hasil ──────────────────────────────────────────────────────

print(f"Menyimpan ke {OUT_PATH}...")
df_hormuz_only.write.mode("overwrite").parquet(OUT_PATH)
print("Time travel selesai.")
