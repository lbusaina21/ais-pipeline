"""
pipeline/02_port_traffic.py
Analisis port traffic:
  1. Baca data AIS clean
  2. Load AOI port Indonesia (manual) → H3 explode resolusi 8
  3. Join AIS dengan port via H3_int_index_8
  4. Agregasi: jumlah vessel per port per hari, per tipe kapal
  5. Simpan hasil ke S3 personal
"""

import os
from datetime import datetime, timedelta
from shapely.geometry import mapping

import h3
import geopandas as gpd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
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
OUT_PATH  = f"{SAVE_PATH}/hasil/port_traffic_{start_str}_{end_str}.parquet"

H3_RESOLUTION = 8

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

# ── Helper ────────────────────────────────────────────────────────────────────

def polygon_to_h3_int(geometry, resolution=8):
    geo_json = mapping(geometry)
    h3_set   = h3.polyfill_geojson(geo_json, resolution)
    return [int(c, 16) for c in h3_set]

# ── Step 1: Baca AIS clean ────────────────────────────────────────────────────

print(f"Membaca {IN_PATH}...")
df_ais = spark.read.parquet(IN_PATH)

# ── Step 2: Load AOI port Indonesia ──────────────────────────────────────────

print("Membaca AOI port dari S3 personal...")
port_raw = spark.read.parquet(f"{SAVE_PATH}ref/port_indonesia_manual.parquet")

# Jika belum ada, buat dari shapefile/GeoJSON yang sudah diupload ke S3
# (asumsi file GeoJSON port sudah ada di S3 personal sebagai referensi statis)
try:
    port_raw.count()
    print("AOI port ditemukan di S3.")
except Exception:
    raise RuntimeError(
        "File port_indonesia_manual.parquet tidak ditemukan di S3 personal. "
        "Jalankan script setup_reference_data.py terlebih dahulu."
    )

# Explode boundary_h3 supaya 1 baris per H3 cell
port_exploded = port_raw.select(
    "Port", "Prov_Port", "Ket_Port", "Port_Country",
    F.explode("boundary_h3").alias("boundary_h3")
)

# ── Step 3: Join AIS dengan port via H3 ──────────────────────────────────────

print("Join AIS dengan port via H3_int_index_8...")
df_joined = df_ais.join(
    F.broadcast(port_exploded),
    df_ais["H3_int_index_8"] == port_exploded["boundary_h3"],
    how="inner",
)

# ── Step 4: Agregasi port traffic ─────────────────────────────────────────────

print("Agregasi port traffic...")
df_traffic = (
    df_joined
    .withColumn("tanggal", F.to_date("dt_pos_utc"))
    .groupBy("Port", "Prov_Port", "Ket_Port", "tanggal", "vessel_type_main")
    .agg(
        F.countDistinct("mmsi").alias("jumlah_vessel"),
        F.count("*").alias("jumlah_sinyal"),
        F.avg("sog").alias("rata_kecepatan"),
    )
    .orderBy("tanggal", "Port")
)

print(f"Total baris hasil: {df_traffic.count():,}")

# ── Step 5: Simpan hasil ──────────────────────────────────────────────────────

print(f"Menyimpan ke {OUT_PATH}...")
df_traffic.write.mode("overwrite").parquet(OUT_PATH)
print("Port traffic selesai.")
