"""
pipeline/00_ingest.py
Ingest data AIS Indonesia dari S3 UNGP:
  1. Polyfill H3 res-5 pada EEZ Indonesia
  2. get_ais() by H3 list → dapat MMSI yang ada di wilayah Indonesia
  3. get_ais() ulang by MMSI list → data lengkap per kapal
  4. Simpan ke S3 personal (landing zone)
"""

import os
from datetime import datetime, timedelta

import geopandas as gpd
import h3
from pyspark.sql import SparkSession
from ais import functions as af

# ── Konfigurasi ───────────────────────────────────────────────────────────────

# Rentang tanggal: default 1 minggu terakhir
# Bisa di-override via environment variable untuk backfill
START_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_START_DATE",
                   (datetime.now() - timedelta(days=9)).strftime("%Y-%m-%d"))
)
END_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_END_DATE",
                   (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"))
)

working_dir   = os.environ["AWS_WORKING_DIRECTORY_PATH"]
SAVE_PATH     = f"s3a://{working_dir}iran_usa_conflict/"
H3_RESOLUTION = 5

print(f"Periode: {START_DATE.date()} s/d {END_DATE.date()}")
print(f"Output: {SAVE_PATH}")

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()
print(f"Spark Connect: {spark.version}")

# ── Step 1: Polyfill H3 pada EEZ Indonesia ───────────────────────────────────

print("Membaca EEZ Indonesia...")
land_eez = gpd.read_file(
    "/vsicurl/https://github.com/nandyarz/ais/raw/main/land-eez/EEZ_Land_v3_202030.shp"
)
indo_eez = land_eez[land_eez["TERRITORY1"] == "Indonesia"]

print(f"Polyfill H3 resolusi {H3_RESOLUTION}...")
h3_indeces_int = []
for _, row in indo_eez.iterrows():
    geom = row.geometry.__geo_interface__
    h3_cells = h3.polyfill(geom, H3_RESOLUTION, geo_json_conformant=True)
    h3_indeces_int.extend([h3.string_to_h3(c) for c in h3_cells])

h3_indeces_int = list(set(h3_indeces_int))
print(f"Total H3 cells: {len(h3_indeces_int):,}")

# ── Step 2: get_ais by H3 → dapat MMSI unik ──────────────────────────────────

print("Tahap 1: get_ais by H3 list...")
ais_h3 = af.get_ais(
    spark,
    start_date=START_DATE,
    end_date=END_DATE,
    h3_list=h3_indeces_int,
)

unique_mmsi_df   = ais_h3.select("mmsi").distinct()
unique_mmsi_list = [row["mmsi"] for row in unique_mmsi_df.collect()]
print(f"MMSI unik ditemukan: {len(unique_mmsi_list):,}")

# ── Step 3: get_ais by MMSI → data lengkap ───────────────────────────────────

print("Tahap 2: get_ais by MMSI list...")
data_ais = af.get_ais(
    spark,
    start_date=START_DATE,
    end_date=END_DATE,
    mmsi_list=unique_mmsi_list,
)
row_count = data_ais.count()
print(f"Total baris data AIS: {row_count:,}")

# ── Step 4: Simpan ke S3 personal ────────────────────────────────────────────

start_str = START_DATE.strftime("%d%b%Y").lower()
end_str   = END_DATE.strftime("%d%b%Y").lower()
out_path  = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-{start_str}-{end_str}.parquet"

print(f"Menyimpan ke {out_path}...")
(
    data_ais
    .write
    .mode("overwrite")
    .parquet(out_path)
)
print("Ingest selesai.")
