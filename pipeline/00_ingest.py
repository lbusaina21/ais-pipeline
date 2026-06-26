"""
pipeline/00_ingest.py
Ingest data AIS Indonesia dari S3 UNGP:
  1. Polyfill H3 res-5 pada EEZ Indonesia
  2. get_ais() by H3 list → dapat MMSI yang ada di wilayah Indonesia
  3. get_ais() ulang by MMSI list → data lengkap per kapal
  4. Simpan ke S3 personal (landing zone)
"""

import sys
sys.path.append("/home/onyxia/work/ais-pipeline")

import h3
import geopandas as gpd
from pyspark.sql import SparkSession
from ais import functions as af

from config import START_DATE, END_DATE, SAVE_PATH, IN_RAW, H3_RESOLUTION_EEZ

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()
print(f"Spark Connect: {spark.version}")
print(f"Periode: {START_DATE.date()} s/d {END_DATE.date()}")
print(f"Output: {IN_RAW}")

# ── Step 1: Polyfill H3 pada EEZ Indonesia ───────────────────────────────────

print("Membaca EEZ Indonesia...")
land_eez = gpd.read_file(
    "/vsicurl/https://github.com/nandyarz/ais/raw/main/land-eez/EEZ_Land_v3_202030.shp"
)
indo_eez = land_eez[land_eez["TERRITORY1"] == "Indonesia"]

print(f"Polyfill H3 resolusi {H3_RESOLUTION_EEZ}...")
h3_indeces_int = []
for _, row in indo_eez.iterrows():
    geom   = row.geometry.__geo_interface__
    cells  = h3.polyfill(geom, H3_RESOLUTION_EEZ, geo_json_conformant=True)
    h3_indeces_int.extend([h3.string_to_h3(c) for c in cells])

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

unique_mmsi_list = [row["mmsi"] for row in ais_h3.select("mmsi").distinct().collect()]
print(f"MMSI unik ditemukan: {len(unique_mmsi_list):,}")

# ── Step 3: get_ais by MMSI → data lengkap ───────────────────────────────────

print("Tahap 2: get_ais by MMSI list...")
data_ais = af.get_ais(
    spark,
    start_date=START_DATE,
    end_date=END_DATE,
    mmsi_list=unique_mmsi_list,
)
print(f"Total baris data AIS: {data_ais.count():,}")

# ── Step 4: Simpan ke S3 personal ────────────────────────────────────────────

print(f"Menyimpan ke {IN_RAW}...")
data_ais.write.mode("overwrite").parquet(IN_RAW)
print("Ingest selesai.")
