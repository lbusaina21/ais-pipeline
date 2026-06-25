"""
pipeline/03_port_call.py
Analisis port call:
  1. Baca data AIS clean
  2. Deteksi boundary (masuk/keluar area pelabuhan) via af.assign_route + boundary logic
  3. Agregasi: arrival/departure time, lama sandar, per kapal per pelabuhan
  4. Simpan hasil ke S3 personal
"""

import os
from datetime import datetime, timedelta
from shapely.geometry import mapping

import h3
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
OUT_PATH  = f"{SAVE_PATH}/hasil/port_call_{start_str}_{end_str}.parquet"

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

# ── Step 1: Baca AIS clean ────────────────────────────────────────────────────

print(f"Membaca {IN_PATH}...")
df_ais = spark.read.parquet(IN_PATH)

# ── Step 2: Load AOI port → explode H3 ───────────────────────────────────────

print("Membaca AOI port...")
port_exploded = (
    spark.read.parquet(f"{SAVE_PATH}ref/port_indonesia_manual.parquet")
    .select("Port", "Prov_Port", "Ket_Port",
            F.explode("boundary_h3").alias("boundary_h3"))
)

# ── Step 3: Join AIS dengan port → tandai posisi dalam port ──────────────────

print("Join AIS dengan port boundary...")
df_in_port = (
    df_ais
    .join(
        F.broadcast(port_exploded),
        df_ais["H3_int_index_8"] == port_exploded["boundary_h3"],
        how="left",
    )
    .withColumn(
        "polygon_name",
        F.when(F.col("Port").isNotNull(), F.col("Port")).otherwise(F.lit(None))
    )
)

# ── Step 4: Assign route — deteksi kapan kapal masuk/keluar port ──────────────

print("Assign route per kapal...")
df_routed = af.assign_route(
    df_in_port,
    ship_unique_identifier_cols=["mmsi"],
    route_order_by_cols=["dt_pos_utc"],
)

# ── Step 5: Filter hanya segmen di dalam port (polygon_name tidak null) ───────

df_in_port_only = df_routed.filter(F.col("polygon_name").isNotNull())

# ── Step 6: Agregasi port call ────────────────────────────────────────────────

print("Agregasi port call (arrival, departure, lama sandar)...")
df_port_call = af.agg_route(
    df_in_port_only,
    group_by_cols=["mmsi", "route_group", "polygon_name", "vessel_type_main"],
    order_by_cols=["dt_pos_utc"],
    num_agg_cols=["sog", "draught"],
    fl_agg_cols=["dt_pos_utc", "vessel_name"],
    checker=False,
)

# Hitung lama sandar dalam jam
df_port_call = df_port_call.withColumn(
    "lama_sandar_jam",
    (
        F.unix_timestamp("departure_dt_pos_utc") -
        F.unix_timestamp("arrival_dt_pos_utc")
    ) / 3600,
)

print(f"Total port call: {df_port_call.count():,}")

# ── Step 7: Simpan hasil ──────────────────────────────────────────────────────

print(f"Menyimpan ke {OUT_PATH}...")
df_port_call.write.mode("overwrite").parquet(OUT_PATH)
print("Port call selesai.")
