"""
pipeline/02_port_traffic.py
Analisis port traffic:
  1. Baca data AIS clean (akumulatif)
  2. Load AOI port Indonesia → explode H3 res-8
  3. Join AIS dengan port via H3_int_index_8
  4. Agregasi traffic per port per hari per tipe kapal
  5. Simpan hasil ke S3 personal
"""

import sys
sys.path.append("/home/onyxia/work/ais-pipeline")

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from ais import functions as af

from config import OUT_ACCUM, REF_PORT, OUT_PORT_TRAFFIC, H3_RESOLUTION_PORT

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

# ── Step 1: Baca AIS clean akumulatif ────────────────────────────────────────

print(f"Membaca {OUT_ACCUM}...")
df_ais = spark.read.parquet(OUT_ACCUM)

# ── Step 2: Load AOI port → explode H3 ───────────────────────────────────────

print("Membaca AOI port...")
port_exploded = (
    spark.read.parquet(REF_PORT)
    .select("Port", "Prov_Port", "Ket_Port",
            F.explode("boundary_h3").alias("boundary_h3"))
)

# ── Step 3: Join AIS dengan port via H3 ──────────────────────────────────────

print("Join AIS dengan port via H3_int_index_8...")
df_joined = df_ais.join(
    F.broadcast(port_exploded),
    df_ais["H3_int_index_8"] == port_exploded["boundary_h3"],
    how="inner",
)

# ── Step 4: Agregasi port traffic ────────────────────────────────────────────

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

print(f"Menyimpan ke {OUT_PORT_TRAFFIC}...")
df_traffic.write.mode("overwrite").parquet(OUT_PORT_TRAFFIC)
print("Port traffic selesai.")
