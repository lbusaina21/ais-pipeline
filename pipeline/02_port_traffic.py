"""
pipeline/02_port_traffic.py
Analisis port traffic Indonesia:
  1. Baca data AIS clean (akumulatif)
  2. Load AOI port Indonesia + port LN dari S3 personal → union
  3. Match kapal dengan port via H3_int_index_8
  4. Sortir port berurutan per kapal → hapus port berulang berturutan
  5. Filter kapal yang mampir >1 port
  6. Buat Origin-Destination (OD) antar port
  7. Filter: hanya rute yang melibatkan Indonesia
  8. Agregasi: all port traffic, inbound, outbound
  9. Simpan hasil ke S3 personal
"""

import sys
sys.path.append("/home/onyxia/work/ais-pipeline")

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, split, count
from pyspark.sql.types import StructType, StructField, DoubleType
import h3

from config import OUT_ACCUM, REF_PORT_INDO, REF_PORT_LN, OUT_TRAFFIC_ALL, OUT_TRAFFIC_INBOUND, OUT_TRAFFIC_OUTBOUND

# ── Spark session ─────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

# ── Step 1: Baca AIS clean akumulatif ────────────────────────────────────────

print(f"Membaca {OUT_ACCUM}...")
df_ais = spark.read.parquet(OUT_ACCUM)

# ── Step 2: Load AOI port → union port Indo + port LN ────────────────────────

print("Membaca AOI port...")
port_indo = (
    spark.read.parquet(REF_PORT_INDO)
    .select("Port", "Country", "boundary_h3", "Port_Country")
)
port_ln = spark.read.parquet(REF_PORT_LN)

ports = port_indo.union(port_ln)

# Tambah koordinat center tiap H3 cell
@F.udf(StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
]))
def h3_center(h3_int):
    h3_hex = hex(h3_int)[2:]
    lat, lon = h3.h3_to_geo(h3_hex)
    return (float(lat), float(lon))

ports = (
    ports
    .withColumn("center", h3_center("boundary_h3"))
    .withColumn("lat", F.col("center.lat"))
    .withColumn("lon", F.col("center.lon"))
    .drop("center")
)

# ── Step 3: Match kapal dengan port via H3 ───────────────────────────────────

print("Match kapal dengan port via H3_int_index_8...")
result_df = (
    df_ais
    .join(ports, df_ais["H3_int_index_8"] == ports["boundary_h3"], "left_outer")
    .filter(F.col("boundary_h3").isNotNull())
    .select(
        "mmsi", "dt_pos_utc", "vessel_type",
        "H3_int_index_8", "Port_Country",
        "boundary_h3", "lat", "lon",
    )
)

# ── Step 4: Sortir port berurutan → hapus port berulang berturutan ────────────

print("Sortir dan deduplikasi port berurutan...")
sorted_df = result_df.orderBy("mmsi", "dt_pos_utc")

window_spec = Window().orderBy("mmsi", "dt_pos_utc")
df_with_lag = sorted_df.withColumn("lag_portcountry", F.lag("Port_Country").over(window_spec))
df_final    = (
    df_with_lag
    .withColumn("delete_flag",
                F.when(F.col("Port_Country") == F.col("lag_portcountry"), 1).otherwise(0))
    .filter(F.col("delete_flag") == 0)
    .drop("lag_portcountry", "delete_flag")
    .orderBy("mmsi", "dt_pos_utc")
)

# ── Step 5: Filter kapal yang mampir >1 port ─────────────────────────────────

print("Filter kapal dengan kunjungan port > 1...")
count_window = Window().partitionBy("mmsi")
filtered_df  = (
    df_final
    .withColumn("count", F.count("*").over(count_window))
    .filter(F.col("count") > 1)
    .drop("count")
    .orderBy("mmsi", "dt_pos_utc")
)

# ── Step 6: Buat Origin-Destination ──────────────────────────────────────────

print("Buat kolom Origin-Destination...")
window_od  = Window().partitionBy("mmsi").orderBy("dt_pos_utc")
df_od      = (
    filtered_df
    .drop("H3_int_index_8", "boundary_h3")
    .withColumn("Port_Country_Destination", F.lead("Port_Country").over(window_od))
    .withColumn("lat_destination",          F.lead("lat").over(window_od))
    .withColumn("lon_destination",          F.lead("lon").over(window_od))
    .filter(F.col("Port_Country_Destination").isNotNull())
    .withColumnRenamed("Port_Country", "Port_Country_Origin")
    .withColumnRenamed("lat",          "lat_origin")
    .withColumnRenamed("lon",          "lon_origin")
)

# Tambah kolom negara
def extract_country(col_name):
    return (
        when(F.col(col_name).contains("-"),
             F.element_at(split(F.col(col_name), " - "), -1))
        .otherwise(F.col(col_name))
    )

df_od = (
    df_od
    .withColumn("Country_Origin",      extract_country("Port_Country_Origin"))
    .withColumn("Country_Destination", extract_country("Port_Country_Destination"))
)

# ── Step 7 & 8: Agregasi all, inbound, outbound ───────────────────────────────

# All — rute yang melibatkan Indonesia (origin ATAU destination = Indonesia)
print("Agregasi all port traffic...")
df_all = (
    df_od
    .filter(
        (col("Country_Destination") != col("Country_Origin")) &
        ((col("Country_Destination") == "Indonesia") | (col("Country_Origin") == "Indonesia"))
    )
    .drop("Country_Destination", "Country_Origin")
    .groupBy(
        "Port_Country_Origin", "Port_Country_Destination",
        "lat_origin", "lon_origin", "lat_destination", "lon_destination",
        "vessel_type", F.to_date("dt_pos_utc").alias("date"),
    )
    .count()
    .orderBy(F.desc("count"))
)

# Inbound — origin LN, destination Indonesia, first arrival per kapal
print("Agregasi inbound...")
window_inbound = Window().partitionBy("mmsi").orderBy("dt_pos_utc")
df_inbound = (
    df_od
    .filter((col("Country_Origin") != "Indonesia") & (col("Country_Destination") == "Indonesia"))
    .withColumn("rank", F.rank().over(window_inbound))
    .filter(col("rank") == 1)
    .drop("rank", "Country_Origin", "Country_Destination")
    .groupBy(
        "Port_Country_Origin", "Port_Country_Destination",
        "vessel_type", F.to_date("dt_pos_utc").alias("date"),
    )
    .count()
    .orderBy(F.desc("count"))
)

# Outbound — origin Indonesia, destination LN, first departure per kapal
print("Agregasi outbound...")
window_outbound = Window().partitionBy("mmsi").orderBy("dt_pos_utc")
df_outbound = (
    df_od
    .filter((col("Country_Origin") == "Indonesia") & (col("Country_Destination") != "Indonesia"))
    .withColumn("rank", F.rank().over(window_outbound))
    .filter(col("rank") == 1)
    .drop("rank", "Country_Origin", "Country_Destination")
    .groupBy(
        "Port_Country_Origin", "Port_Country_Destination",
        "vessel_type", F.to_date("dt_pos_utc").alias("date"),
    )
    .count()
    .orderBy(F.desc("count"))
)

print(f"All traffic : {df_all.count():,} baris")
print(f"Inbound     : {df_inbound.count():,} baris")
print(f"Outbound    : {df_outbound.count():,} baris")

# ── Step 9: Simpan hasil ──────────────────────────────────────────────────────

print(f"Menyimpan ke S3...")
df_all.write.mode("overwrite").parquet(OUT_TRAFFIC_ALL)
df_inbound.write.mode("overwrite").parquet(OUT_TRAFFIC_INBOUND)
df_outbound.write.mode("overwrite").parquet(OUT_TRAFFIC_OUTBOUND)
print("Port traffic selesai.")
