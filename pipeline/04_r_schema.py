import sys
sys.path.append("/home/onyxia/work/ais-pipeline")

from pyspark.sql import SparkSession
from config import (
    OUT_TRAFFIC_ALL, OUT_TRAFFIC_INBOUND, OUT_TRAFFIC_OUTBOUND,
    OUT_VESSEL_HORMUZ, OUT_VESSEL_NO_HORMUZ, OUT_ARRIVAL_RECAP,
    OUT_TIME_TRAVEL,
)

spark = SparkSession.builder.getOrCreate()

spark.read.parquet(OUT_TRAFFIC_ALL).printSchema()
spark.read.parquet(OUT_TRAFFIC_INBOUND).printSchema()
spark.read.parquet(OUT_TRAFFIC_OUTBOUND).printSchema()
spark.read.parquet(OUT_VESSEL_HORMUZ).printSchema()
spark.read.parquet(OUT_VESSEL_NO_HORMUZ).printSchema()
spark.read.parquet(OUT_ARRIVAL_RECAP).printSchema()
spark.read.parquet(OUT_TIME_TRAVEL).printSchema()