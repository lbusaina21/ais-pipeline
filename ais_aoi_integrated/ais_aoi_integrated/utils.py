from pyspark.sql import SparkSession
import geopandas as gpd # membuat geodataframe
import pandas as pd # membuat dataframe pandas
import h3 # membuat dan membantu visualisasi index h3
import matplotlib # plotting untuk visualisasi data
import matplotlib.pyplot as plt # modul dalam matplotlib untuk membuat plot dan grafik
from shapely.geometry import Polygon # kelas Shapely untuk membuat dan memanipulasi poligon
from datetime import datetime # modul untuk manipulasi tanggal dan waktu
import geopandas as gpd
import folium
from shapely.geometry import Polygon, mapping
from shapely.ops import unary_union
from pyspark.sql.functions import create_map, lit
from itertools import chain
from pyspark.sql.functions import element_at

import base64
from IPython.display import HTML

# SEDONA: memungkinkan penggunaan query SQL untuk memproses dan menganalisis data spasial.
import sedona.sql # modul untuk menjalankan query SQL pada data spasial
from sedona.register import SedonaRegistrator # alat untuk mendaftarkan Sedona ke Spark
from sedona.utils import SedonaKryoRegistrator, KryoSerializer 
# registrator untuk serialisasi objek spasial dengan Kryo
# serializer untuk meningkatkan kinerja serialisasi

# PYSPARK: antarmuka Python untuk Apache Spark
import pyspark.sql.functions as F # modul untuk fungsi SQL pada DataFrame
import pyspark.sql.types as pst # modul untuk tipe data SQL pada DataFrame
from pyspark import StorageLevel # kelas untuk menentukan tingkat penyimpanan RDD
from pyspark.sql import SparkSession  # kelas untuk membuat dan mengelola sesi Spark


import sys
import subprocess

# from ais import functions as af
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from shapely.geometry import mapping, Polygon, Point

from multiprocessing import Pool
import tqdm

import geopandas as gpd
import pandas as pd
import numpy as np

import folium
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
import json
import geopandas as gpd
import folium
from shapely.geometry import Polygon, mapping
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count, countDistinct, when, expr, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second
from pyspark.sql.functions import monotonically_increasing_id, lead, lag, abs, row_number
from pyspark.sql.functions import concat_ws, split, lit, min, max
from pyspark.sql.types import IntegerType, StringType, StructType
from pyspark.sql.window import Window


from shapely.geometry import Point, Polygon, mapping
from IPython.display import HTML
from multiprocessing import Pool

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count, countDistinct, when, expr, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second
from pyspark.sql.functions import monotonically_increasing_id, lead, lag, abs, row_number
from pyspark.sql.functions import concat_ws, split, lit, min, max
from pyspark.sql.types import IntegerType, StringType, StructType
from pyspark.sql.window import Window

from shapely.geometry import Point, Polygon, mapping
from IPython.display import HTML
from multiprocessing import Pool
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

import os
working_dir = os.environ["AWS_WORKING_DIRECTORY_PATH"]

# Path
save_path = f"s3a://{working_dir}/iran_usa_conflict/"

def clean_column(df):
    return df.dropna()


def read_aoi(approach: str) -> gpd.GeoDataFrame:
    """
    Membaca shapefile AOI berdasarkan pendekatan yang dipilih.

    Parameters:
        approach (str): 'Manual', 'Cluster', 'Distance', atau 'Heatmap'.

    Returns:
        GeoDataFrame
    """
    base_url = "https://raw.githubusercontent.com/gerynastiar/aoi/main"
    file_map = {
        "Manual": "Manual/Final/Experiment/DLKr_Indo_Final.shp",
        "Cluster": "Cluster/Cluster_AOI_Final.shp",
        "Distance": "Distance/Distance%20Fix%20135/Distance_AOI_135_Ports_Final.shp",
        "Heatmap": "Heatmap/Shapefile/Gabungan_Heatmap/Gabungan.shp"
    }

    if approach not in file_map:
        raise ValueError(f"Approach '{approach}' tidak dikenali. Pilih salah satu dari: {list(file_map.keys())}")

    full_url = f"/vsicurl/{base_url}/{file_map[approach]}"
    
    try:
        gdf = gpd.read_file(full_url)
        gdf = gdf.to_crs("EPSG:4326")  # Pastikan WGS 84
        print(f"Berhasil membaca AOI '{approach}' dengan {len(gdf)} fitur.")
        return gdf
    except Exception as e:
        print(f"❌ Gagal membaca shapefile: {e}")
        return gpd.GeoDataFrame()


def polygon_to_h3(polygon: Polygon, resolution: int) -> list:
    if not polygon.is_valid:
        return []
    if polygon.geom_type == 'MultiPolygon':
        polygon = polygon.geoms[0]
    if polygon.exterior is None:
        return []
    try:
        coords = [(y, x) for x, y in polygon.exterior.coords]
        hexagons = h3.polyfill({"type": "Polygon", "coordinates": [coords]}, resolution)
        return list(hexagons)
    except Exception as e:
        print(f"Error processing polygon: {e}")
        return []


def generate_buffer_hex(gdf_ports: gpd.GeoDataFrame, buffer_radius_m: float = 10000, resolution: int = 8) -> folium.Map:
    """
    Buat peta folium berisi buffer area dari pelabuhan dan representasi H3 hexagons.
    
    Args:
        gdf_ports: GeoDataFrame dari pelabuhan.
        buffer_radius_m: Radius buffer dalam meter (default 10 km).
        resolution: Resolusi H3 (default 8).
    
    Returns:
        folium.Map object siap ditampilkan atau disimpan.
    """
    gdf_proj = gdf_ports.to_crs(epsg=3857)
    gdf_proj["buffer"] = gdf_proj.geometry.buffer(buffer_radius_m)

    merged_buffer = unary_union(gdf_proj["buffer"])
    
    if merged_buffer.geom_type == "MultiPolygon":
        buffer_geometries = list(merged_buffer.geoms)
    else:
        buffer_geometries = [merged_buffer]

    buffer_geometries = [gpd.GeoSeries([geom], crs="EPSG:3857").to_crs(epsg=4326).geometry[0] for geom in buffer_geometries]

    m = folium.Map(location=[-2.5, 117], zoom_start=5)

    buffer_hexagons = set()
    for geom in buffer_geometries:
        buffer_hexagons.update(polygon_to_h3(geom, resolution=resolution))

    return buffer_hexagons

def change_buffer_type(buffer_hexagons, spark):
    buffer_hexagons_int = [
        h3.string_to_h3(h) if isinstance(h, str) else h
        for h in buffer_hexagons
    ]
    df = pd.DataFrame(buffer_hexagons_int, columns=['boundary_h3'])
    sdf = spark.createDataFrame(df)
    tmp_path = "tmp_boundary_hex"
    full_path = save_path + tmp_path
    sdf.write.mode("overwrite").parquet(full_path)
    return spark.read.parquet(full_path)
    # return sdf

def port_h3(gdf_ports: gpd.GeoDataFrame, resolution: int = 8) : 
    gdf_ports["h3_ids"] = gdf_ports.to_crs("EPSG:4326")["geometry"].apply(lambda geom: polygon_to_h3(geom, resolution))
    return gdf_ports
    
def explode_h3(hexagons: pd.DataFrame, spark=None):
    """
    Meledakkan kolom H3 ID dari DataFrame dan ubah ke DataFrame Spark.

    Parameters:
    -----------
    hexagons : pd.DataFrame
        DataFrame dengan kolom 'h3_ids' berisi list H3 index (str atau int).
    spark : SparkSession, optional
        Objek SparkSession aktif. Jika None, akan dibuat otomatis.

    Returns:
    --------
    pyspark.sql.DataFrame
        DataFrame Spark hasil ledakan H3 ID (kolom: h3_ids_int).
    """
    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")

    if "h3_ids" not in hexagons.columns:
        raise ValueError("Kolom 'h3_ids' tidak ditemukan di DataFrame")

    hexagons = hexagons.copy()

    def normalize_h3_list(h3_list):
        return [int(h, 16) if isinstance(h, str) else int(h) for h in h3_list]

    hexagons["h3_ids_int"] = hexagons["h3_ids"].apply(normalize_h3_list)

    exploded = hexagons.explode("h3_ids_int").drop(columns=["geometry"], errors="ignore")

    sdf = spark.createDataFrame(exploded)
    tmp_path = "tmp_port_hex"
    full_path = save_path + tmp_path
    sdf.write.mode("overwrite").parquet(full_path)
    return spark.read.parquet(full_path)
    # return sdf

def ais_buffer(ais, buffer_hexes, resolution: int = 8):
    """
    Join data AIS dengan buffer hex berdasarkan resolusi H3 yang diberikan.

    Parameters:
    -----------
    ais : DataFrame
        Data AIS yang memiliki kolom H3_int_index_{resolution}
    buffer_hexes : DataFrame
        Data hexagon buffer dengan kolom 'boundary_h3'
    resolution : int
        Resolusi H3 yang digunakan (default: 8)

    Returns:
    --------
    DataFrame hasil join
    """
    h3_column = f"H3_int_index_{resolution}"

    if h3_column not in ais.columns:
        raise ValueError(f"Kolom '{h3_column}' tidak ditemukan di data AIS.")

    ais_buffer_joined = ais.join(
        broadcast(buffer_hexes),
        ais[h3_column] == buffer_hexes["boundary_h3"],
        "left"
    )
    return ais_buffer_joined


def ais_port(ais_buffer, port_hexes,  resolution=8) :
    """
    Join data AIS dengan buffer hex berdasarkan resolusi H3 yang diberikan.

    Parameters:
    -----------
    ais : DataFrame
        Data AIS yang memiliki kolom H3_int_index_{resolution}
    buffer_hexes : DataFrame
        Data hexagon buffer dengan kolom 'boundary_h3'
    resolution : int
        Resolusi H3 yang digunakan (default: 8)

    Returns:
    --------
    DataFrame hasil join
    """
    h3_column = f"H3_int_index_{resolution}"

    if h3_column not in ais_buffer.columns:
        raise ValueError(f"Kolom '{h3_column}' tidak ditemukan di data AIS.")
    ais_port_joined = ais_buffer.join(
        port_hexes,
        ais_buffer[h3_column] == port_hexes["h3_ids_int"],
        "left_outer"
    )

    return ais_port_joined

def smbm_spatial_joint(ais_buffer, port_hexes) :
    """
    Join data AIS dengan buffer hex berdasarkan resolusi H3 yang diberikan.

    Parameters:
    -----------
    ais : DataFrame
        Data AIS yang memiliki kolom H3_int_index_{resolution}
    buffer_hexes : DataFrame
        Data hexagon buffer dengan kolom 'boundary_h3'
    resolution : int
        Resolusi H3 yang digunakan (default: 8)

    Returns:
    --------
    DataFrame hasil join
    """
    h3_column = "h3_index_int"

    if h3_column not in ais_buffer.columns:
        raise ValueError(f"Kolom '{h3_column}' tidak ditemukan di data AIS.")

    ais_port_joined = ais_buffer.join(
        port_hexes,
        ais_buffer["h3_index_int"] == port_hexes["h3_ids_int"],
        "inner"
    )

    return ais_port_joined

def algorithm_bcm_draft(ais,  spark=None) : 
    from pyspark.sql.window import Window 
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col, date_format, count, countDistinct, when, expr, unix_timestamp
    from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second
    from pyspark.sql.functions import monotonically_increasing_id, lead, lag, abs, row_number
    from pyspark.sql.functions import concat_ws, split, lit, min, max
    from pyspark.sql.types import IntegerType, StringType, StructType
    from pyspark.sql.window import Window
    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")
    ais = ensure_spark_df(ais, spark)
    match_port_aoi = ais.withColumn("position", 
                                  when(col("h3_ids_int").isNull(), "out")
                                  .otherwise("in"))

    available_columns = match_port_aoi.columns

    prov_col = "Prov_ID" if "Prov_ID" in available_columns else "kd_prov"
    hirarki_col = "Hirarki_1" if "Hirarki_1" in available_columns else "kode_hirar"

    match_port_aoi_select = match_port_aoi.select(
    "mmsi", "vessel_name",
    col(prov_col).alias("Prov_ID"),
    col("Kode").alias("Port"),
    col(hirarki_col).alias("Hirarki_1"),
    "dt_pos_utc",
    col("flag_country").alias("fc_vessel"),
    "GroupBeneficialOwnerCountryOfRegistration",
    "GroupBeneficialOwnerCountryofDomicile",
    "vessel_type", "vessel_type_main", "vessel_type_sub",
    col("nav_status").alias("ns_vessel"),
    "draught", "sog", "position", "latitude", "longitude", "H3_int_index_8")

    match_port_aoi_select = match_port_aoi_select.dropDuplicates()

    #1
    mmsi_with_in_port_only = match_port_aoi_select.filter(match_port_aoi_select.position == "in").select("mmsi").distinct()
    mmsi_with_out_port_only = match_port_aoi_select.filter(match_port_aoi_select.position == "out").select("mmsi").distinct()

    mmsi_to_keep = mmsi_with_in_port_only.join(mmsi_with_out_port_only, "mmsi", "left")

    filtered_data1 = match_port_aoi_select.join(mmsi_to_keep, "mmsi", "inner")

    #2
    filtered_data1 = filtered_data1.orderBy("mmsi", "dt_pos_utc", "Port")

    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    filtered_data2 = filtered_data1.withColumn("first_in_port", F.min(F.when(F.col("position") == "in", F.col("dt_pos_utc"))).over(window_spec))
    filtered_data2 = filtered_data2.withColumn("last_in_port", F.max(F.when(F.col("position") == "in", F.col("dt_pos_utc"))).over(window_spec))

    filtered_data2 = filtered_data2.withColumn("before_first_in_port", F.lead("position").over(window_spec) == "in")
    filtered_data2 = filtered_data2.withColumn("after_last_in_port", F.lag("position").over(window_spec) == "in")

    filtered_data2 = filtered_data2.withColumn("before_first_out_port", F.lead("position").over(window_spec) == "out")
    filtered_data2 = filtered_data2.withColumn("after_last_out_port", F.lag("position").over(window_spec) == "out")

    #3
    filtered_data2 = filtered_data2.filter(
    (
        (F.col("position") == "in") &
        (
            F.col("before_first_out_port").isNull() |
            F.col("after_last_out_port").isNull() |
            (
                (F.col("before_first_out_port") | F.col("after_last_out_port")) &
                ~(F.col("before_first_out_port") & F.col("after_last_out_port"))
            )
        )
    ) |
    (
        (F.col("position") == "out") &
        (
            F.col("before_first_in_port").isNull() |
            F.col("after_last_in_port").isNull() |
            (
                (F.col("before_first_in_port") | F.col("after_last_in_port")) &
                ~(F.col("before_first_in_port") & F.col("after_last_in_port"))
            )
        )
    ))

    #4
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    filtered_data2 = filtered_data2.withColumn("first_in_port", F.min(F.when(F.col("position") == "in", F.col("dt_pos_utc"))).over(window_spec))
    filtered_data2 = filtered_data2.withColumn("last_in_port", F.max(F.when(F.col("position") == "in", F.col("dt_pos_utc"))).over(window_spec))

    filtered_data2 = filtered_data2.withColumn("before_first_in_port", F.lead("position").over(window_spec) == "in")
    filtered_data2 = filtered_data2.withColumn("after_last_in_port", F.lag("position").over(window_spec) == "in")

    filtered_data2 = filtered_data2.withColumn("before_first_out_port", F.lead("position").over(window_spec) == "out")
    filtered_data2 = filtered_data2.withColumn("after_last_out_port", F.lag("position").over(window_spec) == "out")

    #5
    filtered_data2 = filtered_data2.filter(
    (
        (F.col("position") == "in") &
        (
            F.col("before_first_out_port").isNull() |
            F.col("after_last_out_port").isNull() |
            (
                (F.col("before_first_out_port") | F.col("after_last_out_port")) &
                ~(F.col("before_first_out_port") & F.col("after_last_out_port"))
            )
        )
    ) |
    (
        (F.col("position") == "out") &
        (
            F.col("before_first_in_port").isNull() |
            F.col("after_last_in_port").isNull() |
            (
                (F.col("before_first_in_port") | F.col("after_last_in_port")) &
                ~(F.col("before_first_in_port") & F.col("after_last_in_port"))
            )
        )
    ))

    #5
     # Daftar kolom yang ingin dijatuhkan
    kolom_drop = ["first_in_port", "last_in_port", "before_first_in_port", "after_last_in_port"]

    # Menjatuhkan kolom yang tidak diperlukan dari DataFrame
    filtered_data2 = filtered_data2.drop(*kolom_drop)

    #6
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    filter_data = filtered_data2.withColumn(
        "same_port_as_previous",
        F.when(
            F.lag("Port").over(window_spec) == F.col("Port"),
            True
        ).otherwise(False)
    )

    #7
    filter_data = filter_data.withColumn("same_port_next", F.lead("same_port_as_previous").over(window_spec))

    #8
    
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    filter_data = filter_data.withColumn(
        "in_port_count",
        F.sum(F.when(F.col("position") != "out", 1).otherwise(0)).over(window_spec)
    )

    filter_data = filter_data.withColumn(
        "first_out_port",
        F.min(F.when(F.col("position") == "out", F.col("dt_pos_utc"))).over(window_spec)
    ).withColumn(
        "last_out_port",
        F.max(F.when(F.col("position") == "out", F.col("dt_pos_utc"))).over(window_spec)
    )

    filter_data = filter_data.withColumn(
        "before_first_out_port",
        F.lead("position").over(window_spec) == "out"
    ).withColumn(
        "after_last_out_port",
        F.lag("position").over(window_spec) == "out"
    )

    #9
    out_port_condition = F.col("position") == "out"

    in_port_condition = (
        (F.col("position") == "in") &
        (
            (F.col("same_port_as_previous") == False) |
            (
                (F.col("same_port_as_previous") == True) &
                (
                    (F.col("before_first_out_port") == True) |
                    (F.col("after_last_out_port") == True) |
                    (F.col("same_port_next") == False)
                )
            )
        )
    )

    filter_final = filter_data.filter(out_port_condition | in_port_condition)

    #10
    kolom_drop = ["same_port_as_previous", "same_port_next", "in_port_count", "first_out_port", "last_out_port", "before_first_out_port", "after_last_out_port"]

    filter_final = filter_final.drop(*kolom_drop)
    filter_final = filter_final.dropDuplicates()
    filter_final = filter_final.orderBy("mmsi", "dt_pos_utc", "Port")
    after_filter = filter_final.withColumn("masuk_pelabuhan", lit("-")) \
                    .withColumn("keluar_pelabuhan", lit("-"))
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    after_filter = after_filter.withColumn("prev_position", F.lag("position", 1).over(window_spec))

    after_filter = after_filter.withColumn("next_position", F.lead("position", 1).over(window_spec))

    #11
     # Definisikan window specification
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    # Tentukan apakah Port sama dengan baris sebelumnya
    after_filter = after_filter.withColumn(
        "same_port_as_previous",
        F.when(
            F.lag("Port").over(window_spec) == F.col("Port"),
            True
        ).otherwise(False)
    )

    # Tentukan apakah Port sama dengan baris sebelumnya
    after_filter = after_filter.withColumn("same_port_next", F.lead("same_port_as_previous").over(window_spec))

    #12
    # Mendapatkan baris-baris dengan urutan waktu
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    match_port_aoi_select_in = after_filter.filter(col("position") == "in")

    # Menambahkan kolom baru untuk menandai baris pertama dengan nilai "in port" dari semua baris "in port" untuk suatu MMSI
    match_port_aoi_select_in = match_port_aoi_select_in.withColumn("first_in_port_all",
                                    (lag("position", 1).over(window_spec).isNull()) & (col("position") == "in"))

    # Menambahkan kolom baru untuk mendeteksi baris terakhir dengan nilai "in port" dari semua baris "in port" untuk suatu MMSI
    match_port_aoi_select_in = match_port_aoi_select_in.withColumn("last_in_port_all",
                                    (lead("position", 1).over(window_spec).isNull()) & (col("position") == "in"))

    #13
    # Gabungkan kembali dengan DataFrame asli
    joined_data_port = after_filter.join(match_port_aoi_select_in,
                                ["mmsi","vessel_name", "Port", "dt_pos_utc", "fc_vessel", "GroupBeneficialOwnerCountryOfRegistration","GroupBeneficialOwnerCountryofDomicile","vessel_type", "ns_vessel", "draught", "sog", "position", "masuk_pelabuhan", "keluar_pelabuhan",  "prev_position", "next_position", "same_port_as_previous", "same_port_next","latitude", "longitude", "H3_int_index_8"],
                                how='outer')

    # Select kolom yang relevan dan isi nilai NULL dengan False
    match_port = joined_data_port.select("mmsi","vessel_name", "Port", "dt_pos_utc", "fc_vessel","GroupBeneficialOwnerCountryOfRegistration","GroupBeneficialOwnerCountryofDomicile", "vessel_type", "ns_vessel", "draught", "sog", "position", "masuk_pelabuhan", "keluar_pelabuhan",  "prev_position", "next_position", "same_port_as_previous", "same_port_next", "first_in_port_all", "last_in_port_all","latitude", "longitude", "H3_int_index_8")

    #14

    position_out_port_condition = F.col("position") == "out"
    position_in_port_condition = (
        (F.col("position") == "in") &
        (F.col("first_in_port_all").isNotNull()) &
        (F.col("last_in_port_all").isNotNull())
    )

    match_port = match_port.filter(position_out_port_condition | position_in_port_condition)

    #15
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = match_port
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    df_with_prev_next_pos = df.withColumn(
        "prev_position", F.lag("position", 1).over(window_spec)
    ).withColumn(
        "next_position", F.lead("position", 1).over(window_spec)
    )

    masuk_condition = (
        ((F.col("position") == "in") & (F.col("prev_position") == "out")) |
        ((F.col("position") == "out") & (F.col("next_position") == "in"))
    )

    keluar_condition = (
        ((F.col("position") == "in") & (F.col("next_position") == "out")) |
        ((F.col("position") == "out") & (F.col("prev_position") == "in"))
    )

    port_traffic_simple = df_with_prev_next_pos.withColumn(
        "masuk_pelabuhan", F.when(masuk_condition, "masuk").otherwise("-")
    ).withColumn(
        "keluar_pelabuhan", F.when(keluar_condition, "keluar").otherwise("-")
    )

    #16
    from pyspark.sql.functions import col, when
    from functools import reduce

    # Alias kolom
    p = match_port  # biar singkat

    # Komponen logika dasar
    is_in = p["position"] == "in"
    is_out = p["position"] == "out"
    first = p["first_in_port_all"] == "True"
    last = p["last_in_port_all"] == "True"
    not_first = ~first
    not_last = ~last
    prev_in = p["prev_position"] == "in"
    prev_out = p["prev_position"] == "out"
    prev_null = p["prev_position"].isNull()
    next_in = p["next_position"] == "in"
    next_out = p["next_position"] == "out"
    next_null = p["next_position"].isNull()
    same_next = p["same_port_next"] == True
    diff_next = ~same_next
    same_prev = p["same_port_as_previous"] == True
    diff_prev = ~same_prev

    masuk_conditions = [
        is_in & first & not_last & prev_out & next_out,
        is_in & first & not_last & prev_out & next_in & diff_next,
        is_in & first & not_last & prev_out & next_in & same_next,
        is_in & not_first & not_last & prev_out & next_out,
        is_in & not_first & not_last & prev_out & next_in & diff_next,
        is_in & not_first & not_last & prev_out & next_in & same_next,
        is_in & not_first & not_last & prev_in & diff_prev & next_out,
        is_in & not_first & not_last & prev_in & diff_prev & next_in & diff_next,
        is_in & not_first & not_last & prev_in & diff_prev & next_in & same_next,
        is_in & not_first & not_last & prev_out & next_null,
        is_in & not_first & not_last & prev_in & diff_prev & next_null,
        is_in & not_first & last & prev_out & next_out,
        is_in & not_first & last & prev_in & diff_prev & next_out,
        is_in & not_first & last & prev_out & next_null,
        is_in & not_first & last & prev_in & diff_prev & next_null,
        is_in & first & last & prev_out,
    ]

    keluar_conditions = [
        is_in & first & not_last & prev_null & next_in & diff_next,
        is_in & first & not_last & prev_null & next_out,
        is_in & first & not_last & prev_out & next_out,
        is_in & first & not_last & prev_out & next_in & diff_next,
        is_in & not_first & not_last & prev_out & next_out,
        is_in & not_first & not_last & prev_out & next_in & diff_next,
        is_in & not_first & not_last & prev_in & same_prev & next_out,
        is_in & not_first & not_last & prev_in & same_prev & next_in & diff_next,
        is_in & not_first & not_last & prev_in & diff_prev & next_out,
        is_in & not_first & not_last & prev_in & diff_prev & next_in & diff_next,
        is_in & not_first & not_last & prev_in & same_prev & next_out,
        is_in & not_first & last & prev_out & next_out,
        is_in & not_first & last & prev_in & diff_prev & next_out,
        is_in & not_first & last & prev_in & same_prev & next_out,
        is_in & first & last & next_out
    ]

    # Gabungkan kondisi pakai OR
    masuk_all = reduce(lambda x, y: x | y, masuk_conditions)
    keluar_all = reduce(lambda x, y: x | y, keluar_conditions)

    # Terapkan ke DataFrame
    port_traffic = p \
        .withColumn("masuk_pelabuhan", when(masuk_all, "masuk").otherwise("-")) \
        .withColumn("keluar_pelabuhan", when(keluar_all, "keluar").otherwise("-"))

    #17
    # Daftar kolom yang ingin dijatuhkan
    kolom_drop = ["prev_position", "next_position", "same_port_as_previous", "same_port_next", "first_in_port_all", "last_in_port_all"]

    # Menjatuhkan kolom yang tidak diperlukan dari DataFrame
    port_traffic1 = port_traffic.drop(*kolom_drop)

    #18
    port_traffic_in = port_traffic1.filter(col("position") == "in")
    # Mendapatkan baris-baris dengan urutan waktu
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    # Tambahkan kolom prev_position
    port_traffic1 = port_traffic1.withColumn("prev_position", F.lag("position", 1).over(window_spec))

    # Tambahkan kolom next_position
    port_traffic1 = port_traffic1.withColumn("next_position", F.lead("position", 1).over(window_spec))

    #19
    # Mendapatkan baris-baris dengan urutan waktu
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    match_port_aoi_select_out = port_traffic1.filter(col("position") == "out")

    # Menambahkan kolom baru untuk menandai baris pertama dengan nilai "out port" dari semua baris "out port" untuk suatu MMSI
    match_port_aoi_select_out = match_port_aoi_select_out.withColumn("first_out_port_all",
                                    (lag("position", 1).over(window_spec).isNull()) & (col("position") == "out"))

    # Menambahkan kolom baru untuk mendeteksi baris terakhir dengan nilai "out port" dari semua baris "out port" untuk suatu MMSI
    match_port_aoi_select_out = match_port_aoi_select_out.withColumn("last_out_port_all",
                                    (lead("position", 1).over(window_spec).isNull()) & (col("position") == "out"))

    #20
    # Gabungkan kembali dengan DataFrame asli
    joined_data = port_traffic1.join(match_port_aoi_select_out,
                                ["mmsi", "vessel_name","Port", "dt_pos_utc", "fc_vessel","GroupBeneficialOwnerCountryOfRegistration","GroupBeneficialOwnerCountryofDomicile", "vessel_type", "ns_vessel", "draught", "sog", "position", "masuk_pelabuhan", "keluar_pelabuhan", "prev_position", "next_position","latitude", "longitude", "H3_int_index_8"],
                                how='outer')

    # Select kolom yang relevan dan isi nilai NULL dengan False
    port_traffic2 = joined_data.select("mmsi", "vessel_name","Port", "dt_pos_utc", "fc_vessel","GroupBeneficialOwnerCountryOfRegistration","GroupBeneficialOwnerCountryofDomicile", "vessel_type", "ns_vessel", "draught", "sog", "position", "masuk_pelabuhan", "keluar_pelabuhan", "prev_position", "next_position", "first_out_port_all", "last_out_port_all","latitude", "longitude", "H3_int_index_8")

    #21
    port_traffic2 = port_traffic2.filter(
        (F.col("position") == "in") |
        (
            (F.col("position") == "out")
            &
            (
                (F.col("first_out_port_all").isNotNull())
                &
                (F.col("last_out_port_all").isNotNull())
            )
        )
    )

    #22
    from pyspark.sql.functions import col, when

    # Alias DataFrame
    p = port_traffic2.alias("p")

    # Gunakan kolom dengan nama kualifikasi
    is_out = col("p.position") == "out"
    next_in = col("p.next_position") == "in"
    prev_in = col("p.prev_position") == "in"

    # Gabungkan kondisi
    masuk_condition = is_out & next_in
    keluar_condition = is_out & prev_in

    # Tambahkan kolom
    port_traffic3 = p.withColumn("masuk_pelabuhan", when(masuk_condition, "masuk").otherwise("-")) \
                    .withColumn("keluar_pelabuhan", when(keluar_condition, "keluar").otherwise("-"))
    port_traffic3 = port_traffic3.orderBy("mmsi", "dt_pos_utc", "Port")

    #23
    # Daftar kolom yang ingin dijatuhkan
    kolom_drop = ["prev_position", "next_position", "first_out_port_all", "last_out_port_all"]

    # Menjatuhkan kolom yang tidak diperlukan dari DataFrame
    port_traffic3 = port_traffic3.drop(*kolom_drop)
    port_traffic_out = port_traffic3.filter(col("position") == "out")

    #24
    result_out_in = port_traffic_in.unionAll(port_traffic_out)
    result_out_in = result_out_in.orderBy("mmsi", "dt_pos_utc", "Port")

    # Membuat window specification
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc")

    # Menambahkan kolom selisih waktu
    result_diff = result_out_in.withColumn(
        "time",
        unix_timestamp(F.lead("dt_pos_utc").over(window_spec)) - unix_timestamp("dt_pos_utc")
    )
    return result_diff

def algorithm_bcm(ais,  spark=None) : 
    from pyspark.sql.window import Window 
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col, date_format, count, countDistinct, when, expr, unix_timestamp
    from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second
    from pyspark.sql.functions import monotonically_increasing_id, lead, lag, abs, row_number
    from pyspark.sql.functions import concat_ws, split, lit, min, max
    from pyspark.sql.types import IntegerType, StringType, StructType
    from pyspark.sql import SparkSession
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, when
    from functools import reduce

    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")

    ais_in_port = ais.withColumn("position",
                                when(col("h3_ids_int").isNull(), "out")
                                .otherwise("in"))

    available_columns = ais_in_port.columns

    prov_col = "Prov_ID" if "Prov_ID" in available_columns else "kd_prov"
    hirarki_col = "Hirarki_1" if "Hirarki_1" in available_columns else "kode_hirar"

    ais_in_port = ais_in_port.select(
    "mmsi", "vessel_name",
    col(prov_col).alias("Prov_ID"),
    col("Kode").alias("Port"),
    col(hirarki_col).alias("Hirarki_1"),
    "dt_pos_utc",
    col("flag_country").alias("fc_vessel"),
    "GroupBeneficialOwnerCountryofDomicile",
    "GroupBeneficialOwnerCountryOfRegistration",
    "RegisteredOwnerCountryofDomicile",
    "vessel_type", "vessel_type_main", "vessel_type_sub",
    col("nav_status").alias("ns_vessel"),
    "draught", "sog", "position", "latitude", "longitude", "H3_int_index_8")

    ais_in_port = ais_in_port.dropDuplicates()

    # hapus mmsi dengan nilai in saja atau out saja
    mmsi_in_port_only = ais_in_port.filter(F.col("position") == "in").select("mmsi").distinct()
    mmsi_out_port_only = ais_in_port.filter(F.col("position") == "out").select("mmsi").distinct()
    mmsi_with_in_or_out = mmsi_in_port_only.join(mmsi_out_port_only, "mmsi", "left")

    ais_filtered_by_mmsi = ais_in_port.join(mmsi_with_in_or_out, "mmsi", "inner")

    # catat waktu pertama kali masuk dan terakhir di pelabuhan dan beri flag pada recordnya juga
    ais_filtered_by_mmsi = ais_filtered_by_mmsi.orderBy("mmsi", "dt_pos_utc", "Port")

    mmsi_time_window = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    ais_with_port_events = ais_filtered_by_mmsi \
        .withColumn("first_in_timestamp", F.min(F.when(F.col("position") == "in", F.col("dt_pos_utc"))).over(mmsi_time_window)) \
        .withColumn("last_in_timestamp", F.max(F.when(F.col("position") == "in", F.col("dt_pos_utc"))).over(mmsi_time_window)) \
        .withColumn("is_before_first_in", F.lead("position").over(mmsi_time_window) == "in") \
        .withColumn("is_after_last_in", F.lag("position").over(mmsi_time_window) == "in") \
        .withColumn("is_before_first_out", F.lead("position").over(mmsi_time_window) == "out") \
        .withColumn("is_after_last_out", F.lag("position").over(mmsi_time_window) == "out")
    
    # pertahankan hanya record pertama dan terakhir di dalam pelabuhan dan record pertama sebelum masuk dan sesudah keluar pelabuhan
    ais_with_port_events = ais_with_port_events.filter(
        (
            (F.col("position") == "in") &
            (
                F.col("is_before_first_out").isNull() |
                F.col("is_after_last_out").isNull() |
                (
                    (F.col("is_before_first_out") | F.col("is_after_last_out")) &
                    ~(F.col("is_before_first_out") & F.col("is_after_last_out"))
                )
            )
        ) |
        (
            (F.col("position") == "out") &
            (
                F.col("is_before_first_in").isNull() |
                F.col("is_after_last_in").isNull() |
                (
                    (F.col("is_before_first_in") | F.col("is_after_last_in")) &
                    ~(F.col("is_before_first_in") & F.col("is_after_last_in"))
                )
            )
        )
    )

    ais_with_port_events = ais_with_port_events \
        .withColumn("first_in_timestamp", F.min(F.when(F.col("position") == "in", F.col("dt_pos_utc"))).over(mmsi_time_window)) \
        .withColumn("last_in_timestamp", F.max(F.when(F.col("position") == "in", F.col("dt_pos_utc"))).over(mmsi_time_window)) \
        .withColumn("is_before_first_in", F.lead("position").over(mmsi_time_window) == "in") \
        .withColumn("is_after_last_in", F.lag("position").over(mmsi_time_window) == "in") \
        .withColumn("is_before_first_out", F.lead("position").over(mmsi_time_window) == "out") \
        .withColumn("is_after_last_out", F.lag("position").over(mmsi_time_window) == "out")

    ais_with_port_events = ais_with_port_events.filter(
        (
            (F.col("position") == "in") &
            (
                F.col("is_before_first_out").isNull() |
                F.col("is_after_last_out").isNull() |
                (
                    (F.col("is_before_first_out") | F.col("is_after_last_out")) &
                    ~(F.col("is_before_first_out") & F.col("is_after_last_out"))
                )
            )
        ) |
        (
            (F.col("position") == "out") &
            (
                F.col("is_before_first_in").isNull() |
                F.col("is_after_last_in").isNull() |
                (
                    (F.col("is_before_first_in") | F.col("is_after_last_in")) &
                    ~(F.col("is_before_first_in") & F.col("is_after_last_in"))
                )
            )
        )
    )

    kolom_drop = ["first_in_port", "last_in_port", "before_first_in_port", "after_last_in_port","before_first_out_port","after_last_out_port", "prev_position","next_position","first_in_timestamp","last_in_timestamp","is_before_first_in", "is_after_last_in", "is_before_first_out", "is_after_last_out", "prev_position","next_position"]

    # Menjatuhkan kolom yang tidak diperlukan dari DataFrame
    ais_with_port_events = ais_with_port_events.drop(*kolom_drop)

    #6
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = ais_with_port_events
    window_spec = Window.partitionBy("mmsi").orderBy("dt_pos_utc", "Port")

    df_with_prev_next_pos = df.withColumn(
        "prev_position", F.lag("position", 1).over(window_spec)
    ).withColumn(
        "next_position", F.lead("position", 1).over(window_spec)
    )

    masuk_condition = (
        ((F.col("position") == "in") & (F.col("prev_position") == "out")) |
        ((F.col("position") == "out") & (F.col("next_position") == "in"))
    )

    keluar_condition = (
        ((F.col("position") == "in") & (F.col("next_position") == "out")) |
        ((F.col("position") == "out") & (F.col("prev_position") == "in"))
    )

    port_traffic = df_with_prev_next_pos.withColumn(
        "masuk_pelabuhan", F.when(masuk_condition, "masuk").otherwise("-")
    ).withColumn(
        "keluar_pelabuhan", F.when(keluar_condition, "keluar").otherwise("-")
    )

    kolom_drop = ["prev_position","next_position"]
    port_traffic = port_traffic.drop(*kolom_drop)
    result_diff = port_traffic.withColumn(
        "time",
        unix_timestamp(F.lead("dt_pos_utc").over(window_spec)) - unix_timestamp("dt_pos_utc")
    )
    return result_diff

def create_download_link(df, title, filename):
    df_pandas = df.toPandas()
    
    csv = df_pandas.to_csv(index=False)
    
    b64 = base64.b64encode(csv.encode())
    payload = b64.decode()
    
    html = f'<a download="{filename}" href="data:text/csv;base64,{payload}" target="_blank">{title}</a>'
    
    return HTML(html)

def boundary_draft(ais, aoi, resolution: int = 8, buffer_radius_m: float = 10000,  spark=None) : 
    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")
    ais = ensure_spark_df(ais, spark)
    print(f"Membaca AOI dengan pendekatan '{aoi}'.")
    aoi_ais = read_aoi(aoi)
    print(f"\nMembentuk buffer di sekitar AOI")
    buffer_hex = generate_buffer_hex(aoi_ais, buffer_radius_m, resolution)
    buffer_sdf = change_buffer_type(buffer_hex, spark)
    print(f"\nMengekstrak data AIS di dalam buffer")
    ais_in_buffer = ais_buffer(ais, buffer_sdf, resolution)
    port_hex = port_h3(aoi_ais, resolution)
    port_hexes = explode_h3(port_hex, spark)
    print(f"\nMengekstrak data AIS di dalam pelabuhan")
    # print(port_hexes.head())
    ais_in_port = ais_port(ais_in_buffer, port_hexes, resolution)
    print(f"\nAlgoritma aktivitas kapal di pelabuhan dimulai")
    arus = algorithm_bcm_draft(ais_in_port, spark)
    print(f"\nSelesai!")
    return arus

def boundary(ais, aoi, resolution: int = 8, buffer_radius_m: float = 10000,  spark=None) : 
    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")
    ais = ensure_spark_df(ais, spark)
    print(f"Membaca AOI dengan pendekatan '{aoi}'.")
    aoi_ais = read_aoi(aoi)
    if (aoi == "Manual") & (resolution < 10):
        selected_ports = ['REO', 'MPG', 'RCI', 'PTE', 'MRT', 'MDC']
        aoi_ais = aoi_ais[~aoi_ais['Kode'].str.upper().isin(selected_ports)]
    print(f"\nPelabuhan dengan Kode REO, MPG, RCI, PTE, MRT, dan MDC tidak diikutsertakan karena terlalu kecil untuk didefinisikan dengan resolusi H3 Index di bawah 10")

    print(f"\nMembentuk buffer di sekitar AOI")
    buffer_hex = generate_buffer_hex(aoi_ais, buffer_radius_m, resolution)
    buffer_sdf = change_buffer_type(buffer_hex, spark)
    print(f"\nMengekstrak data AIS di dalam buffer")
    ais_in_buffer = ais_buffer(ais, buffer_sdf, resolution)
    port_hex = port_h3(aoi_ais, resolution)
    port_hexes = explode_h3(port_hex, spark)
    print(f"\nMengekstrak data AIS di dalam pelabuhan")
    # print(port_hexes.head())
    ais_inside_port = ais_port(ais_in_buffer, port_hexes, resolution)
    print(f"\nAlgoritma aktivitas kapal di pelabuhan dimulai")
    arus = algorithm_bcm(ais_inside_port, spark)
    print(f"\nSelesai! Fixed")
    return arus

from pyspark.sql.functions import col, date_format, month, year, count

from pyspark.sql.functions import col, date_format, month, year, count, create_map, lit
from itertools import chain

def ensure_spark_df(df, spark=None):
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql import DataFrame as SparkDataFrame
    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")

    if isinstance(df, pd.DataFrame):
        return spark.createDataFrame(df)
    elif isinstance(df, (SparkDataFrame, ConnectDataFrame)):
        return df
    else:
        raise TypeError("Input harus berupa pandas.DataFrame atau pyspark.sql.DataFrame atau pyspark.sql.connect.dataframe.DataFrame")

def recap_boundary_with_hormuz(
    ais_activity,
    activity="arrival",         
    port_code=None,               
    month_obs=None,                   
    year_obs=None,                    
    port_time_limit=0, 
    spark=None         
):

    if spark is None:
        raise ValueError("spark session harus di-pass")

    ais_activity = ensure_spark_df(ais_activity, spark)

    # =========================
    # COUNTRY TAGGING
    # =========================
    df = ais_activity.withColumn(
        "status_country_domicile", 
        F.when(F.col("GroupBeneficialOwnerCountryofDomicile") == "Indonesia", "Domestic").otherwise("Foreign")
    )

    df = df.withColumn(
        "status_country_register", 
        F.when(F.col("GroupBeneficialOwnerCountryOfRegistration") == "Indonesia", "Domestic").otherwise("Foreign")
    )

    df = df.withColumn(
        "status_country_flag", 
        F.when(F.col("fc_vessel") == "Indonesia", "Domestic").otherwise("Foreign")
    )

    min_durasi_detik = port_time_limit * 60

    # =========================
    # FILTER ACTIVITY
    # =========================
    if activity == "arrival":
        df_filtered = df.filter(
            (col("masuk_pelabuhan") == "masuk") &
            (col("position") == "in")
        )

        if port_time_limit > 0:
            df_filtered = df_filtered.filter(col("time") >= min_durasi_detik)

    elif activity == "departed":
        if port_time_limit == 0:
            df_filtered = df.filter(
                (col("keluar_pelabuhan") == "keluar") &
                (col("position") == "in")
            )

        elif port_time_limit > 0:
            df_pandas = df.toPandas()

            df_pandas_2 = df_pandas.sort_values(by=["mmsi", "dt_pos_utc"]).reset_index(drop=True)

            df_pandas_2["prev_time_1"] = df_pandas_2["time"].shift(1)
            df_pandas_2["prev_time_2"] = df_pandas_2["time"].shift(2)
            df_pandas_2["pos"] = df_pandas_2["position"].shift(2)
            df_pandas_2["in"] = df_pandas_2["masuk_pelabuhan"].shift(2)
            df_pandas_2["Port_prev"] = df_pandas_2["Port"].shift(1)
            df_pandas_2["mmsi_prev"] = df_pandas_2["mmsi"].shift(2)

            if "is_through_hormuz" in df_pandas_2.columns:
                df_pandas_2["is_through_hormuz_prev"] = df_pandas_2["is_through_hormuz"].shift(1)

            # FILTER
            df_filtered = df_pandas_2[
                (df_pandas_2["position"] == "out") &
                (df_pandas_2["keluar_pelabuhan"] == "keluar") &
                (df_pandas_2["prev_time_2"] >= min_durasi_detik) &
                (df_pandas_2["pos"] == "in") &
                (df_pandas_2["in"] == "masuk") &
                (df_pandas_2["mmsi"] == df_pandas_2["mmsi_prev"])
            ]

            # AMBIL PORT SEBELUMNYA
            df_filtered.loc[:, "Port"] = df_filtered["Port_prev"]

            if "is_through_hormuz_prev" in df_filtered.columns:
                df_filtered.loc[:, "is_through_hormuz"] = df_filtered["is_through_hormuz_prev"]

            # FORMAT DATE
            df_filtered["dt_pos_utc"] = pd.to_datetime(df_filtered["dt_pos_utc"]).dt.date

            # BALIK KE SPARK
            df_filtered = ensure_spark_df(df_filtered, spark)
    else:
        raise ValueError("activity harus 'arrival' atau 'departed'")

    # =========================
    # DATE
    # =========================
    df_filtered = df_filtered.withColumn("date", date_format(col("dt_pos_utc"), "yyyy-MM-dd"))\
                             .withColumn("month", month(col("dt_pos_utc")))\
                             .withColumn("year", year(col("dt_pos_utc")))

    # =========================
    # FILTER PARAMETER
    # =========================
    if port_code:
        df_filtered = df_filtered.filter(col("Port") == port_code)
    if month_obs:
        df_filtered = df_filtered.filter(col("month") == month_obs)
    if year_obs:
        df_filtered = df_filtered.filter(col("year") == year_obs)

    # =========================
    # DETECT KOLOM HORMUZ
    # =========================
    group_cols = ["date", "Port", "status_country_domicile", "vessel_type", "vessel_type_main", "vessel_type_sub", "fc_vessel", 
                  "GroupBeneficialOwnerCountryofDomicile",
                  "GroupBeneficialOwnerCountryOfRegistration", "RegisteredOwnerCountryofDomicile", "ns_vessel"]

    if "is_through_hormuz" in df_filtered.columns:
        group_cols.append("is_through_hormuz")

    if "first_detected_hormuz" in df_filtered.columns:
        group_cols.append("first_detected_hormuz")

    if "last_detected_hormuz" in df_filtered.columns:
        group_cols.append("last_detected_hormuz")

    # =========================
    # AGGREGATION
    # =========================
    recap_df = df_filtered.groupBy(*group_cols)\
                          .agg(count("*").alias("count"))

    # =========================
    # MAPPING NAMA PELABUHAN
    # =========================
    kode_to_nama = {
        'CLG': 'Calang',
        'KUA': 'Kuala Langsa',
        'LSW': 'Lhokseumawe/Kreung Geukeh',
        'MLH': 'Malahayati',
        'MEQ': 'Meulaboh',
        'SBG': 'Sabang',
        'SNL': 'Singkil',
        'BLW': 'Belawan',
        'GNS': 'Gunung Sitoli',
        'KTJ': 'Kuala Tanjung',
        'LHA': 'Lahewa',
        'INL': 'Natal/ Sikara-kara',
        'PDD': 'Pangkalan Dodek',
        'PKS': 'Pangkalan Susu',
        'PTE': 'Pulau Tello',
        'SLG': 'Sibolga',
        'SRU': 'Sirombu',
        'TBE': 'Tanjung Beringin',
        'TDA': 'Teluk Dalam',
        'LIG': 'Teluk Leidong',
        'MPG': 'Muara Padang',
        'TBY': 'Teluk Bayur',
        'IBI': 'Bagan Siapi-api',
        'BKI': 'Bengkalis',
        'DUM': 'Dumai',
        'ENO': 'Kuala Enok',
        'MRT': 'Meranti/ Dorak',
        'RCI': 'Rengat/Kuala Cinaku',
        'SLJ': 'Selat Panjang',
        'SUQ': 'Sungai Guntung',
        'TMD': 'Tanjung Medang',
        'TLN': 'Tembilahan',
        'MSK': 'Muara Sabak',
        'TNU': 'Talang Duku',
        'ING': 'Boom Baru/ Palembang',
        'TGP': 'Tanjung Api-Api',
        'BKS': 'Pulau Baai',
        'TAG': 'Kota Agung/ Batu Balai',
        'PNJ': 'Panjang',
        'BLU': 'Belinyu',
        'MUO': 'Muntok',
        'PGX': 'Pangkal Balam',
        'TJQ': 'Tanjung Pandan',
        'BUR': 'Batam',
        'SKJ': 'Sei Kolak Kijang',
        'TJB': 'Tanjung Balai Karimun',
        'TBD': 'Tanjung Batu Kundur',
        'TNJ': 'Tanjung Pinang',
        'IMW': 'Marunda',
        'TPR': 'Tanjung priok',
        'CBN': 'Cirebon',
        'PDR': 'Pangandaran/Bojongsalawe',
        'PTM': 'Patimban',
        'SRG': 'Tanjung Emas',
        'CXP': 'Tanjung Intan',
        'BWB': 'Banyu Wangi/ Boom',
        'GRE': 'Gresik',
        'KAT': 'Kalianget',
        'PAZ': 'Pasuruan',
        'AEA': 'Probolinggo',
        'SUB': 'Tanjung Perak',
        'IBP': 'Banten',
        'BOA': 'Benoa',
        'CEB': 'Celukan Bawang',
        'BAD': 'Badas',
        'BMU': 'Bima',
        'LMR': 'Lembar',
        'ENE': 'Ende',
        'IPI': 'Ippi',
        'KBH': 'Kalabahi',
        'LBO': 'Labuan Bajo',
        'MOF': 'Maumere/ Lorens Say',
        'REO': 'Reo',
        'ISA': 'Seba',
        'TQP': 'Tenau/ Kupang',
        'WGP': 'Waingapu',
        'PNK': 'Pontianak',
        'SNE': 'Sintete',
        'KPE': 'Kuala Pembuang',
        'KUM': 'Kumai',
        'PLB': 'Pangkalan Bun',
        'PMI': 'Pegatan Mendawai',
        'PPS': 'Pulang Pisau',
        'SMQ': 'Sampit',
        'SMA': 'Samuda',
        'SAA': 'Sukamara',
        'BDJ': 'Banjarmasin',
        'SUR': 'Satui',
        'BPN': 'Balikpapan',
        'LTU': 'Lhok Tuan',
        'SGQ': 'Sangatta',
        'SKI': 'Sangkulirang',
        'TLA': 'Tanjung Laut',
        'TRE': 'Tanjung Redeb',
        'TTK': 'Nunukan / Tunon Taka',
        'MLD': 'Tarakan / Malundung',
        'LUK': 'Labuhan Uki',
        'MDC': 'Manado',
        'BGG': 'Banggai',
        'DNA': 'Donggala',
        'LUW': 'Luwuk',
        'PTL': 'Pantoloan',
        'TLI': 'Toli-toli',
        'WNQ': 'Wani',
        'BAE': 'Bajoe',
        'MAK': 'Makassar',
        'ARE': 'Pare-Pare',
        'YAR': 'Selayar/Benteng/Rauf Rahman',
        'BBM': 'Bau-Bau/Murhum',
        'KDI': 'Kendari/Bungkutoko',
        'KOL': 'Kolaka',
        'NGG': 'Anggrek',
        'GTO': 'Gorontalo',
        'MJU': 'Mamuju',
        'AMQ': 'Ambon',
        'NDA': 'Banda Naira',
        'LQA': 'Leksula',
        'NAM': 'Leksula',
        'SXK': 'Saumlaki',
        'TUA': 'Tual',
        'DRA': 'Daruba',
        'LBH': 'Labuha',
        'SIO': 'Soasio/Goto',
        'TEI': 'Ternate/ A. Yani',
        'TBO': 'Tobelo',
        'BIK': 'Biak',
        'DJJ': 'Jayapura',
        'MKQ': 'Merauke',
        'NBX': 'Nabire',
        'FKQ': 'Fak-fak',
        'MKW': 'Manokwari',
        'SOQ': 'Sorong',
        'WSR': 'Wasior',
        'ISG': 'Sapudi',
        'BIT': 'Bitung'
    }

    mapping_expr = create_map([lit(x) for x in chain(*kode_to_nama.items())])

    recap_df = recap_df.withColumn(
        "nama_pelabuhan",
        element_at(mapping_expr, col("Port"))
    )

    return recap_df

def recap_boundary(
    ais_activity,
    activity="arrival",         
    port_code=None,               
    month_obs=None,                   
    year_obs=None,                    
    port_time_limit=0, 
    spark = None         
):
    """
    Fungsi untuk merekap data AIS kapal berdasarkan durasi minimal berada di pelabuhan
    dan jenis kedatangan/keberangkatan.

    Parameters:
    - ais_activity: DataFrame input (AIS)
    - activity: "arrival" atau "departed"
    - port_code: string kode pelabuhan (opsional)
    - month: int 1-12 (opsional)
    - year: int (opsional)
    - port_time_limit: int durasi minimum dalam menit (default: 30).
                       Jika 0, maka tanpa filter durasi.

    Returns:
    - DataFrame rekap per tanggal
    """
    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")
        
    ais_activity = ensure_spark_df(ais_activity, spark)
    country_separation = ais_activity.withColumn(
        "status_country_domicile", 
        F.when(F.col("GroupBeneficialOwnerCountryofDomicile") == "Indonesia", "Domestic").otherwise("Foreign")
    )

    country_separation2 = country_separation.withColumn(
        "status_country_register", 
        F.when(F.col("GroupBeneficialOwnerCountryOfRegistration") == "Indonesia", "Domestic").otherwise("Foreign")
    )

    country_separation3 = country_separation2.withColumn(
        "status_country_flag", 
        F.when(F.col("fc_vessel") == "Indonesia", "Domestic").otherwise("Foreign")
    )
    min_durasi_detik = port_time_limit * 60

    if activity == "arrival":
        df_filtered = country_separation3.filter(
            (col("masuk_pelabuhan") == "masuk") &
            (col("position") == "in")
        )
        if port_time_limit > 0:
            df_filtered = df_filtered.filter(col("time") >= min_durasi_detik)

    elif activity == "departed":
        if port_time_limit == 0 :
            df_filtered = country_separation3.filter(
                (col("keluar_pelabuhan") == "keluar") &
                (col("position") == "in")
            )

        elif port_time_limit > 0:
            df_pandas = country_separation3.toPandas()

            df_pandas_2 = df_pandas.sort_values(by=["mmsi", "dt_pos_utc"]).reset_index(drop=True)
            # Buat kolom untuk melihat dua baris sebelumnya di data asli
            df_pandas_2["prev_time_1"] = df_pandas_2["time"].shift(1)  # Baris sebelumnya
            df_pandas_2["prev_time_2"] = df_pandas_2["time"].shift(2)  # Dua baris sebelumnya
            df_pandas_2["pos"] = df_pandas_2["position"].shift(2)  # Dua baris sebelumnya
            df_pandas_2["in"] = df_pandas_2["masuk_pelabuhan"].shift(2)  # Dua baris sebelumnya
            df_pandas_2["Port_prev"] = df_pandas_2["Port"].shift(1) 
            df_pandas_2["mmsi_prev"] = df_pandas_2["mmsi"].shift(2)

            # Filter berdasarkan kondisi utama: position = "out port" & keluar_pelabuhan = "keluar"
            df_filtered = df_pandas_2[
                (df_pandas_2["position"] == "out") &
                (df_pandas_2["keluar_pelabuhan"] == "keluar") &
                (df_pandas_2["prev_time_2"] >= min_durasi_detik) &
                (df_pandas_2["pos"] == "in") &
                (df_pandas_2["in"] == "masuk") &
                (df_pandas_2["mmsi"] == df_pandas_2["mmsi_prev"])
            ]

            # Tambahkan nilai Port dari baris sebelumnya ke hasil filter
            df_filtered.loc[:, "Port"] = df_filtered["Port_prev"]
            df_filtered["dt_pos_utc"] = pd.to_datetime(df_filtered["dt_pos_utc"]).dt.date
            df_filtered = ensure_spark_df(df_filtered, spark)
    else:
        raise ValueError("Parameter 'activity' harus 'arrival' atau 'departed'.")


    df_filtered = df_filtered.withColumn("date", date_format(col("dt_pos_utc"), "yyyy-MM-dd"))\
                            .withColumn("month", month(col("dt_pos_utc")))\
                            .withColumn("year", year(col("dt_pos_utc")))

    if port_code:
        df_filtered = df_filtered.filter(col("Port") == port_code)
    if month_obs:
        df_filtered = df_filtered.filter(col("month") == month_obs)
    if year_obs:
        df_filtered = df_filtered.filter(col("year") == year_obs)

    recap_df = df_filtered.groupBy("date", "Port", "status_country_domicile", "vessel_type", "fc_vessel")\
                          .agg(count("*").alias("count"))

    kode_to_nama = {
    'CLG': 'Calang',
    'KUA': 'Kuala Langsa',
    'LSW': 'Lhokseumawe/Kreung Geukeh',
    'MLH': 'Malahayati',
    'MEQ': 'Meulaboh',
    'SBG': 'Sabang',
    'SNL': 'Singkil',
    'BLW': 'Belawan',
    'GNS': 'Gunung Sitoli',
    'KTJ': 'Kuala Tanjung',
    'LHA': 'Lahewa',
    'INL': 'Natal/ Sikara-kara',
    'PDD': 'Pangkalan Dodek',
    'PKS': 'Pangkalan Susu',
    'PTE': 'Pulau Tello',
    'SLG': 'Sibolga',
    'SRU': 'Sirombu',
    'TBE': 'Tanjung Beringin',
    'TDA': 'Teluk Dalam',
    'LIG': 'Teluk Leidong',
    'MPG': 'Muara Padang',
    'TBY': 'Teluk Bayur',
    'IBI': 'Bagan Siapi-api',
    'BKI': 'Bengkalis',
    'DUM': 'Dumai',
    'ENO': 'Kuala Enok',
    'MRT': 'Meranti/ Dorak',
    'RCI': 'Rengat/Kuala Cinaku',
    'SLJ': 'Selat Panjang',
    'SUQ': 'Sungai Guntung',
    'TMD': 'Tanjung Medang',
    'TLN': 'Tembilahan',
    'MSK': 'Muara Sabak',
    'TNU': 'Talang Duku',
    'ING': 'Boom Baru/ Palembang',
    'TGP': 'Tanjung Api-Api',
    'BKS': 'Pulau Baai',
    'TAG': 'Kota Agung/ Batu Balai',
    'PNJ': 'Panjang',
    'BLU': 'Belinyu',
    'MUO': 'Muntok',
    'PGX': 'Pangkal Balam',
    'TJQ': 'Tanjung Pandan',
    'BUR': 'Batam',
    'SKJ': 'Sei Kolak Kijang',
    'TJB': 'Tanjung Balai Karimun',
    'TBD': 'Tanjung Batu Kundur',
    'TNJ': 'Tanjung Pinang',
    'IMW': 'Marunda',
    'TPR': 'Tanjung priok',
    'CBN': 'Cirebon',
    'PDR': 'Pangandaran/Bojongsalawe',
    'PTM': 'Patimban',
    'SRG': 'Tanjung Emas',
    'CXP': 'Tanjung Intan',
    'BWB': 'Banyu Wangi/ Boom',
    'GRE': 'Gresik',
    'KAT': 'Kalianget',
    'PAZ': 'Pasuruan',
    'AEA': 'Probolinggo',
    'SUB': 'Tanjung Perak',
    'IBP': 'Banten',
    'BOA': 'Benoa',
    'CEB': 'Celukan Bawang',
    'BAD': 'Badas',
    'BMU': 'Bima',
    'LMR': 'Lembar',
    'ENE': 'Ende',
    'IPI': 'Ippi',
    'KBH': 'Kalabahi',
    'LBO': 'Labuan Bajo',
    'MOF': 'Maumere/ Lorens Say',
    'REO': 'Reo',
    'ISA': 'Seba',
    'TQP': 'Tenau/ Kupang',
    'WGP': 'Waingapu',
    'PNK': 'Pontianak',
    'SNE': 'Sintete',
    'KPE': 'Kuala Pembuang',
    'KUM': 'Kumai',
    'PLB': 'Pangkalan Bun',
    'PMI': 'Pegatan Mendawai',
    'PPS': 'Pulang Pisau',
    'SMQ': 'Sampit',
    'SMA': 'Samuda',
    'SAA': 'Sukamara',
    'BDJ': 'Banjarmasin',
    'SUR': 'Satui',
    'BPN': 'Balikpapan',
    'LTU': 'Lhok Tuan',
    'SGQ': 'Sangatta',
    'SKI': 'Sangkulirang',
    'TLA': 'Tanjung Laut',
    'TRE': 'Tanjung Redeb',
    'TTK': 'Nunukan / Tunon Taka',
    'MLD': 'Tarakan / Malundung',
    'LUK': 'Labuhan Uki',
    'MDC': 'Manado',
    'BGG': 'Banggai',
    'DNA': 'Donggala',
    'LUW': 'Luwuk',
    'PTL': 'Pantoloan',
    'TLI': 'Toli-toli',
    'WNQ': 'Wani',
    'BAE': 'Bajoe',
    'MAK': 'Makassar',
    'ARE': 'Pare-Pare',
    'YAR': 'Selayar/Benteng/Rauf Rahman',
    'BBM': 'Bau-Bau/Murhum',
    'KDI': 'Kendari/Bungkutoko',
    'KOL': 'Kolaka',
    'NGG': 'Anggrek',
    'GTO': 'Gorontalo',
    'MJU': 'Mamuju',
    'AMQ': 'Ambon',
    'NDA': 'Banda Naira',
    'LQA': 'Leksula',
    'NAM': 'Leksula',
    'SXK': 'Saumlaki',
    'TUA': 'Tual',
    'DRA': 'Daruba',
    'LBH': 'Labuha',
    'SIO': 'Soasio/Goto',
    'TEI': 'Ternate/ A. Yani',
    'TBO': 'Tobelo',
    'BIK': 'Biak',
    'DJJ': 'Jayapura',
    'MKQ': 'Merauke',
    'NBX': 'Nabire',
    'FKQ': 'Fak-fak',
    'MKW': 'Manokwari',
    'SOQ': 'Sorong',
    'WSR': 'Wasior',
    'ISG': 'Sapudi',
    'BIT': 'Bitung'}

    mapping_expr = create_map([lit(x) for x in chain(*kode_to_nama.items())])
    recap_df = recap_df.withColumn("nama_pelabuhan", element_at(mapping_expr, col("Port")))

    durasi_label = f"{port_time_limit}menit"
    parts = [activity, durasi_label]
    if port_code:
        parts.append(f"port_{port_code.lower()}")
    if month_obs:
        parts.append(f"bulan_{month_obs:02d}")
    if year_obs:
        parts.append(f"tahun_{year_obs}")

    title = " | ".join(parts).replace("_", " ").title()
    filename = "_".join(parts) + ".csv"

    # create_download_link(recap_df, title=title, filename=filename)

    return recap_df

def list_vessel_through_hormuz(
    ais_activity,
    activity="arrival",
    port_code=None,
    month_obs=None,
    year_obs=None,
    port_time_limit=0,
    spark=None
):
    if spark is None:
        raise ValueError("spark session harus di-pass")

    ais_activity = ensure_spark_df(ais_activity, spark)

    df = ais_activity.withColumn(
        "status_country_domicile",
        F.when(F.col("GroupBeneficialOwnerCountryofDomicile") == "Indonesia", "Domestic").otherwise("Foreign")
    )

    min_durasi_detik = port_time_limit * 60

    if activity == "arrival":
        df_filtered = df.filter(
            (col("masuk_pelabuhan") == "masuk") &
            (col("position") == "in")
        )
        if port_time_limit > 0:
            df_filtered = df_filtered.filter(col("time") >= min_durasi_detik)

    elif activity == "departed":
        if port_time_limit == 0:
            df_filtered = df.filter(
                (col("keluar_pelabuhan") == "keluar") &
                (col("position") == "in")
            )
        elif port_time_limit > 0:
            df_pandas = df.toPandas()
            df_pandas_2 = df_pandas.sort_values(by=["mmsi", "dt_pos_utc"]).reset_index(drop=True)
            df_pandas_2["prev_time_2"] = df_pandas_2["time"].shift(2)
            df_pandas_2["pos"] = df_pandas_2["position"].shift(2)
            df_pandas_2["in"] = df_pandas_2["masuk_pelabuhan"].shift(2)
            df_pandas_2["Port_prev"] = df_pandas_2["Port"].shift(1)
            df_pandas_2["mmsi_prev"] = df_pandas_2["mmsi"].shift(2)

            if "is_through_hormuz" in df_pandas_2.columns:
                df_pandas_2["is_through_hormuz_prev"] = df_pandas_2["is_through_hormuz"].shift(1)

            df_filtered = df_pandas_2[
                (df_pandas_2["position"] == "out") &
                (df_pandas_2["keluar_pelabuhan"] == "keluar") &
                (df_pandas_2["prev_time_2"] >= min_durasi_detik) &
                (df_pandas_2["pos"] == "in") &
                (df_pandas_2["in"] == "masuk") &
                (df_pandas_2["mmsi"] == df_pandas_2["mmsi_prev"])
            ]
            df_filtered.loc[:, "Port"] = df_filtered["Port_prev"]

            if "is_through_hormuz_prev" in df_filtered.columns:
                df_filtered.loc[:, "is_through_hormuz"] = df_filtered["is_through_hormuz_prev"]

            df_filtered["dt_pos_utc"] = pd.to_datetime(df_filtered["dt_pos_utc"]).dt.date
            df_filtered = ensure_spark_df(df_filtered, spark)
    else:
        raise ValueError("activity harus 'arrival' atau 'departed'")

    df_filtered = df_filtered \
        .withColumn("date", date_format(col("dt_pos_utc"), "yyyy-MM-dd")) \
        .withColumn("month", month(col("dt_pos_utc"))) \
        .withColumn("year", year(col("dt_pos_utc")))

    if port_code:
        df_filtered = df_filtered.filter(col("Port") == port_code)
    if month_obs:
        df_filtered = df_filtered.filter(col("month") == month_obs)
    if year_obs:
        df_filtered = df_filtered.filter(col("year") == year_obs)

    if "is_through_hormuz" in df_filtered.columns:
        df_filtered = df_filtered.filter(col("is_through_hormuz") == True)
    else:
        raise ValueError("Kolom 'is_through_hormuz' tidak ditemukan. Pastikan data sudah di-flag sebelumnya.")

    select_cols = [
        "mmsi", "vessel_name", "vessel_type", 
        "vessel_type_main", "vessel_type_sub", "fc_vessel",
        "GroupBeneficialOwnerCountryofDomicile",
        "GroupBeneficialOwnerCountryOfRegistration",
        "status_country_domicile",
        "Port", "date", "draught", "ns_vessel", "sog",
        "latitude", "longitude", "H3_int_index_8",
        "is_through_hormuz"
    ]

    for optional_col in ["first_detected_hormuz", "last_detected_hormuz"]:
        if optional_col in df_filtered.columns:
            select_cols.append(optional_col)

    df_vessels = df_filtered.select(*select_cols).dropDuplicates(["mmsi", "Port", "date"])

    kode_to_nama = {
        'CLG': 'Calang',
        'KUA': 'Kuala Langsa',
        'LSW': 'Lhokseumawe/Kreung Geukeh',
        'MLH': 'Malahayati',
        'MEQ': 'Meulaboh',
        'SBG': 'Sabang',
        'SNL': 'Singkil',
        'BLW': 'Belawan',
        'GNS': 'Gunung Sitoli',
        'KTJ': 'Kuala Tanjung',
        'LHA': 'Lahewa',
        'INL': 'Natal/ Sikara-kara',
        'PDD': 'Pangkalan Dodek',
        'PKS': 'Pangkalan Susu',
        'PTE': 'Pulau Tello',
        'SLG': 'Sibolga',
        'SRU': 'Sirombu',
        'TBE': 'Tanjung Beringin',
        'TDA': 'Teluk Dalam',
        'LIG': 'Teluk Leidong',
        'MPG': 'Muara Padang',
        'TBY': 'Teluk Bayur',
        'IBI': 'Bagan Siapi-api',
        'BKI': 'Bengkalis',
        'DUM': 'Dumai',
        'ENO': 'Kuala Enok',
        'MRT': 'Meranti/ Dorak',
        'RCI': 'Rengat/Kuala Cinaku',
        'SLJ': 'Selat Panjang',
        'SUQ': 'Sungai Guntung',
        'TMD': 'Tanjung Medang',
        'TLN': 'Tembilahan',
        'MSK': 'Muara Sabak',
        'TNU': 'Talang Duku',
        'ING': 'Boom Baru/ Palembang',
        'TGP': 'Tanjung Api-Api',
        'BKS': 'Pulau Baai',
        'TAG': 'Kota Agung/ Batu Balai',
        'PNJ': 'Panjang',
        'BLU': 'Belinyu',
        'MUO': 'Muntok',
        'PGX': 'Pangkal Balam',
        'TJQ': 'Tanjung Pandan',
        'BUR': 'Batam',
        'SKJ': 'Sei Kolak Kijang',
        'TJB': 'Tanjung Balai Karimun',
        'TBD': 'Tanjung Batu Kundur',
        'TNJ': 'Tanjung Pinang',
        'IMW': 'Marunda',
        'TPR': 'Tanjung priok',
        'CBN': 'Cirebon',
        'PDR': 'Pangandaran/Bojongsalawe',
        'PTM': 'Patimban',
        'SRG': 'Tanjung Emas',
        'CXP': 'Tanjung Intan',
        'BWB': 'Banyu Wangi/ Boom',
        'GRE': 'Gresik',
        'KAT': 'Kalianget',
        'PAZ': 'Pasuruan',
        'AEA': 'Probolinggo',
        'SUB': 'Tanjung Perak',
        'IBP': 'Banten',
        'BOA': 'Benoa',
        'CEB': 'Celukan Bawang',
        'BAD': 'Badas',
        'BMU': 'Bima',
        'LMR': 'Lembar',
        'ENE': 'Ende',
        'IPI': 'Ippi',
        'KBH': 'Kalabahi',
        'LBO': 'Labuan Bajo',
        'MOF': 'Maumere/ Lorens Say',
        'REO': 'Reo',
        'ISA': 'Seba',
        'TQP': 'Tenau/ Kupang',
        'WGP': 'Waingapu',
        'PNK': 'Pontianak',
        'SNE': 'Sintete',
        'KPE': 'Kuala Pembuang',
        'KUM': 'Kumai',
        'PLB': 'Pangkalan Bun',
        'PMI': 'Pegatan Mendawai',
        'PPS': 'Pulang Pisau',
        'SMQ': 'Sampit',
        'SMA': 'Samuda',
        'SAA': 'Sukamara',
        'BDJ': 'Banjarmasin',
        'SUR': 'Satui',
        'BPN': 'Balikpapan',
        'LTU': 'Lhok Tuan',
        'SGQ': 'Sangatta',
        'SKI': 'Sangkulirang',
        'TLA': 'Tanjung Laut',
        'TRE': 'Tanjung Redeb',
        'TTK': 'Nunukan / Tunon Taka',
        'MLD': 'Tarakan / Malundung',
        'LUK': 'Labuhan Uki',
        'MDC': 'Manado',
        'BGG': 'Banggai',
        'DNA': 'Donggala',
        'LUW': 'Luwuk',
        'PTL': 'Pantoloan',
        'TLI': 'Toli-toli',
        'WNQ': 'Wani',
        'BAE': 'Bajoe',
        'MAK': 'Makassar',
        'ARE': 'Pare-Pare',
        'YAR': 'Selayar/Benteng/Rauf Rahman',
        'BBM': 'Bau-Bau/Murhum',
        'KDI': 'Kendari/Bungkutoko',
        'KOL': 'Kolaka',
        'NGG': 'Anggrek',
        'GTO': 'Gorontalo',
        'MJU': 'Mamuju',
        'AMQ': 'Ambon',
        'NDA': 'Banda Naira',
        'LQA': 'Leksula',
        'NAM': 'Leksula',
        'SXK': 'Saumlaki',
        'TUA': 'Tual',
        'DRA': 'Daruba',
        'LBH': 'Labuha',
        'SIO': 'Soasio/Goto',
        'TEI': 'Ternate/ A. Yani',
        'TBO': 'Tobelo',
        'BIK': 'Biak',
        'DJJ': 'Jayapura',
        'MKQ': 'Merauke',
        'NBX': 'Nabire',
        'FKQ': 'Fak-fak',
        'MKW': 'Manokwari',
        'SOQ': 'Sorong',
        'WSR': 'Wasior',
        'ISG': 'Sapudi',
        'BIT': 'Bitung'}
    
    mapping_expr = create_map([lit(x) for x in chain(*kode_to_nama.items())])
    df_vessels = df_vessels.withColumn("nama_pelabuhan", element_at(mapping_expr, col("Port")))

    return df_vessels

def list_vessel_not_through_hormuz(
    ais_activity,
    activity="arrival",
    port_code=None,
    month_obs=None,
    year_obs=None,
    port_time_limit=0,
    spark=None
):
    if spark is None:
        raise ValueError("spark session harus di-pass")

    ais_activity = ensure_spark_df(ais_activity, spark)

    df = ais_activity.withColumn(
        "status_country_domicile",
        F.when(F.col("GroupBeneficialOwnerCountryofDomicile") == "Indonesia", "Domestic").otherwise("Foreign")
    )

    min_durasi_detik = port_time_limit * 60

    if activity == "arrival":
        df_filtered = df.filter(
            (col("masuk_pelabuhan") == "masuk") &
            (col("position") == "in")
        )
        if port_time_limit > 0:
            df_filtered = df_filtered.filter(col("time") >= min_durasi_detik)

    elif activity == "departed":
        if port_time_limit == 0:
            df_filtered = df.filter(
                (col("keluar_pelabuhan") == "keluar") &
                (col("position") == "in")
            )
        elif port_time_limit > 0:
            df_pandas = df.toPandas()
            df_pandas_2 = df_pandas.sort_values(by=["mmsi", "dt_pos_utc"]).reset_index(drop=True)
            df_pandas_2["prev_time_2"] = df_pandas_2["time"].shift(2)
            df_pandas_2["pos"] = df_pandas_2["position"].shift(2)
            df_pandas_2["in"] = df_pandas_2["masuk_pelabuhan"].shift(2)
            df_pandas_2["Port_prev"] = df_pandas_2["Port"].shift(1)
            df_pandas_2["mmsi_prev"] = df_pandas_2["mmsi"].shift(2)

            if "is_through_hormuz" in df_pandas_2.columns:
                df_pandas_2["is_through_hormuz_prev"] = df_pandas_2["is_through_hormuz"].shift(1)

            df_filtered = df_pandas_2[
                (df_pandas_2["position"] == "out") &
                (df_pandas_2["keluar_pelabuhan"] == "keluar") &
                (df_pandas_2["prev_time_2"] >= min_durasi_detik) &
                (df_pandas_2["pos"] == "in") &
                (df_pandas_2["in"] == "masuk") &
                (df_pandas_2["mmsi"] == df_pandas_2["mmsi_prev"])
            ]
            df_filtered.loc[:, "Port"] = df_filtered["Port_prev"]

            if "is_through_hormuz_prev" in df_filtered.columns:
                df_filtered.loc[:, "is_through_hormuz"] = df_filtered["is_through_hormuz_prev"]

            df_filtered["dt_pos_utc"] = pd.to_datetime(df_filtered["dt_pos_utc"]).dt.date
            df_filtered = ensure_spark_df(df_filtered, spark)
    else:
        raise ValueError("activity harus 'arrival' atau 'departed'")

    df_filtered = df_filtered \
        .withColumn("date", date_format(col("dt_pos_utc"), "yyyy-MM-dd")) \
        .withColumn("month", month(col("dt_pos_utc"))) \
        .withColumn("year", year(col("dt_pos_utc")))

    if port_code:
        df_filtered = df_filtered.filter(col("Port") == port_code)
    if month_obs:
        df_filtered = df_filtered.filter(col("month") == month_obs)
    if year_obs:
        df_filtered = df_filtered.filter(col("year") == year_obs)

    if "is_through_hormuz" in df_filtered.columns:
        df_filtered = df_filtered.filter(col("is_through_hormuz") == False)
    else:
        raise ValueError("Kolom 'is_through_hormuz' tidak ditemukan. Pastikan data sudah di-flag sebelumnya.")

    select_cols = [
        "mmsi", "vessel_name", "vessel_type", 
        "vessel_type_main", "vessel_type_sub", "fc_vessel",
        "GroupBeneficialOwnerCountryofDomicile",
        "GroupBeneficialOwnerCountryOfRegistration",
        "status_country_domicile",
        "Port", "date", "draught", "ns_vessel", "sog",
        "latitude", "longitude", "H3_int_index_8",
        "is_through_hormuz"
    ]

    for optional_col in ["first_detected_hormuz", "last_detected_hormuz"]:
        if optional_col in df_filtered.columns:
            select_cols.append(optional_col)

    df_vessels = df_filtered.select(*select_cols).dropDuplicates(["mmsi", "Port", "date"])

    kode_to_nama = {
        'CLG': 'Calang',
        'KUA': 'Kuala Langsa',
        'LSW': 'Lhokseumawe/Kreung Geukeh',
        'MLH': 'Malahayati',
        'MEQ': 'Meulaboh',
        'SBG': 'Sabang',
        'SNL': 'Singkil',
        'BLW': 'Belawan',
        'GNS': 'Gunung Sitoli',
        'KTJ': 'Kuala Tanjung',
        'LHA': 'Lahewa',
        'INL': 'Natal/ Sikara-kara',
        'PDD': 'Pangkalan Dodek',
        'PKS': 'Pangkalan Susu',
        'PTE': 'Pulau Tello',
        'SLG': 'Sibolga',
        'SRU': 'Sirombu',
        'TBE': 'Tanjung Beringin',
        'TDA': 'Teluk Dalam',
        'LIG': 'Teluk Leidong',
        'MPG': 'Muara Padang',
        'TBY': 'Teluk Bayur',
        'IBI': 'Bagan Siapi-api',
        'BKI': 'Bengkalis',
        'DUM': 'Dumai',
        'ENO': 'Kuala Enok',
        'MRT': 'Meranti/ Dorak',
        'RCI': 'Rengat/Kuala Cinaku',
        'SLJ': 'Selat Panjang',
        'SUQ': 'Sungai Guntung',
        'TMD': 'Tanjung Medang',
        'TLN': 'Tembilahan',
        'MSK': 'Muara Sabak',
        'TNU': 'Talang Duku',
        'ING': 'Boom Baru/ Palembang',
        'TGP': 'Tanjung Api-Api',
        'BKS': 'Pulau Baai',
        'TAG': 'Kota Agung/ Batu Balai',
        'PNJ': 'Panjang',
        'BLU': 'Belinyu',
        'MUO': 'Muntok',
        'PGX': 'Pangkal Balam',
        'TJQ': 'Tanjung Pandan',
        'BUR': 'Batam',
        'SKJ': 'Sei Kolak Kijang',
        'TJB': 'Tanjung Balai Karimun',
        'TBD': 'Tanjung Batu Kundur',
        'TNJ': 'Tanjung Pinang',
        'IMW': 'Marunda',
        'TPR': 'Tanjung priok',
        'CBN': 'Cirebon',
        'PDR': 'Pangandaran/Bojongsalawe',
        'PTM': 'Patimban',
        'SRG': 'Tanjung Emas',
        'CXP': 'Tanjung Intan',
        'BWB': 'Banyu Wangi/ Boom',
        'GRE': 'Gresik',
        'KAT': 'Kalianget',
        'PAZ': 'Pasuruan',
        'AEA': 'Probolinggo',
        'SUB': 'Tanjung Perak',
        'IBP': 'Banten',
        'BOA': 'Benoa',
        'CEB': 'Celukan Bawang',
        'BAD': 'Badas',
        'BMU': 'Bima',
        'LMR': 'Lembar',
        'ENE': 'Ende',
        'IPI': 'Ippi',
        'KBH': 'Kalabahi',
        'LBO': 'Labuan Bajo',
        'MOF': 'Maumere/ Lorens Say',
        'REO': 'Reo',
        'ISA': 'Seba',
        'TQP': 'Tenau/ Kupang',
        'WGP': 'Waingapu',
        'PNK': 'Pontianak',
        'SNE': 'Sintete',
        'KPE': 'Kuala Pembuang',
        'KUM': 'Kumai',
        'PLB': 'Pangkalan Bun',
        'PMI': 'Pegatan Mendawai',
        'PPS': 'Pulang Pisau',
        'SMQ': 'Sampit',
        'SMA': 'Samuda',
        'SAA': 'Sukamara',
        'BDJ': 'Banjarmasin',
        'SUR': 'Satui',
        'BPN': 'Balikpapan',
        'LTU': 'Lhok Tuan',
        'SGQ': 'Sangatta',
        'SKI': 'Sangkulirang',
        'TLA': 'Tanjung Laut',
        'TRE': 'Tanjung Redeb',
        'TTK': 'Nunukan / Tunon Taka',
        'MLD': 'Tarakan / Malundung',
        'LUK': 'Labuhan Uki',
        'MDC': 'Manado',
        'BGG': 'Banggai',
        'DNA': 'Donggala',
        'LUW': 'Luwuk',
        'PTL': 'Pantoloan',
        'TLI': 'Toli-toli',
        'WNQ': 'Wani',
        'BAE': 'Bajoe',
        'MAK': 'Makassar',
        'ARE': 'Pare-Pare',
        'YAR': 'Selayar/Benteng/Rauf Rahman',
        'BBM': 'Bau-Bau/Murhum',
        'KDI': 'Kendari/Bungkutoko',
        'KOL': 'Kolaka',
        'NGG': 'Anggrek',
        'GTO': 'Gorontalo',
        'MJU': 'Mamuju',
        'AMQ': 'Ambon',
        'NDA': 'Banda Naira',
        'LQA': 'Leksula',
        'NAM': 'Leksula',
        'SXK': 'Saumlaki',
        'TUA': 'Tual',
        'DRA': 'Daruba',
        'LBH': 'Labuha',
        'SIO': 'Soasio/Goto',
        'TEI': 'Ternate/ A. Yani',
        'TBO': 'Tobelo',
        'BIK': 'Biak',
        'DJJ': 'Jayapura',
        'MKQ': 'Merauke',
        'NBX': 'Nabire',
        'FKQ': 'Fak-fak',
        'MKW': 'Manokwari',
        'SOQ': 'Sorong',
        'WSR': 'Wasior',
        'ISG': 'Sapudi',
        'BIT': 'Bitung'}
    
    mapping_expr = create_map([lit(x) for x in chain(*kode_to_nama.items())])
    df_vessels = df_vessels.withColumn("nama_pelabuhan", element_at(mapping_expr, col("Port")))

    return df_vessels

def data_reduction_in_buffer(ais, buffer_hexes, resolution: int = 8):
    """
    Join data AIS dengan buffer hex berdasarkan resolusi H3 yang diberikan.

    Parameters:
    -----------
    ais : DataFrame
        Data AIS yang memiliki kolom H3_int_index_{resolution}
    buffer_hexes : DataFrame
        Data hexagon buffer dengan kolom 'boundary_h3'
    resolution : int
        Resolusi H3 yang digunakan (default: 8)

    Returns:
    --------
    DataFrame hasil join
    """
    h3_column = f"H3_int_index_{resolution}"

    if h3_column not in ais.columns:
        raise ValueError(f"Kolom '{h3_column}' tidak ditemukan di data AIS.")

    ais_buffer_joined = ais.join(
        buffer_hexes,
        ais[h3_column] == buffer_hexes["boundary_h3"],
        "inner"
    )

    return ais_buffer_joined

def _result_calculator(the_trigger, delta_time_lower, delta_time_upper, min_valid_records, factor=2,geom_type='box'):
    '''
    Intenal funtion to calculate a result from stopped ship event (the_trigger)
    
    Parameters
    ----------
    the_trigger - list: from termination of stopped ship by either escape
        or end of time period.
    delta_time_lower - float: lower time estimate of stopped ship event
        calculated by _lower_upper_time_estimates
    delta_time_upper - float: upper time estimate of stopped ship event
        calculated by _lower_upper_time_estimates  
    min_valid_records - integer: minimum size of observations wrapped up into the_trigger for which
        it is acceptable to use this method.  This is driven by statistical imperatives in so far as
        it uses standard deviations and this requires a minimum number of observations to be valid.
    factor - integer{default:2}: used to create bounding box.  Corresponds to the number of standard deviations
        used for box creation
    geom_type - string{default:'box'} controls types of wkt returned
        'box': Returns well known text (wkt) representation of bounding box based on
            average lat/long standard deviations of observations used
        'tri-line': Creates a tri-line of three points as well known text (wkt).
            The three points of the tri line correspond to: 
            -trigger event, 
            -average co-ord, 
            -last co-ordinates in stopping event (observation prior to disarm event)
        'point': Creates a point as well known text (wkt).
            point corresponds to trigger event.
    
    
    Returns
    ----------
    tuple - (string, list, header_list)
        string contains flag as to validity of stopping event
        list contains data of stopping event
        header_list of strings for naming list down stream
    
    NOTES
    ----------
    We have expectation on schema of the trigger
                the_trigger.append(row[idx_h3])  #0
                the_trigger.append(the_mmsi) #1
                the_trigger.append(the_imo) #2
                the_trigger.append(row[idx_obs_time]) #3 trigger time
                the_trigger.append(row[idx_ais_length]) #4
                the_trigger.append(row[idx_ais_width]) #5
                the_trigger.append(row[idx_ais_vessel_type]) #6
                the_trigger.append(row[idx_ais_vessel_type_main]) #7
                
                the_trigger.append([row[idx_nav]]) #-6 [list of of navigation status]
                the_trigger.append([row[idx_cluster_time]]) #-5 [list of unix times observed]
                the_trigger.append([row[idx_lat]]) #-4 [list of lat observations]
                the_trigger.append([row[idx_lng]]) #-3 [list of lng observations]
                
                the_trigger.append(state_value) #-2
                the_trigger.append(1) #-1  number of records observed is last              
                
    We have expectation of schema of the result
                h3
                mmsi
                imo
                ais_length
                ais_width
                ais_vessel_type
                ais_vessel_type_main
                obs_time
                visit_lower
                visit_upper
                unix_trigger
                unix_average
                unix_disarm
                standard_deviation_lat
                standard_deviation_lat
                trigger_lat
                trigger_lng
                average_lat
                average_lng
                disarm_lat
                disarm_lng
                state_start
                state_end
                record_count
                status
                wkt [box|triline|point]
                wkt_avg_pt
            

    '''
    from ais_aoi_integrated.upload_data import load as cso_load
    from ais_aoi_integrated.cso_ais import cso_ais as cso_a
    from ais_aoi_integrated.cso_utility import cso_utility as cso_u
    from ais_aoi_integrated.cso_proximity import cso_proximity as cso_p
    import math 
    result = []
    
    # define header list that will can be used to convert to data frame: save someone having to do this later.
    '''header_list = ['h3_index_int','mmsi','imo','length','width','vessel_type','vessel_type_main','obs_time','flag_country','source','destination',
        'time_lower','time_upper','unix_trigger','avg_unix','unix_disarm','sd_lat','sd_lng',
        'trigger_lat','trigger_lng','avg_lat','avg_lng','disarm_lat','disarm_lng',
        'state_initial','state_final','obs_count','is_valid','geom_wkt']'''
    
    header_list =['h3_index_int',
                  'mmsi','imo','length','width','vessel_type','vessel_type_main',
                  'obs_time','flag_country','source','destination','time_lower','time_upper','unix_trigger','avg_unix','unix_disarm',
                  'sd_lat','sd_lng','trigger_lat','trigger_lng','avg_lat','avg_lng','disarm_lat','disarm_lng',
                  'mode_nav_status', 'obs_nav_status',
                  'state_initial','state_final','obs_count','is_valid','geom_wkt','wkt_avg_pt']
    
    
    try:
        if not (geom_type=='box' or geom_type=='tri-line' or geom_type=='point'  ):
            raise ValueError("geom_type input: only one of box|tri-line|point are acceptable values.")



        # we assume status is valid and mutate for case where it is not!
        #status = 'valid'
        
        # get trigger location and time
        trigger_lat = the_trigger[-4][0]
        trigger_lng = the_trigger[-3][0]
        unix_trigger= the_trigger[-5][0]
        
        # get average location and time
        avg_lat = cso_u.calc_list_average(the_trigger[-4])
        avg_lng = cso_u.calc_list_average(the_trigger[-3])
        avg_unix = math.floor(cso_u.calc_list_average(the_trigger[-5]))# always want an integer second. use floor
        
        # get mode of navigation status and number of observations in mode
        #print(the_trigger[-6])
        full_mode_nav_status = cso_u.calc_list_mode(the_trigger[-6],1) # get last value in mode list
        mode_nav_status = full_mode_nav_status[0] # get the mode value
        obs_nav_status = full_mode_nav_status[1] # gets the count of time this is use in this stopped ship event
        
        # get disarm location and time
        disarm_lat = the_trigger[-4][-1]
        disarm_lng = the_trigger[-3][-1]  
        unix_disarm = the_trigger[-5][-1] 
        
          
        #deal with valid stopping events, sufficent observations
        if the_trigger[-1]> min_valid_records:
            status = 'valid'

            # internal helper acting on lat/lng now safe to do standard deviation caluation
            standard_deviation_lat = cso_u.calc_list_standard_deviation(the_trigger[-4])
            standard_deviation_lng = cso_u.calc_list_standard_deviation(the_trigger[-3])

            # deal with geom and wkt value now.
            if geom_type == 'box':
                wkt = cso_u.calc_stddev_boundingbox_wkt(avg_lat,avg_lng,standard_deviation_lat,standard_deviation_lng,factor)
            elif geom_type == 'tri-line':
                wkt = cso_u.calc_tri_line_wkt(trigger_lat,trigger_lng, avg_lat, avg_lng, disarm_lat, disarm_lng)
            else:
                wkt = cso_u.calc_point_wkt(trigger_lat,trigger_lng)
                
            wkt_avg_pt = cso_u.calc_point_wkt(avg_lat,avg_lng)

        #deal with invalid stopping events, insufficent observations
        else:
            status = 'invalid'
            wkt = cso_u.calc_point_wkt(trigger_lat,trigger_lng)
            wkt_avg_pt = cso_u.calc_point_wkt(trigger_lat,trigger_lng)
            # set all non trigger inputs to 0
            standard_deviation_lat = 0 
            standard_deviation_lng = 0


        # populate result with trigger data passed in to function 
        result.append(the_trigger[0]) # h3
        result.append(the_trigger[1]) # mmsi
        result.append(the_trigger[2]) # imo
        result.append(the_trigger[4]) # ais_length
        result.append(the_trigger[5]) # ais_width
        result.append(the_trigger[6]) # ais_vessel_type
        result.append(the_trigger[7]) # ais_vessel_type_main
        result.append(the_trigger[3]) # obs_time
        result.append(the_trigger[8]) # flag_country
        result.append(the_trigger[9]) # source
        result.append(the_trigger[10]) # destination
        
        # Now append passed in values to function
        result.append(delta_time_lower)
        result.append(delta_time_upper)        
        
        # Now append result with data derived in function
        result.append(unix_trigger)
        result.append(avg_unix)
        result.append(unix_disarm)
        result.append(standard_deviation_lat)
        result.append(standard_deviation_lng)
        result.append(trigger_lat)
        result.append(trigger_lng)
        result.append(avg_lat)
        result.append(avg_lng)
        result.append(disarm_lat)
        result.append(disarm_lng)
        
        result.append(mode_nav_status)
        result.append(obs_nav_status)
        
        result.append(the_trigger[-2][0]) # State of observation stream at start of stopping event
        result.append(the_trigger[-2][1]) # State of observation stream at end of stopping event
        result.append(the_trigger[-1]) # Number of obserations
        result.append(status)
        result.append(wkt)
        result.append(wkt_avg_pt)
        #debug print(len(header_list))
        #debug print(len(result))
        

        # check if outputs are internally consistent.
        if (len(header_list)!=len(result)):
            raise ValueError("length of internal header list must match length of intended result\n \
                             i.e len(_result_calculator[1]) != len(_result_calculator[2])")
            
        return status, result, header_list

    except Exception as e:
        print(f'Result calculation fail...')
        print(f'.....')
        print(f'Error: \n{e}\n')

def min_obs(vessel_type):
    if vessel_type.lower() == "passenger":
        min_observations = 2
        min_ship_observations = 2
    else:
        min_observations = 10
        min_ship_observations = 20
    return min_observations, min_ship_observations


def algorithm_stationary(ais, resolution: int = 10, buffer_radius_m: float = 10000,  spark=None):
    from ais_aoi_integrated.upload_data import load as cso_load
    from ais_aoi_integrated.cso_ais import cso_ais as cso_a
    from ais_aoi_integrated.cso_utility import cso_utility as cso_u
    from ais_aoi_integrated.cso_proximity import cso_proximity as cso_p

    import math 
    from functools import reduce
    import warnings
    warnings.filterwarnings("ignore")


    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")

    ais = ensure_spark_df(ais, spark)
    aoi_ais = read_aoi("Manual")
    if (resolution < 10):
        selected_ports = ['REO', 'MPG', 'RCI', 'PTE', 'MRT', 'MDC']
        aoi_ais = aoi_ais[~aoi_ais['Kode'].str.upper().isin(selected_ports)]
    print(f"\nBuffer di sekitar pelabuhan dengan Kode REO, MPG, RCI, PTE, MRT, dan MDC tidak diikutsertakan karena terlalu kecil untuk didefinisikan dengan resolusi H3 Index di bawah 10")

    print(f"\nMembentuk buffer di sekitar AOI")
    buffer_hex = generate_buffer_hex(aoi_ais, buffer_radius_m, resolution)
    buffer_sdf = change_buffer_type(buffer_hex, spark)
    print(f"\nMengekstrak data AIS di dalam buffer")
    ais_in_buffer = data_reduction_in_buffer(ais, buffer_sdf, resolution)
    ais_in_buffer = cso_a.cso_ais2unixtime(spark,ais_in_buffer)
    print(f"\nMemulai algoritma")

    startWorkup= datetime.now()

    ais_in_buffer.createOrReplaceTempView("a")
    ais_data_nogeom =spark.sql(
    f'''
    select 
        a.*
    FROM
        a 
    '''
    )

    ais_data_nogeom = ais_data_nogeom.drop("geom").cache()

    print(f"Process takes time\nOf:{datetime.now()-startWorkup}")

    try:
        del startWorkup
    except:
        pass

    startWorkup= datetime.now()
    iterationList = ais_data_nogeom.select('mmsi').dropDuplicates().toPandas()['mmsi'].to_list()
    print(f"\nNumber of mmsi  identifed in data Is:{len(iterationList)}\n")
    print(f"Process takes time\nOf:{datetime.now()-startWorkup}")

    #parameter control
    std_dev_factor = 1
    #geom_type = 'box'
    geom_type = 'tri-line'

    #hasil
    the_insufficent = []
    the_nonVisits = []
    the_stopped_ship = []

    set_ship = set()

    #index mapping
    idx_h3 = 0 
    idx_cluster_time = 1
    idx_nav = 2
    idx_obs_time = 3
    idx_ais_length = 4
    idx_ais_width  = 5
    idx_ais_vessel_type = 6
    idx_ais_vessel_type_main = 7
    idx_flag_country = 8
    idx_source = 9
    idx_destination = 10
    idx_sog = 11
    idx_cog = 12
    idx_imo = 13
    idx_goss_tonnage = 11
    idx_lat = -2
    idx_lng = -1

    #variabel event berenti atau the_trigger
    idx_ref_counts= -1
    idx_ref_state = -2 
    idx_lng_list = -3 
    idx_lat_list = -4 
    idx_time_list = -5
    idx_nav_status_list = -6 
    
    #urut waktu
    time_sort_order = True
    
    time_factor = 1 
    if not time_sort_order:
        time_factor = -1

    test_list = iterationList
    #test_list

    the_k_depth = 3
    the_h3  =  f"H3_int_index_{resolution}"

    subset_df = ais_data_nogeom.select('mmsi',
                                   the_h3, #remember this is a hyper-parameter value!
                      "cluster_time",
                      "nav_status",
                      "dt_pos_utc",
                      "length",
                      "width",
                      "vessel_type",
                      "vessel_type_main", 
                      "flag_country",
                      "source",
                      "destination",
                      "sog",
                      "cog", 
                      "imo",
                      "latitude",
                      "longitude").cache()
    
    startProcess = datetime.now()

    mmsi_list_length = len(test_list)
    mmsi_list_count = 0

    for x in test_list:
        mmsi_list_count += 1
        the_mmsi = x
        #the_mmsi = x[0]
        the_imo = -1 
        isFirst = True
        isLast = False
        the_trigger = []
        the_prior = []
        the_previous = []    
        
        # ubah list MMSI jadi pandas
        f = subset_df.filter(subset_df['mmsi']==the_mmsi).toPandas()
        
        # buat baris baru
        g = f.loc[(f['mmsi']== the_mmsi) ,
                        [the_h3, #hyper-parameter
                        "cluster_time",
                        "nav_status",
                        "dt_pos_utc",

                        "length",
                        "width",
                        "vessel_type",
                        "vessel_type_main",                      
                        "flag_country",
                        "source",
                        "destination",
                        "sog",
                        "cog", 
                        "imo",
                        "latitude",
                        "longitude"]].sort_values(by=['cluster_time'])
        
        vessel_type_value = g["vessel_type"].iloc[0]  

        min_ship_observations, min_observations = min_obs(vessel_type_value)
        observations =  len(g.index)
        
        # cek min observasi
        if observations <= min_ship_observations:
            print(f"MMSI {the_mmsi} skipped due to insufficient observations: {observations}")
            the_insufficent.append([f"Insufficent obs:{len(g.index)}",
                                    the_mmsi                     
                                ])

            if mmsi_list_count % 20 == 0:
                print(f"Processed {round(mmsi_list_count/mmsi_list_length*100)}%: {mmsi_list_count} of {mmsi_list_length} mmsi ")
            elif mmsi_list_count == mmsi_list_length:
                print(f"Done! Processed: {mmsi_list_count} of {mmsi_list_length} mmsi ")        
            
            continue
        else:
            count = 0    
            
        for row_index,row in g.iterrows():
            count = count+1
            if count > 1 and isFirst == True :
                isFirst = False
            if count == observations and not isLast:
                isLast = True

            # 1 special case first observation --> Ship was stopped initially at start of time period
            # 2 normal case mid stream neither special case of first or last observaion --> Ship stopped during time period
            # 3 special case last observation --> Ship stopped at end of time period
            state = 2
            if isFirst:
                state = 1 
            if isLast:
                state =3        

            #snapshot kondisi kapal pada satu waktu
            the_current = [
                            row[idx_h3], #0
                            row[idx_cluster_time],#1
                            row[idx_nav],#2
                            row[idx_obs_time], #3
                            row[idx_ais_length], #4
                            row[idx_ais_width], #5
                            row[idx_ais_vessel_type], #6
                            row[idx_ais_vessel_type_main], #7
                            row[idx_flag_country], #8
                            row[idx_source], #9
                            row[idx_destination], #10
                            [row[idx_lat],row[idx_lng]] 
                        ]
            
            if not math.isnan(row[idx_imo])  and the_imo == -1:
                the_imo = int(row[idx_imo])

            if isFirst  :
                the_previous = the_current
                
            # deteksi kapal berhenti    
            if row[idx_sog] < 1 :
                #kalau belum ada trigger maka buat event baru
                if len(the_trigger)==0:
                    state_value = [state,state]
                    
                    the_trigger.append(row[idx_h3])  #0
                    the_trigger.append(the_mmsi) #1
                    the_trigger.append(the_imo) #2
                    the_trigger.append(row[idx_obs_time]) #3
                    the_trigger.append(row[idx_ais_length]) #4
                    the_trigger.append(row[idx_ais_width]) #5
                    the_trigger.append(row[idx_ais_vessel_type]) #6
                    the_trigger.append(row[idx_ais_vessel_type_main]) #7
                    the_trigger.append(row[idx_flag_country]) #8
                    the_trigger.append(row[idx_source]) #9
                    the_trigger.append(row[idx_destination]) #10

                    the_trigger.append([row[idx_nav]]) # -6 
                    the_trigger.append([row[idx_cluster_time]]) #-5
                    the_trigger.append([row[idx_lat]]) #-4
                    the_trigger.append([row[idx_lng]]) #-3
                    the_trigger.append(state_value) #-2
                    the_trigger.append(1) #-1

                    the_prior = the_previous
                    set_ship.add((the_mmsi,)) #adding tuple to set
                
                # kalau udahm periksa apakah sinyal masih ada di k-depth
                else:
                    # kalau masih tambahkan info koordinat dll
                    if cso_p.cso_h3_adjacency_test(the_current[idx_h3],cso_p.cso_k_ring(the_trigger[idx_h3],the_k_depth)):
                        the_trigger[idx_ref_counts] += 1 # update count
                        the_trigger[idx_ref_state][1] = state # update state_value for end with current state
                        
                        the_trigger[idx_nav_status_list].append(row[idx_nav]) #-6
                        the_trigger[idx_time_list].append(row[idx_cluster_time]) #-5
                        the_trigger[idx_lat_list].append(row[idx_lat]) #-4
                        the_trigger[idx_lng_list].append(row[idx_lng]) #-3
                    
                    #kalau engga hitung waktu time lower dan upper
                    else:
                        t = cso_u.calc_lower_upper_time_estimates(the_prior[idx_cluster_time], 
                                                    the_trigger[-5][0], 
                                                    the_previous[idx_cluster_time],
                                                    the_current[idx_cluster_time],
                                                    time_factor)
                        delta_time_upper = t[0]
                        delta_time_lower = t[1]
                        
                        result = _result_calculator(the_trigger, delta_time_lower, delta_time_upper, 
                                                    min_observations, std_dev_factor,geom_type)
                        the_stopped_ship.append(result)
                        
                        del result
                        #reset
                        the_trigger.clear()
                        the_prior.clear()

            
            else:
                if len(the_trigger) > 0:

                    if cso_p.cso_h3_adjacency_test(the_current[idx_h3],cso_p.cso_k_ring(the_trigger[idx_h3],the_k_depth)):
                        the_trigger[idx_ref_counts] += 1
                        the_trigger[idx_ref_state][1] = state
                        
                        the_trigger[idx_nav_status_list].append(row[idx_nav]) #-6
                        the_trigger[idx_time_list].append(row[idx_cluster_time]) #-5
                        the_trigger[idx_lat_list].append(row[idx_lat]) #-4
                        the_trigger[idx_lng_list].append(row[idx_lng]) #-3
                        
                    else:
                        t = cso_u.calc_lower_upper_time_estimates(the_prior[idx_cluster_time], 
                                                        the_trigger[-5][0],
                                                        the_previous[idx_cluster_time], 
                                                        the_current[idx_cluster_time],
                                                        time_factor)
                        delta_time_upper = t[0]
                        delta_time_lower = t[1]

                        result = _result_calculator(the_trigger, delta_time_lower, delta_time_upper, 
                                                    min_observations, std_dev_factor,geom_type)
                        
                        the_stopped_ship.append(result)

                        del result

                        #reset
                        the_trigger.clear()
                        the_prior.clear()


            the_previous  = the_current   
            
            # jika ini baris terakhir dan event stop masih aktif
            if isLast and len(the_trigger) > 0:
                
                the_trigger[idx_ref_counts] += 1 # update count
                the_trigger[idx_ref_state][1] = state
                
                the_trigger[idx_nav_status_list].append(row[idx_nav]) #-6
                the_trigger[idx_time_list].append(row[idx_cluster_time]) #-5
                the_trigger[idx_lat_list].append(row[idx_lat]) #-4
                the_trigger[idx_lng_list].append(row[idx_lng]) #-3

                t = cso_u.calc_lower_upper_time_estimates(the_prior[idx_cluster_time], 
                                            the_trigger[-5][0], 
                                            the_current[idx_cluster_time],
                                            the_current[idx_cluster_time], 
                                            time_factor)
                delta_time_upper = t[0]
                delta_time_lower = t[1]
    
                result = _result_calculator(the_trigger, delta_time_lower, delta_time_upper, 
                                            min_observations, std_dev_factor,geom_type)

                the_stopped_ship.append(result)

                del result
                the_trigger.clear()
                the_prior.clear()
                the_previous.clear()

                if((the_mmsi,)) not in set_ship:
                    the_nonVisits.append([f"Non visiting obs:{len(g.index)}",
                                        the_mmsi])


        if mmsi_list_count % 100 == 0:
            print(f"Processed {round(mmsi_list_count/mmsi_list_length*100)}%: {mmsi_list_count} of {mmsi_list_length} mmsi ")
        elif mmsi_list_count == mmsi_list_length:
            print(f"Done! Processed: {mmsi_list_count} of {mmsi_list_length} mmsi ")

    print(f"Completed initial stopped ships calcuation")    
    print(f"Ships in domain is: MMSI's is:..{mmsi_list_length}")
    print(f"Stopped Ships data: Lenght is:..{len(the_stopped_ship)}")
    print(f"Insufficent data: Lenght is:....{len(the_insufficent)}")
    print(f"Non vists data: Lenght is:......{len(the_nonVisits)}")

    print(f"spark2pandas process takes time\nOf:{datetime.now()-startProcess}")

    fieldListStoppedShip = the_stopped_ship[-1][-1]
    tempdata  = the_stopped_ship[-1][1]
    #new_func(fieldListStoppedShip, tempdata)
    new_list = []
    for ss in the_stopped_ship:

        new_list.append(ss[1])

        #print(new_list)

    # health warning do not use for short lists
    # https://stackoverflow.com/questions/40517553/pyspark-valueerror-some-of-types-cannot-be-determined-after-inferring
    # error risk: ValueError: Some of types cannot be determined after inferring
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType
    schema = StructType([ 
    StructField('h3_index_int', LongType(),True),
    StructField('mmsi', IntegerType(),True),
    StructField('imo', IntegerType(),True), 
    StructField('length',DoubleType() ,True),
    StructField('width',DoubleType() ,True), 
    StructField('vessel_type', StringType(), True), 
    StructField('vessel_type_main', StringType(), True), 
    StructField('obs_time',TimestampType() , True), 
    StructField('flag_country', StringType(), True), 
    StructField('source', StringType(), True), 
    StructField('destination', StringType(), True), 
    StructField('time_lower', IntegerType(),True), 
    StructField('time_upper', IntegerType(),True), 
    StructField('unix_trigger', IntegerType(),True), 
    StructField('avg_unix', IntegerType(),True), 
    StructField('unix_disarm', IntegerType(),True), 
    StructField('sd_lat',DoubleType() ,True),
    StructField('sd_lng',DoubleType() ,True), 
    StructField('trigger_lat',DoubleType() ,True),
    StructField('trigger_lng',DoubleType() ,True), 
    StructField('avg_lat',DoubleType() ,True),
    StructField('avg_lng',DoubleType() ,True), 
    StructField('disarm_lat',DoubleType() ,True), 
    StructField('disarm_lng',DoubleType() ,True), 
    StructField('mode_nav_status',  StringType(), True),
    StructField('obs_nav_status', IntegerType(), True), 
    StructField('state_initial', IntegerType(), True), 
    StructField('state_final',  IntegerType(), True),
    StructField('obs_count', IntegerType(), True), 
    StructField('is_valid', StringType(), True), 
    StructField('geom_wkt', StringType(), True), 
    StructField('wkt_avg_pt', StringType(), True)
    ])
        

    pd_stopped_ship = pd.DataFrame(new_list, columns=fieldListStoppedShip)
    df_stopped_ship = spark.createDataFrame(pd_stopped_ship,schema=schema )
    df_stopped_ship.head()

    return df_stopped_ship

def new_func(fieldListStoppedShip, tempdata):
    print(tempdata)
    print(fieldListStoppedShip)



def stationary(ais, resolution: int = 10, buffer_radius_m=10000, spark=None) : 
    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")

    aktivitas = algorithm_stationary(ais, resolution, buffer_radius_m)
    print(f"\nAlgoritma selesai")
    return aktivitas

def stationary_in_port(stopped_ships, aoi, resolution=8, spark=None) : 
    if spark is None:
        raise ValueError("spark session harus di-pass secara eksplisit (Spark Connect tidak support getOrCreate)")
    aoi_ais = read_aoi(aoi)

    aoi_ais["h3_ids"] = aoi_ais.to_crs("EPSG:4326")["geometry"].apply(lambda geom: polygon_to_h3(geom, resolution))

    aoi_ais['h3_ids_int'] = aoi_ais['h3_ids'].apply(lambda x: [int(id, 16) for id in x])

    aoi_ais = aoi_ais.explode('h3_ids_int')

    if aoi.lower() == "manual":
        aoi_ais = pd.DataFrame(aoi_ais.drop(columns=[
            'geometry', 'Shape_Leng', 'Shape_Area', 'Nama_1',
            'Ket_1', 'Prov_Name', 'h3_ids'
        ]))
    elif aoi.lower() == "heatmap":
        aoi_ais = pd.DataFrame(aoi_ais.drop(columns=[
            'geometry', 'name', 'prov', 'hirarki_p', 'h3_ids','count'
        ]))
    elif aoi.lower() == "cluster":
        aoi_ais = pd.DataFrame(aoi_ais.drop(columns=[
            'geometry', 'Port', 'h3_ids'
        ]))
    else:
        aoi_ais = pd.DataFrame(aoi_ais.drop(columns=[
            'geometry', 'namobj', 'fcode', 'fcode', 'fcode','prov','kabkot','hirarki_p','objectid','h3_ids'
        ]))

    if aoi.lower() != "manual":
        if 'kode' in aoi_ais.columns:
            aoi_ais = aoi_ais.rename(columns={'kode': 'Kode'})

    aoi_ais = spark.createDataFrame(aoi_ais).repartition(300)

    print(f"\nMengekstrak kapal diam di dalam pelabuhan")
    ais_in_port = smbm_spatial_joint(stopped_ships, aoi_ais)
    ais_in_port = ais_in_port.drop("h3_ids", "h3_ids_int")
    print(f"\nSelesai!")
    return ais_in_port

def recap_stationary(stopped_ships, activity, port_code=None, date_obs=None, month_obs=None, year_obs=None):
    from pyspark.sql.functions import col, count, lit, expr

    if not dict(stopped_ships.dtypes)['obs_time'].startswith('timestamp'):
        stopped_ships = stopped_ships.withColumn('obs_time', col('obs_time').cast('timestamp'))

    if port_code:
        stopped_ships = stopped_ships.filter(col('Kode') == port_code)

    if date_obs:
        stopped_ships = stopped_ships.filter(expr(f"DAY(obs_time) = {date_obs}"))
    if month_obs:
        stopped_ships = stopped_ships.filter(expr(f"MONTH(obs_time) = {month_obs}"))
    if year_obs:
        stopped_ships = stopped_ships.filter(expr(f"YEAR(obs_time) = {year_obs}"))

    from pyspark.sql.functions import to_date

    if activity == "arrival":
        result = stopped_ships.filter(
            (col("state_initial") == 2) & (col("state_final").isin([2, 3]))
        ).withColumn("tanggal", to_date(col("obs_time"))) \
        .groupBy("Kode", "tanggal") \
        .agg(count("*").alias("kedatangan"))

    elif activity == "departed":
        result = stopped_ships.filter(
            (col("state_final") == 2) & (col("state_initial").isin([1, 2]))
        ).withColumn("tanggal", to_date(col("obs_time"))) \
        .groupBy("Kode", "tanggal") \
        .agg(count("*").alias("keberangkatan"))

    else:
        raise ValueError("activity harus 'arrival' atau 'departed'")

    pelabuhan_dict = {
        'CLG': 'Calang',
        'KUA': 'Kuala Langsa',
        'LSW': 'Lhokseumawe/Kreung Geukeh',
        'MLH': 'Malahayati',
        'MEQ': 'Meulaboh',
        'SBG': 'Sabang',
        'SNL': 'Singkil',
        'BLW': 'Belawan',
        'GNS': 'Gunung Sitoli',
        'KTJ': 'Kuala Tanjung',
        'LHA': 'Lahewa',
        'INL': 'Natal/ Sikara-kara',
        'PDD': 'Pangkalan Dodek',
        'PKS': 'Pangkalan Susu',
        'PTE': 'Pulau Tello',
        'SLG': 'Sibolga',
        'SRU': 'Sirombu',
        'TBE': 'Tanjung Beringin',
        'TDA': 'Teluk Dalam',
        'LIG': 'Teluk Leidong',
        'MPG': 'Muara Padang',
        'TBY': 'Teluk Bayur',
        'IBI': 'Bagan Siapi-api',
        'BKI': 'Bengkalis',
        'DUM': 'Dumai',
        'ENO': 'Kuala Enok',
        'MRT': 'Meranti/ Dorak',
        'RCI': 'Rengat/Kuala Cinaku',
        'SLJ': 'Selat Panjang',
        'SUQ': 'Sungai Guntung',
        'TMD': 'Tanjung Medang',
        'TLN': 'Tembilahan',
        'MSK': 'Muara Sabak',
        'TNU': 'Talang Duku',
        'ING': 'Boom Baru/ Palembang',
        'TGP': 'Tanjung Api-Api',
        'BKS': 'Pulau Baai',
        'TAG': 'Kota Agung/ Batu Balai',
        'PNJ': 'Panjang',
        'BLU': 'Belinyu',
        'MUO': 'Muntok',
        'PGX': 'Pangkal Balam',
        'TJQ': 'Tanjung Pandan',
        'BUR': 'Batam',
        'SKJ': 'Sei Kolak Kijang',
        'TJB': 'Tanjung Balai Karimun',
        'TBD': 'Tanjung Batu Kundur',
        'TNJ': 'Tanjung Pinang',
        'IMW': 'Marunda',
        'TPR': 'Tanjung priok',
        'CBN': 'Cirebon',
        'PDR': 'Pangandaran/Bojongsalawe',
        'PTM': 'Patimban',
        'SRG': 'Tanjung Emas',
        'CXP': 'Tanjung Intan',
        'BWB': 'Banyu Wangi/ Boom',
        'GRE': 'Gresik',
        'KAT': 'Kalianget',
        'PAZ': 'Pasuruan',
        'AEA': 'Probolinggo',
        'SUB': 'Tanjung Perak',
        'IBP': 'Banten',
        'BOA': 'Benoa',
        'CEB': 'Celukan Bawang',
        'BAD': 'Badas',
        'BMU': 'Bima',
        'LMR': 'Lembar',
        'ENE': 'Ende',
        'IPI': 'Ippi',
        'KBH': 'Kalabahi',
        'LBO': 'Labuan Bajo',
        'MOF': 'Maumere/ Lorens Say',
        'REO': 'Reo',
        'ISA': 'Seba',
        'TQP': 'Tenau/ Kupang',
        'WGP': 'Waingapu',
        'PNK': 'Pontianak',
        'SNE': 'Sintete',
        'KPE': 'Kuala Pembuang',
        'KUM': 'Kumai',
        'PLB': 'Pangkalan Bun',
        'PMI': 'Pegatan Mendawai',
        'PPS': 'Pulang Pisau',
        'SMQ': 'Sampit',
        'SMA': 'Samuda',
        'SAA': 'Sukamara',
        'BDJ': 'Banjarmasin',
        'SUR': 'Satui',
        'BPN': 'Balikpapan',
        'LTU': 'Lhok Tuan',
        'SGQ': 'Sangatta',
        'SKI': 'Sangkulirang',
        'TLA': 'Tanjung Laut',
        'TRE': 'Tanjung Redeb',
        'TTK': 'Nunukan / Tunon Taka',
        'MLD': 'Tarakan / Malundung',
        'LUK': 'Labuhan Uki',
        'MDC': 'Manado',
        'BGG': 'Banggai',
        'DNA': 'Donggala',
        'LUW': 'Luwuk',
        'PTL': 'Pantoloan',
        'TLI': 'Toli-toli',
        'WNQ': 'Wani',
        'BAE': 'Bajoe',
        'MAK': 'Makassar',
        'ARE': 'Pare-Pare',
        'YAR': 'Selayar/Benteng/Rauf Rahman',
        'BBM': 'Bau-Bau/Murhum',
        'KDI': 'Kendari/Bungkutoko',
        'KOL': 'Kolaka',
        'NGG': 'Anggrek',
        'GTO': 'Gorontalo',
        'MJU': 'Mamuju',
        'AMQ': 'Ambon',
        'NDA': 'Banda Naira',
        'LQA': 'Leksula',
        'NAM': 'Leksula',
        'SXK': 'Saumlaki',
        'TUA': 'Tual',
        'DRA': 'Daruba',
        'LBH': 'Labuha',
        'SIO': 'Soasio/Goto',
        'TEI': 'Ternate/ A. Yani',
        'TBO': 'Tobelo',
        'BIK': 'Biak',
        'DJJ': 'Jayapura',
        'MKQ': 'Merauke',
        'NBX': 'Nabire',
        'FKQ': 'Fak-fak',
        'MKW': 'Manokwari',
        'SOQ': 'Sorong',
        'WSR': 'Wasior',
        'ISG': 'Sapudi',
        'BIT': 'Bitung'}
    
    pelabuhan_expr = "map(" + ", ".join([f'"{k}", "{v}"' for k, v in pelabuhan_dict.items()]) + ")[Kode]"
    result = result.withColumn("nama_pelabuhan", expr(pelabuhan_expr))

    return result

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, expr, to_date, year, month, dayofmonth, percentile_approx

def summarize_time_lower(df: DataFrame, groupby_cols: list) -> DataFrame:
    """
    Menghitung rata-rata dan median dari kolom 'time_lower', dengan groupBy dinamis.

    Parameters:
    ----------
    df : DataFrame
        Data Spark yang memiliki kolom 'time_lower' (dalam satuan detik/menit/jam).
    groupby_cols : list
        Daftar nama kolom untuk pengelompokan, bisa berupa:
        - nama kolom asli ('Kode', 'vessel_type', dst)
        - atau turunan waktu seperti: 'year', 'month', 'day'

    Returns:
    -------
    DataFrame
        Hasil agregasi dengan kolom rata-rata dan median.
    """
    # Buat kolom tambahan jika user ingin groupby waktu
    if 'year' in groupby_cols:
        df = df.withColumn('year', year(col('obs_time')))
    if 'month' in groupby_cols:
        df = df.withColumn('month', month(col('obs_time')))
    if 'day' in groupby_cols:
        df = df.withColumn('day', dayofmonth(col('obs_time')))
    if 'date' in groupby_cols:
        df = df.withColumn('date', to_date(col('obs_time')))

    # Hitung rata-rata dan median (approximate)
    summary_df = df.groupBy(*groupby_cols).agg(
        avg("time_lower").alias("rata_rata_durasi"),
        percentile_approx("time_lower", 0.5, 100).alias("median_durasi")
    )

    return summary_df

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, to_date, avg, percentile_approx

def summarize_time_in_port(ais: DataFrame, groupby_cols: list, method: str) -> DataFrame:
    """
    Menghitung rata-rata dan median dari durasi kapal di pelabuhan, berdasarkan metode dan pengelompokan fleksibel.

    Parameter:
    - df (DataFrame): Spark DataFrame yang mengandung kolom durasi ('time_lower' atau 'time') dan 'obs_time'
    - groupby_cols (list): Kolom-kolom yang digunakan untuk pengelompokan
    - method (str): Pilihan metode: 'boundary' atau 'stationary'

    Return:
    - DataFrame hasil agregasi
    """

    allowed_methods = ['boundary', 'stationary']
    if method not in allowed_methods:
        raise ValueError(f"Parameter 'method' harus salah satu dari {allowed_methods}. "
                         f"'{method}' tidak dikenali.")

    if method == "stationary":
        if 'year' in groupby_cols:
            ais = ais.withColumn('year', year(col('obs_time')))
        if 'month' in groupby_cols:
            ais = ais.withColumn('month', month(col('obs_time')))
        if 'day' in groupby_cols:
            ais = ais.withColumn('day', dayofmonth(col('obs_time')))
        if 'date' in groupby_cols:
            ais = ais.withColumn('date', to_date(col('obs_time')))
        if "time_lower" not in ais.columns:
            raise ValueError("Kolom 'time_lower' tidak ditemukan di DataFrame. "
                             "Gunakan metode 'boundary' jika Anda ingin menggunakan kolom 'time'.")
        result = ais.groupBy(*groupby_cols).agg(
            avg("time_lower").alias("Rata-rata (Jam)"),
            percentile_approx("time_lower", 0.5, 100).alias("Median (Jam)")
        )

    else: 
        if 'year' in groupby_cols:
            ais = ais.withColumn('year', year(col('dt_pos_utc')))
        if 'month' in groupby_cols:
            ais = ais.withColumn('month', month(col('dt_pos_utc')))
        if 'day' in groupby_cols:
            ais = ais.withColumn('day', dayofmonth(col('dt_pos_utc')))
        if 'date' in groupby_cols:
            ais = ais.withColumn('date', to_date(col('dt_pos_utc')))
        
        ais = ais.filter((col("position") == "in") & (col("masuk_pelabuhan") == "masuk"))
        if "time" not in ais.columns:
            raise ValueError("Kolom 'time' tidak ditemukan di DataFrame. "
                             "Gunakan metode 'stationary' jika Anda ingin menggunakan kolom 'time_lower'.")
   
        ais = ais.withColumn("time_hour", col("time") / 3600)
        result = ais.groupBy(*groupby_cols).agg(
            avg("time_hour").alias("Rata-rata (Jam)"),
            percentile_approx("time_hour", 0.5, 120).alias("Median (Jam)")
        ).filter(
            col("Rata-rata (Jam)").isNotNull() & col("Median (Jam)").isNotNull()
        )

    return result

def visualisasi_aoi(
    aoi: gpd.GeoDataFrame,
    kode_pelabuhan: list = None,
    resolution: int = 7
):
    """
    Visualisasikan polygon ke dalam H3 index berdasarkan filter 'Kode' tertentu (bisa banyak).

    Parameter:
    - gdf (GeoDataFrame): GeoDataFrame dengan kolom 'geometry' dan 'Kode'
    - kode_filter (list or None): Daftar nilai Kode untuk divisualisasikan, jika None maka ambil semua
    - resolution (int): Resolusi H3 (default = 7)

    Output:
    - Peta interaktif berbasis folium
    """

    if "Kode" in aoi.columns:
        kode_column = "Kode"
    elif "kode" in aoi.columns:
        kode_column = "kode"
    else:
        raise ValueError("Kolom 'Kode' atau 'kode' tidak ditemukan di GeoDataFrame.")

    # Filter berdasarkan kode_pelabuhan jika diberikan
    if kode_pelabuhan is not None:
        gdf_filtered = aoi[aoi[kode_column].isin(kode_pelabuhan)]
        if gdf_filtered.empty:
            raise ValueError(f"Tidak ditemukan data dengan {kode_column} = {kode_pelabuhan}")
    else:
        gdf_filtered = aoi.copy()

    h3_indexes = set()
    for geom in gdf_filtered.geometry:
        if not geom.is_valid:
            geom = geom.buffer(0)
        if geom.geom_type == "MultiPolygon":
            for poly in geom.geoms:
                h3_indexes.update(h3.polyfill(poly.__geo_interface__, resolution, geo_json_conformant=True))
        else:
            h3_indexes.update(h3.polyfill(geom.__geo_interface__, resolution, geo_json_conformant=True))

    h3_polygons = [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in h3_indexes]
    h3_gdf = gpd.GeoDataFrame(geometry=h3_polygons, crs="EPSG:4326")


    centroid = gdf_filtered.geometry.unary_union.centroid
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=7, tiles='CartoDB positron')

    for _, row in h3_gdf.iterrows():
        folium.GeoJson(
            row.geometry.__geo_interface__,
            style_function=lambda x: {
                'fillColor': '#3186cc',
                'color': 'black',
                'weight': 0.5,
                'fillOpacity': 0.4,
            }
        ).add_to(m)


    for _, row in gdf_filtered.iterrows():
        folium.GeoJson(
            row.geometry.__geo_interface__,
            style_function=lambda x: {
                'fillColor': '#ff7800',
                'color': 'red',
                'weight': 1.2,
                'fillOpacity': 0.2
            }
        ).add_to(m)

    return m
