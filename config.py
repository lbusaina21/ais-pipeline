"""
config.py
Konfigurasi terpusat untuk pipeline AIS Indonesia.
Semua script pipeline import dari sini.

Environment variables yang bisa di-set di init_pipeline.sh:
  PIPELINE_START_DATE    : format YYYY-MM-DD (default: 7 hari lalu)
  PIPELINE_END_DATE      : format YYYY-MM-DD (default: hari ini)
  PIPELINE_ACCUM_START   : format ddmonyyyy  (default: sama dengan start_str)
  PIPELINE_PREV_ACCUM_PATH: path S3 file akumulatif sebelumnya (default: kosong)
"""

import os
from datetime import datetime, timedelta

# ── Periode data ──────────────────────────────────────────────────────────────

END_ACCUM_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_END_DATE",
                   (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"))
)
START_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_START_DATE",
                   (datetime.now() - timedelta(days=9)).strftime("%Y-%m-%d"))
)
END_DATE = datetime.fromisoformat(
    os.environ.get("PIPELINE_END_DATE",
                   (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"))
)

end_accum_str   = END_ACCUM_DATE.strftime("%d%b%Y").lower()
start_str = START_DATE.strftime("%d%b%Y").lower()
end_str   = END_DATE.strftime("%d%b%Y").lower()

# ── S3 paths ──────────────────────────────────────────────────────────────────

working_dir = os.environ["AWS_WORKING_DIRECTORY_PATH"]
SAVE_PATH   = f"s3a://{working_dir}iran_usa_conflict/"

# Input/output tiap stage
IN_RAW     = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-{start_str}-{end_str}.parquet"
OUT_DETAIL = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-detail-{start_str}-{end_str}.parquet"

# Akumulatif
ACCUM_START   = os.environ.get("PIPELINE_ACCUM_START", start_str)
PREV_ACCUM    = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-detail-{ACCUM_START}-{end_accum_str}.parquet"
OUT_ACCUM     = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-detail-{ACCUM_START}-{end_str}.parquet"

# Hasil analisis
OUT_TRAFFIC_ALL         = f"{SAVE_PATH}hasil/port_traffic_all_{ACCUM_START}_{end_str}.parquet"
OUT_TRAFFIC_INBOUND     = f"{SAVE_PATH}hasil/port_traffic_inbound_{ACCUM_START}_{end_str}.parquet"
OUT_TRAFFIC_OUTBOUND    = f"{SAVE_PATH}hasil/port_traffic_outbound_{ACCUM_START}_{end_str}.parquet"
OUT_HORMUZ_TAG          = f"{SAVE_PATH}hasil/tagging-vessel-hormuz-{ACCUM_START}-{end_str}.parquet"
OUT_VESSEL_HORMUZ       = f"{SAVE_PATH}hasil/vessel_through_hormuz_{ACCUM_START}_{end_str}.parquet"
OUT_VESSEL_NO_HORMUZ    = f"{SAVE_PATH}hasil/vessel_not_through_hormuz_{ACCUM_START}_{end_str}.parquet"
OUT_ARRIVAL_RECAP       = f"{SAVE_PATH}hasil/arrival_recap_hormuz_{ACCUM_START}_{end_str}.parquet"
OUT_TIME_TRAVEL         = f"{SAVE_PATH}hasil/time-travel-hormuz-to-indonesia-{start_str}-{end_str}.parquet"

# Referensi statis
REF_EEZ_LAND       = f"/vsicurl/https://github.com/nandyarz/ais/raw/main/land-eez/EEZ_Land_v3_202030.shp"
REF_PORT_INDO      = f"{SAVE_PATH}port_indonesia_manual.parquet"
REF_PORT_LN        = f"{SAVE_PATH}port_ln.parquet"

# ── Konstanta ─────────────────────────────────────────────────────────────────

H3_RESOLUTION_EEZ  = 5    # untuk polyfill EEZ Indonesia
H3_RESOLUTION_PORT = 8    # untuk join dengan AOI port

# Manual fix vessel_type
VESSEL_TYPE_FIXES = [
    ("PERTAMINA GAS DAHLIA",  "Gas Tanker",             "Lpg Tanker"),
    ("PERTAMINA PRIDE",       "Oil And Chemical Tanker", "Crude Oil Tanker"),
    ("MARINA AMAN",           "Oil And Chemical Tanker", "Crude Oil Tanker"),
    ("SC OCEAN LXI",          "Oil And Chemical Tanker", "Crude Oil Tanker"),
    ("MBS BULELENG",          "Bulk Carrier",            None),
    ("SUCCESS FORTUNE XL",    "Oil And Chemical Tanker", "Crude Oil Tanker"),
]

# Kolom output final preprocessing
SELECT_COLS = [
    "mmsi", "imo", "vessel_name",
    "vessel_type", "vessel_type_main", "vessel_type_sub",
    "flag_country",
    "RegisteredOwnerCountryofDomicile",
    "GroupBeneficialOwnerCountryOfRegistration",
    "GroupBeneficialOwnerCountryofDomicile",
    "length", "LengthOverallLOA", "width", "BreadthExtreme", "Depth", "draught",
    "GrossTonnage", "NetTonnage", "Deadweight", "LightDisplacementTonnage",
    "nav_status", "heading", "sog",
    "dt_pos_utc", "latitude", "longitude", "H3_int_index_8",
]

# Polygon Hormuz
REF_POLYGON_HORMUZ = [
    [54.7, 24.9], [54.7, 26.0], [54.9, 26.6],
    [55.5, 27.0], [56.5, 27.2], [56.5, 26.5],
    [56.1, 26.1], [55.7, 25.7], [54.7, 24.9],
]

# Missing Column
MISSING_COLS = [
    "imo", "length", "width", "LengthOverallLOA",
    "BreadthExtreme", "GrossTonnage", "NetTonnage", "Deadweight",
    "LightDisplacementTonnage", "Depth", "dt_pos_utc",
]

# SQL Server BPS
SQL_SERVER   = os.environ.get("BPS_SQLSERVER_HOST", "NOVA.ms.bps.go.id")
SQL_DATABASE = os.environ.get("BPS_SQLSERVER_DB",   "sd_web_scraping")
SQL_USERNAME = os.environ.get("BPS_SQLSERVER_USER")
SQL_PASSWORD = os.environ.get("BPS_SQLSERVER_PASS")
