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
PREV_ACCUM   = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-detail-{ACCUM_START}-{end_accum_str}.parquet"
OUT_ACCUM     = f"{SAVE_PATH}data-ais-indonesia-by-mmsi-detail-{ACCUM_START}-{end_str}.parquet"

# Hasil analisis
OUT_PORT_TRAFFIC  = f"{SAVE_PATH}hasil/port_traffic_{ACCUM_START}_{end_str}.parquet"
OUT_PORT_CALL     = f"{SAVE_PATH}hasil/port_call_{ACCUM_START}_{end_str}.parquet"
OUT_TIME_TRAVEL   = f"{SAVE_PATH}hasil/time_travel_hormuz_{ACCUM_START}_{end_str}.parquet"

# Referensi statis
# REF_PORT      = f"{SAVE_PATH}ref/port_indonesia_manual.parquet"

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

# SQL Server BPS
SQL_SERVER   = os.environ.get("BPS_SQLSERVER_HOST", "NOVA.ms.bps.go.id")
SQL_DATABASE = os.environ.get("BPS_SQLSERVER_DB",   "sd_web_scraping")
SQL_USERNAME = os.environ.get("BPS_SQLSERVER_USER")
SQL_PASSWORD = os.environ.get("BPS_SQLSERVER_PASS")
