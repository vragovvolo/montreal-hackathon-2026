# Databricks notebook source
# MAGIC %md
# MAGIC # Montreal Hackathon - Quebec Open Data Setup
# MAGIC
# MAGIC **Run this notebook once to load all hackathon datasets into Unity Catalog.**
# MAGIC
# MAGIC This notebook will:
# MAGIC 1. Create catalog `montreal_hackathon`, schema `quebec_data`, and a volume for reference PDFs
# MAGIC 2. Clone the data repo from GitHub (with Git LFS)
# MAGIC 3. Load 8 datasets filtered to **Quebec only**, with clean column names
# MAGIC 4. Upload metadata PDFs to a Unity Catalog Volume
# MAGIC
# MAGIC ### Tables Created
# MAGIC | Table | Source | Description |
# MAGIC |-------|--------|-------------|
# MAGIC | `education_facilities` | ODEF v3.0.1 | Schools & post-secondary (~QC subset of 19K) |
# MAGIC | `healthcare_facilities` | ODHF v1.1 | Hospitals, clinics, care facilities (~QC subset of 7K) |
# MAGIC | `cultural_art_facilities` | ODCAF v1.0 | Museums, galleries, theatres (~QC subset of 8K) |
# MAGIC | `recreation_sport_facilities` | ODRSF v1.0 | Parks, arenas, pools (~QC subset of 182K) |
# MAGIC | `bridges_tunnels` | ODI v2 | Bridge & tunnel infrastructure |
# MAGIC | `cycling_network` | Can Cycle Network 2024 | Cycling infrastructure segments |
# MAGIC | `pedestrian_network` | Ped Network 2025 | Sidewalks, paths, crosswalks |
# MAGIC | `transit_stops` | Public Transit 2025 | Transit stop locations |
# MAGIC | `transit_routes` | Public Transit 2025 | Transit route geometries |
# MAGIC | `transit_stm_stops` | STM GTFS | Montreal transit stops |
# MAGIC | `transit_stm_routes` | STM GTFS | Montreal transit routes |
# MAGIC | `transit_stm_trips` | STM GTFS | Montreal transit trips |
# MAGIC | `transit_stm_stop_times` | STM GTFS | Montreal stop arrival/departure times |
# MAGIC | `transit_stl_stops` | STL GTFS | Laval transit stops |
# MAGIC | `transit_stl_routes` | STL GTFS | Laval transit routes |
# MAGIC
# MAGIC ### Volume
# MAGIC `montreal_hackathon.quebec_data.reference_docs` — contains:
# MAGIC - 8 dataset metadata PDFs (methodology, schemas, data quality)
# MAGIC - 8 Montreal building plan PDFs (floorplans & brochures for 628, TDC2, TDC3, L'Avenue, Terra, YUL)
# MAGIC - Supporting CSVs (data sources, record layouts, classification dictionaries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

CATALOG = "montreal_hackathon"
SCHEMA = "quebec_data"
VOLUME = "reference_docs"
REPO_URL = "https://github.com/DuaAdit/databricks_hackathon.git"
CLONE_DIR = "/tmp/hackathon_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Quebec filter values
QC_CODES = {"QC", "Qc", "qc", "Quebec", "Québec", "quebec", "québec", "24"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies & Clone Repo

# COMMAND ----------

# MAGIC %pip install geopandas fiona pyproj -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Re-declare config after Python restart
CATALOG = "montreal_hackathon"
SCHEMA = "quebec_data"
VOLUME = "reference_docs"
REPO_URL = "https://github.com/DuaAdit/databricks_hackathon.git"
CLONE_DIR = "/tmp/hackathon_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
QC_CODES = {"QC", "Qc", "qc", "Quebec", "Québec", "quebec", "québec", "24"}

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install -y git-lfs > /dev/null 2>&1
# MAGIC rm -rf /tmp/hackathon_data
# MAGIC git clone --branch main https://github.com/DuaAdit/databricks_hackathon.git /tmp/hackathon_data
# MAGIC cd /tmp/hackathon_data && git lfs pull

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Catalog, Schema & Volume

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"✓ Created {CATALOG}.{SCHEMA} and volume {VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper Functions

# COMMAND ----------

import re
import os
import shutil
import pandas as pd
import geopandas as gpd
from pyspark.sql import functions as F
from pyspark.sql.types import *

def clean_column_name(name: str) -> str:
    """Convert column name to clean snake_case."""
    # Handle common camelCase patterns
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
    # Replace non-alphanumeric with underscore
    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    # Collapse multiple underscores
    name = re.sub(r'_+', '_', name)
    # Strip leading/trailing underscores, lowercase
    return name.strip('_').lower()


def clean_columns(df):
    """Rename all columns in a Spark DataFrame to clean snake_case."""
    for col in df.columns:
        new_name = clean_column_name(col)
        if new_name != col:
            df = df.withColumnRenamed(col, new_name)
    return df


def find_province_column(df):
    """Find the column most likely to contain province codes."""
    candidates = [c for c in df.columns if any(
        kw in c.lower() for kw in ['prov', 'province']
    )]
    return candidates[0] if candidates else None


def filter_quebec(df, prov_col=None):
    """Filter a Spark DataFrame to Quebec rows."""
    if prov_col is None:
        prov_col = find_province_column(df)
    if prov_col is None:
        print("  ⚠ No province column found — loading all rows")
        return df
    qc_filter = F.col(prov_col).isin(list(QC_CODES))
    filtered = df.filter(qc_filter)
    total = df.count()
    kept = filtered.count()
    print(f"  Filtered {prov_col}: {kept}/{total} rows are Quebec")
    return filtered


def save_table(df, table_name):
    """Save a Spark DataFrame as a managed Delta table."""
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    count = spark.table(full_name).count()
    print(f"  ✓ Saved {full_name} ({count} rows)")


def load_csv_dataset(file_path, table_name, prov_col_hint=None):
    """Load a CSV, filter to Quebec, clean columns, save as Delta."""
    print(f"\n{'='*60}")
    print(f"Loading {table_name} from {os.path.basename(file_path)}")
    print(f"{'='*60}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:{file_path}")
    df = clean_columns(df)
    prov_col = prov_col_hint if prov_col_hint else find_province_column(df)
    df = filter_quebec(df, prov_col)
    save_table(df, table_name)
    return df


def load_gpkg_dataset(file_path, table_name, layer=None, prov_col_hint=None):
    """Load a GeoPackage layer, filter to Quebec, convert geometry to WKT, save as Delta."""
    print(f"\n{'='*60}")
    print(f"Loading {table_name} from {os.path.basename(file_path)}" + (f" (layer: {layer})" if layer else ""))
    print(f"{'='*60}")
    gdf = gpd.read_file(file_path, layer=layer)
    # Convert geometry to WKT string and reproject to WGS84 for lat/lon
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        try:
            gdf_wgs84 = gdf.to_crs(epsg=4326)
            gdf["longitude"] = gdf_wgs84.geometry.centroid.x
            gdf["latitude"] = gdf_wgs84.geometry.centroid.y
        except Exception:
            pass
    elif "geometry" in gdf.columns:
        gdf["longitude"] = gdf.geometry.centroid.x
        gdf["latitude"] = gdf.geometry.centroid.y
    gdf["geometry_wkt"] = gdf.geometry.apply(lambda g: g.wkt if g else None)
    gdf = gdf.drop(columns=["geometry"])
    pdf = pd.DataFrame(gdf)
    df = spark.createDataFrame(pdf)
    df = clean_columns(df)
    prov_col = prov_col_hint if prov_col_hint else find_province_column(df)
    df = filter_quebec(df, prov_col)
    save_table(df, table_name)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load CSV Datasets (Facilities)

# COMMAND ----------

# Education Facilities (ODEF)
load_csv_dataset(
    f"{CLONE_DIR}/ODEF/odef_v3_0_1.csv",
    "education_facilities",
    prov_col_hint="province_code"
)

# COMMAND ----------

# Healthcare Facilities (ODHF)
load_csv_dataset(
    f"{CLONE_DIR}/ODHF/odhf_v1.1.csv",
    "healthcare_facilities",
    prov_col_hint="province"
)

# COMMAND ----------

# Cultural & Art Facilities (ODCAF)
load_csv_dataset(
    f"{CLONE_DIR}/ODCAF/ODCAF_v1.0.csv",
    "cultural_art_facilities",
    prov_col_hint="prov_terr"
)

# COMMAND ----------

# Recreation & Sport Facilities (ODRSF)
load_csv_dataset(
    f"{CLONE_DIR}/ODRSF/ODRSF_v1.0.csv",
    "recreation_sport_facilities",
    prov_col_hint="prov_terr"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load GeoPackage Datasets (Infrastructure & Networks)

# COMMAND ----------

# Bridges & Tunnels
load_gpkg_dataset(
    f"{CLONE_DIR}/Bridges & Tunnels/odi_bridges_tunnels.gpkg",
    "bridges_tunnels",
    prov_col_hint="prov_terr"
)

# COMMAND ----------

# Cycling Network
load_gpkg_dataset(
    f"{CLONE_DIR}/Cycling Network/cycle_network_2024.gpkg",
    "cycling_network",
    prov_col_hint="provinceterritory"
)

# COMMAND ----------

# Pedestrian Network
load_gpkg_dataset(
    f"{CLONE_DIR}/Pedestrain Network/pedestrian_network.gpkg",
    "pedestrian_network",
    prov_col_hint="prov_terr"
)

# COMMAND ----------

# Transit Stops & Routes (from unified GPKG)
import fiona

gpkg_path = f"{CLONE_DIR}/Public transport/stops_and_routes.gpkg"
layers = fiona.listlayers(gpkg_path)
print(f"GPKG layers: {layers}")

for layer in layers:
    if "stop" in layer.lower():
        load_gpkg_dataset(gpkg_path, "transit_stops", layer=layer, prov_col_hint="prov_terr")
    elif "shape" in layer.lower() or "route" in layer.lower():
        load_gpkg_dataset(gpkg_path, "transit_routes", layer=layer, prov_col_hint="prov_terr")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Load GTFS Data (Montreal & Laval Transit)

# COMMAND ----------

import zipfile

def load_gtfs_feed(zip_path, agency_prefix, agency_name):
    """Extract and load key GTFS tables from a zip file."""
    print(f"\n{'='*60}")
    print(f"Loading GTFS feed: {agency_name}")
    print(f"{'='*60}")
    extract_dir = f"/tmp/gtfs_{agency_prefix}"
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_dir)

    gtfs_tables = {
        "stops": f"transit_{agency_prefix}_stops",
        "routes": f"transit_{agency_prefix}_routes",
        "trips": f"transit_{agency_prefix}_trips",
        "stop_times": f"transit_{agency_prefix}_stop_times",
    }

    for gtfs_file, table_name in gtfs_tables.items():
        txt_path = os.path.join(extract_dir, f"{gtfs_file}.txt")
        if os.path.exists(txt_path):
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:{txt_path}")
            df = clean_columns(df)
            df = df.withColumn("agency", F.lit(agency_name))
            save_table(df, table_name)
        else:
            print(f"  ⚠ {gtfs_file}.txt not found — skipping")

# STM - Société de transport de Montréal
load_gtfs_feed(
    f"{CLONE_DIR}/Public transport/societe_transport_montreal/gtfs.zip",
    "stm",
    "Société de transport de Montréal"
)

# COMMAND ----------

# STL - Société de transport de Laval
load_gtfs_feed(
    f"{CLONE_DIR}/Public transport/societe_transport_laval/gtfs.zip",
    "stl",
    "Société de transport de Laval"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Upload PDFs to Volume (for Knowledge Assistant / RAG)

# COMMAND ----------

pdf_files = [
    # Dataset metadata
    ("Bridges & Tunnels/ODI v2 Metadata.pdf", "odi_bridges_tunnels_metadata.pdf"),
    ("Cycling Network/Metadata_Report_Canadian_Cycle_Network.pdf", "cycling_network_metadata.pdf"),
    ("ODCAF/ODCAF_Metadata_v1.0.pdf", "cultural_art_facilities_metadata.pdf"),
    ("ODEF/ODEF v3 Metadata.pdf", "education_facilities_metadata.pdf"),
    ("ODHF/ODHF_metadata_v1.1.pdf", "healthcare_facilities_metadata.pdf"),
    ("ODRSF/ODRSF_Metadata_v1.0.pdf", "recreation_sport_facilities_metadata.pdf"),
    ("Pedestrain Network/Metadata_Report_Pedestrian.pdf", "pedestrian_network_metadata.pdf"),
    ("Public transport/Metadata_Report_Canadian_Public_Transit_Network.pdf", "public_transit_metadata.pdf"),
    # Montreal building plans (floorplans & brochures)
    ("Building Plans/5a3b0d81e54aed000114d574_628_floorplans.pdf", "building_628_floorplans.pdf"),
    ("Building Plans/5a3b0db3e54aed000114d580_TDC2-Signature-Suites-Floor-Plans.pdf", "building_tdc2_signature_suites_floorplans.pdf"),
    ("Building Plans/5a3b0de838a27e000192d57c_TDC3-Floorplans.pdf", "building_tdc3_floorplans.pdf"),
    ("Building Plans/5a3b0e913b892f0001d632be_TDC3-Podium-floorpans.pdf", "building_tdc3_podium_floorplans.pdf"),
    ("Building Plans/5a3b0eb5c81e0c0001fbc465_Lavenue_elevation_penthouse withFP-Brochure.pdf", "building_lavenue_penthouse_brochure.pdf"),
    ("Building Plans/5a3b0edf3b892f0001d632ca_lavenue_brochure_EN.pdf", "building_lavenue_brochure.pdf"),
    ("Building Plans/5a3b0f1f049ac1000113c2a2_Terra_floor_plan.pdf", "building_terra_floorplan.pdf"),
    ("Building Plans/5a3b3319e59b0a0001ab1f8c_yul.pdf", "building_yul_floorplan.pdf"),
]

print(f"Uploading PDFs to {VOLUME_PATH}/\n")
for src_rel, dest_name in pdf_files:
    src = os.path.join(CLONE_DIR, src_rel)
    dest = os.path.join(VOLUME_PATH, dest_name)
    if os.path.exists(src):
        shutil.copy2(src, dest)
        size_kb = os.path.getsize(src) / 1024
        print(f"  ✓ {dest_name} ({size_kb:.0f} KB)")
    else:
        print(f"  ⚠ Not found: {src_rel}")

# COMMAND ----------

# Also upload supporting CSVs (data sources, record layouts) for additional KA context
support_files = [
    ("ODEF/record_layout.csv", "education_record_layout.csv"),
    ("ODEF/source_list.csv", "education_source_list.csv"),
    ("ODHF/odhf_v1.1.csv", None),  # skip - already a table
    ("ODCAF/Data_Sources.csv", "cultural_art_data_sources.csv"),
    ("ODRSF/Data_Sources.csv", "recreation_sport_data_sources.csv"),
    ("Cycling Network/classification_dictionary.csv", "cycling_classification_dictionary.csv"),
    ("Cycling Network/column_descriptions.csv", "cycling_column_descriptions.csv"),
    ("Cycling Network/data_sources.csv", "cycling_data_sources.csv"),
    ("Pedestrain Network/column_descriptions.csv", "pedestrian_column_descriptions.csv"),
    ("Pedestrain Network/data_sources.csv", "pedestrian_data_sources.csv"),
    ("Public transport/data_sources.csv", "transit_data_sources.csv"),
    ("Public transport/validation_summary.csv", "transit_validation_summary.csv"),
    ("Bridges & Tunnels/data_providers_bridges_tunnels.csv", "bridges_data_providers.csv"),
    ("Bridges & Tunnels/record_layout_bridges_tunnels.csv", "bridges_record_layout.csv"),
]

print(f"\nUploading supporting CSVs to {VOLUME_PATH}/\n")
for src_rel, dest_name in support_files:
    if dest_name is None:
        continue
    src = os.path.join(CLONE_DIR, src_rel)
    dest = os.path.join(VOLUME_PATH, dest_name)
    if os.path.exists(src):
        shutil.copy2(src, dest)
        print(f"  ✓ {dest_name}")
    else:
        print(f"  ⚠ Not found: {src_rel}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary

# COMMAND ----------

print("=" * 70)
print(f"  MONTREAL HACKATHON DATA SETUP COMPLETE")
print(f"  Catalog: {CATALOG}")
print(f"  Schema:  {CATALOG}.{SCHEMA}")
print(f"  Volume:  {VOLUME_PATH}")
print("=" * 70)

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
print(f"\n📊 Tables ({len(tables)}):")
for t in tables:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{t.tableName}").count()
    print(f"  • {t.tableName:40s} {count:>8,} rows")

print(f"\n📄 Volume files:")
for f in os.listdir(VOLUME_PATH):
    size = os.path.getsize(os.path.join(VOLUME_PATH, f)) / 1024
    print(f"  • {f:50s} {size:>8.0f} KB")

print(f"""
{'=' * 70}
  NEXT STEPS FOR PARTICIPANTS:

  🤖 Knowledge Assistant: Use PDFs in {VOLUME_PATH}
     to build a RAG-powered Q&A bot about Quebec infrastructure data.

  📊 Genie Space: Point a Genie Space at {CATALOG}.{SCHEMA}
     for natural language SQL exploration of Quebec facilities & transit.

  🧠 Multi-Agent Supervisor: Build specialized agents for each domain
     (education, health, culture, recreation, transit, infrastructure)
     and orchestrate them with a supervisor agent.
{'=' * 70}
""")
