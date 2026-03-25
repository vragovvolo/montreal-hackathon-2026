# Databricks notebook source
# MAGIC %md
# MAGIC # Montreal Hackathon - Quebec Open Data Setup
# MAGIC
# MAGIC **Run this notebook once to load all hackathon datasets into Unity Catalog.**
# MAGIC
# MAGIC Compatible with **serverless compute**.
# MAGIC
# MAGIC **Pre-requisite:** Raw data files must already be in `/Volumes/montreal_hackathon/quebec_data/raw_data/`.
# MAGIC PDFs must already be in `/Volumes/montreal_hackathon/quebec_data/reference_docs/`.
# MAGIC
# MAGIC ### Tables Created
# MAGIC | Table | Source | Description |
# MAGIC |-------|--------|-------------|
# MAGIC | `education_facilities` | ODEF v3.0.1 | Schools & post-secondary |
# MAGIC | `healthcare_facilities` | ODHF v1.1 | Hospitals, clinics, care facilities |
# MAGIC | `cultural_art_facilities` | ODCAF v1.0 | Museums, galleries, theatres |
# MAGIC | `recreation_sport_facilities` | ODRSF v1.0 | Parks, arenas, pools |
# MAGIC | `bridges_tunnels` | ODI v2 | Bridge & tunnel infrastructure |
# MAGIC | `cycling_network` | Can Cycle Network 2024 | Cycling infrastructure segments |
# MAGIC | `pedestrian_network` | Ped Network 2025 | Sidewalks, paths, crosswalks |
# MAGIC | `transit_stops` | Public Transit 2025 | Transit stop locations |
# MAGIC | `transit_routes` | Public Transit 2025 | Transit route geometries |
# MAGIC | `transit_stm_*` | STM GTFS | Montreal transit (stops, routes, trips, stop_times) |
# MAGIC | `transit_stl_*` | STL GTFS | Laval transit (stops, routes) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

CATALOG = "montreal_hackathon"
SCHEMA = "quebec_data"
RAW_VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
REF_VOL = f"/Volumes/{CATALOG}/{SCHEMA}/reference_docs"

# Quebec filter values
QC_CODES = ["QC", "Qc", "qc", "Quebec", "Québec", "quebec", "québec", "24"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# MAGIC %pip install geopandas fiona pyproj "numpy<2" -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Re-declare config after Python restart
import re, os, shutil, zipfile
import pandas as pd
import geopandas as gpd
from pyspark.sql import functions as F

CATALOG = "montreal_hackathon"
SCHEMA = "quebec_data"
RAW_VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
REF_VOL = f"/Volumes/{CATALOG}/{SCHEMA}/reference_docs"
QC_CODES = ["QC", "Qc", "qc", "Quebec", "Québec", "quebec", "québec", "24"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify Data Files

# COMMAND ----------

print("Raw data files:")
for f in sorted(os.listdir(RAW_VOL)):
    size = os.path.getsize(os.path.join(RAW_VOL, f)) / 1024 / 1024
    print(f"  {f:45s} {size:>8.1f} MB")

print(f"\nReference docs:")
for f in sorted(os.listdir(REF_VOL)):
    size = os.path.getsize(os.path.join(REF_VOL, f)) / 1024
    print(f"  {f:55s} {size:>8.0f} KB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper Functions

# COMMAND ----------

def clean_column_name(name: str) -> str:
    """Convert column name to clean snake_case."""
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    name = re.sub(r'_+', '_', name)
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
    candidates = [c for c in df.columns if any(kw in c.lower() for kw in ['prov', 'province'])]
    return candidates[0] if candidates else None


def filter_quebec(df, prov_col=None):
    """Filter a Spark DataFrame to Quebec rows."""
    if prov_col is None:
        prov_col = find_province_column(df)
    if prov_col is None:
        print("  Warning: No province column found - loading all rows")
        return df
    filtered = df.filter(F.col(prov_col).isin(QC_CODES))
    total = df.count()
    kept = filtered.count()
    print(f"  Filtered {prov_col}: {kept}/{total} rows are Quebec")
    return filtered


def save_table(df, table_name):
    """Save a Spark DataFrame as a managed Delta table."""
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    count = spark.table(full_name).count()
    print(f"  Saved {full_name} ({count} rows)")


def load_csv(filename, table_name, prov_col_hint=None):
    """Load CSV from volume, filter to Quebec, clean columns, save as Delta."""
    print(f"\n{'='*60}")
    print(f"Loading {table_name}")
    print(f"{'='*60}")
    path = f"{RAW_VOL}/{filename}"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df = clean_columns(df)
    prov_col = prov_col_hint or find_province_column(df)
    df = filter_quebec(df, prov_col)
    save_table(df, table_name)


def load_gpkg(filename, table_name, layer=None, prov_col_hint=None):
    """Load GeoPackage from volume, filter to Quebec, save as Delta."""
    print(f"\n{'='*60}")
    print(f"Loading {table_name}" + (f" (layer: {layer})" if layer else ""))
    print(f"{'='*60}")
    path = f"{RAW_VOL}/{filename}"
    gdf = gpd.read_file(path, layer=layer)
    # Reproject to WGS84 and extract lat/lon
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        try:
            gdf_wgs84 = gdf.to_crs(epsg=4326)
            gdf["longitude"] = gdf_wgs84.geometry.centroid.x
            gdf["latitude"] = gdf_wgs84.geometry.centroid.y
        except Exception as e:
            print(f"  Warning: Could not reproject: {e}")
    elif "geometry" in gdf.columns:
        gdf["longitude"] = gdf.geometry.centroid.x
        gdf["latitude"] = gdf.geometry.centroid.y
    gdf["geometry_wkt"] = gdf.geometry.apply(lambda g: g.wkt if g else None)
    gdf = gdf.drop(columns=["geometry"])
    df = spark.createDataFrame(pd.DataFrame(gdf))
    df = clean_columns(df)
    prov_col = prov_col_hint or find_province_column(df)
    df = filter_quebec(df, prov_col)
    save_table(df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load CSV Datasets (Facilities)

# COMMAND ----------

load_csv("education_facilities.csv", "education_facilities", prov_col_hint="province_code")

# COMMAND ----------

load_csv("healthcare_facilities.csv", "healthcare_facilities", prov_col_hint="province")

# COMMAND ----------

load_csv("cultural_art_facilities.csv", "cultural_art_facilities", prov_col_hint="prov_terr")

# COMMAND ----------

load_csv("recreation_sport_facilities.csv", "recreation_sport_facilities", prov_col_hint="prov_terr")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load GeoPackage Datasets (Infrastructure & Networks)

# COMMAND ----------

load_gpkg("bridges_tunnels.gpkg", "bridges_tunnels", prov_col_hint="prov_terr")

# COMMAND ----------

load_gpkg("cycling_network.gpkg", "cycling_network", prov_col_hint="provinceterritory")

# COMMAND ----------

load_gpkg("pedestrian_network.gpkg", "pedestrian_network", prov_col_hint="prov_terr")

# COMMAND ----------

# Transit Stops & Routes (from unified GPKG)
import fiona

transit_path = f"{RAW_VOL}/transit_stops_routes.gpkg"
if os.path.exists(transit_path) and os.path.getsize(transit_path) > 0:
    layers = fiona.listlayers(transit_path)
    print(f"GPKG layers: {layers}")
    for layer in layers:
        if "stop" in layer.lower():
            load_gpkg("transit_stops_routes.gpkg", "transit_stops", layer=layer, prov_col_hint="prov_terr")
        elif "shape" in layer.lower() or "route" in layer.lower():
            load_gpkg("transit_stops_routes.gpkg", "transit_routes", layer=layer, prov_col_hint="prov_terr")
else:
    print("Transit GPKG not available - skipping transit_stops and transit_routes tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Load GTFS Data (Montreal & Laval Transit)

# COMMAND ----------

def load_gtfs(zip_filename, agency_prefix, agency_name):
    """Load GTFS tables from a zip in the volume."""
    zip_path = f"{RAW_VOL}/{zip_filename}"
    if not os.path.exists(zip_path) or os.path.getsize(zip_path) == 0:
        print(f"GTFS file {zip_filename} not available - skipping {agency_name}")
        return
    print(f"\n{'='*60}")
    print(f"Loading GTFS: {agency_name}")
    print(f"{'='*60}")
    extract_dir = f"/tmp/gtfs_{agency_prefix}"
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_dir)
    for gtfs_file, tbl in [("stops", f"transit_{agency_prefix}_stops"),
                            ("routes", f"transit_{agency_prefix}_routes"),
                            ("trips", f"transit_{agency_prefix}_trips"),
                            ("stop_times", f"transit_{agency_prefix}_stop_times")]:
        txt = os.path.join(extract_dir, f"{gtfs_file}.txt")
        if os.path.exists(txt):
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file:{txt}")
            df = clean_columns(df)
            df = df.withColumn("agency", F.lit(agency_name))
            save_table(df, tbl)
        else:
            print(f"  {gtfs_file}.txt not found - skipping")

load_gtfs("gtfs_stm.zip", "stm", "STM Montreal")

# COMMAND ----------

load_gtfs("gtfs_stl.zip", "stl", "STL Laval")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print("=" * 70)
print(f"  MONTREAL HACKATHON DATA SETUP COMPLETE")
print(f"  Catalog: {CATALOG}")
print(f"  Schema:  {CATALOG}.{SCHEMA}")
print("=" * 70)

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
print(f"\nTables ({len(tables)}):")
for t in sorted(tables, key=lambda x: x.tableName):
    count = spark.table(f"{CATALOG}.{SCHEMA}.{t.tableName}").count()
    print(f"  - {t.tableName:40s} {count:>8,} rows")

print(f"\nReference docs ({REF_VOL}):")
for f in sorted(os.listdir(REF_VOL)):
    size = os.path.getsize(os.path.join(REF_VOL, f)) / 1024
    print(f"  - {f:55s} {size:>6.0f} KB")

print(f"""
{'=' * 70}
  NEXT STEPS:

  Knowledge Assistant: Build RAG over PDFs in {REF_VOL}
  Genie Space: Point at {CATALOG}.{SCHEMA} for natural language SQL
  Multi-Agent Supervisor: Domain agents for education, health, culture,
    recreation, transit, infrastructure — orchestrated by a supervisor
{'=' * 70}
""")
