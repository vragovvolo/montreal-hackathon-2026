# Montreal Hackathon - Quebec Open Data

## Quick Start

1. Import `01_setup_data.py` as a Databricks notebook
2. Run on serverless compute
3. All tables land in `montreal_hackathon.quebec_data`

**Pre-requisite:** Raw data files must be pre-loaded in `/Volumes/montreal_hackathon/quebec_data/raw_data/` before running the notebook.

---

## Datasets

All data sourced from **Statistics Canada** open data initiatives (2020-2025), filtered to **Quebec only**.

### Tabular Datasets (CSV -> Delta Tables)

#### `education_facilities`
**Source:** Open Database of Educational Facilities (ODEF) v3.0.1, December 2024
Quebec primary, secondary, and post-secondary schools (public and private). Includes school names, addresses, grade ranges, ISCED classification levels (pre-K through post-secondary), French immersion indicators (early/middle/late), official language minority school flags, and Census subdivision linkage. Originally compiled from 43 provincial and federal sources covering every province and territory.

#### `healthcare_facilities`
**Source:** Open Database of Healthcare Facilities (ODHF) v1.1, August 2020
Quebec hospitals, ambulatory health care services, and nursing/residential care facilities. Classified by NAICS sectors 621-623. Includes facility names, addresses, geocoded lat/lon, and Census subdivision linkage. Deduplicated using ML-based record linkage (Python Record Linkage Toolkit + Scikit-Learn). 7,033 records nationally before Quebec filtering.

#### `cultural_art_facilities`
**Source:** Open Database of Cultural and Art Facilities (ODCAF) v1.0, October 2020
Quebec museums, galleries, theatres, libraries, heritage sites, arts centres, and festival sites. Nine facility type categories based on NAICS sectors 711-712. Includes addresses, geocoded coordinates, and data provider attribution. Fuzzy matching (FuzzyWuzzy) was used to remove 2,435 duplicates nationally. ~8,000 records before Quebec filtering.

#### `recreation_sport_facilities`
**Source:** Open Database of Recreational and Sport Facilities (ODRSF) v1.0, September 2021
Quebec parks, arenas, pools, trails, playgrounds, community centres, sports fields, stadiums, rinks, splash pads, skate parks, and more. Eighteen facility type categories. The largest dataset (~182,000 records nationally from 452 data sources). Note: coverage is not comprehensive — e.g., only ~25% of Canada's golf courses and ~60% of rinks are represented.

### Geospatial Datasets (GeoPackage -> Delta Tables)

All geospatial datasets are reprojected from their original CRS (typically EPSG:3347 StatsCan Lambert) to WGS84 (EPSG:4326). Geometry is stored as WKT strings with centroid lat/lon extracted.

#### `bridges_tunnels`
**Source:** Open Database of Infrastructure (ODI) v2.0, November 2024
Quebec bridges, tunnels, and culverts. Part of a larger infrastructure database (~2.7 million records nationally covering airports, railways, water systems, telecom, and more). Each record includes the feature name, sub-type classification, data provider, CCPI classification, and Census subdivision. 82 data providers nationally spanning municipalities, provinces, and Natural Resources Canada.

#### `cycling_network`
**Source:** Canadian Cycling Network Database, January 2025
Quebec cycling infrastructure classified under the Canadian Bicycle Infrastructure Classification System (Can-BICS). Eight infrastructure types across three comfort-safety levels: high comfort (cycle tracks, local street bikeways, bike paths), medium (multi-use paths), and low (painted bike lanes). Also includes non-conforming types (gravel trails, shared roadways). 18,700 km nationally across 75 municipalities. Surface type and lane width available for some segments.

#### `pedestrian_network`
**Source:** Canadian Pedestrian Network Database, March 2025
Quebec sidewalks, pedestrian paths, multi-use paths, unpaved paths, crosswalks, pedestrian zones, bridges/underpasses, and stairways. Eight infrastructure types with width (in meters) and surface material (paved, gravel, wood, natural) where available. Data collected November 2023 - February 2024 from 55 municipalities nationally. Classification validated via Google Street View.

#### `transit_stops` and `transit_routes`
**Source:** Canadian Public Transit Network Database, January 2025
Quebec transit stop locations and route geometries from 139 GTFS feeds nationally. The stops layer contains geographic coordinates; the routes/shapes layer contains simplified vehicle path geometries (250m tolerance). Data validated using MobilityData Canonical GTFS Schedule Validator.

### GTFS Transit Feeds (ZIP -> Delta Tables)

#### `transit_stm_stops`, `transit_stm_routes`, `transit_stm_trips`, `transit_stm_stop_times`
**Source:** Société de transport de Montréal (STM) GTFS feed
Complete Montreal transit data: stop locations, route definitions, trip schedules, and arrival/departure times at every stop. Standard GTFS format.

#### `transit_stl_stops`, `transit_stl_routes`
**Source:** Société de transport de Laval (STL) GTFS feed
Laval transit stop locations and route definitions.

---

## Reference Documents (Volume)

Located in `/Volumes/montreal_hackathon/quebec_data/reference_docs/`

### Dataset Metadata PDFs

| File | Description |
|------|-------------|
| `education_facilities_metadata.pdf` | ODEF v3 specification: ISCED classification system, grade range mapping by province, French immersion methodology, data collection and geocoding procedures |
| `healthcare_facilities_metadata.pdf` | ODHF v1.1 specification: NAICS-based facility classification, ML deduplication methodology, geocoding via OpenStreetMap Nominatim, data source inventory by province |
| `cultural_art_facilities_metadata.pdf` | ODCAF v1.0 specification: nine facility type categories, address parsing (libpostal), fuzzy duplicate removal, NAICS sector 711/712 mapping |
| `recreation_sport_facilities_metadata.pdf` | ODRSF v1.0 specification: eighteen facility categories, coverage limitations, 452 data source inventory |
| `odi_bridges_tunnels_metadata.pdf` | ODI v2.0 specification: infrastructure classification (CCPI), 12 infrastructure categories, deduplication methodology, data provider inventory |
| `cycling_network_metadata.pdf` | Can-BICS classification system, 317 municipal bikeway types mapped to 8 standard categories, Google Street View validation methodology, municipality coverage summary |
| `pedestrian_network_metadata.pdf` | Pedestrian infrastructure classification, width standardization, surface material taxonomy, data collection timeline |
| `public_transit_metadata.pdf` | GTFS feed validation, 139 agency inventory, stops/shapes layer construction, data quality notes |

### Montreal & Quebec Policy Documents

| File | Description |
|------|-------------|
| `montreal_urban_plan_pum2050.pdf` | Plan d'urbanisme et de mobilité 2050 — Montreal's master city plan covering zoning, land use, densification, mobility corridors, and development guidelines (French, 24 MB) |
| `montreal_urban_plan_pum2050_english.pdf` | PUM 2050 "Projet de Ville" overview — English version of the city vision document (30 MB) |
| `stm_annual_report_2024.pdf` | STM Rapport annuel 2024 — Montreal transit ridership (314.6M trips, +9% YoY), route performance, fleet expansion, and financial summary (French, 7.4 MB) |
| `stm_financial_report_2024.pdf` | STM Financial Report 2024 — Detailed financials for Société de transport de Montréal (French, 656 KB) |
| `quebec_infrastructure_plan_pqi_2026_2036.pdf` | Plan québécois des infrastructures 2026-2036 — Provincial 10-year capital investment plan for roads, bridges, transit, and public buildings (French, 4.5 MB) |
| `montreal_plan_velo_2019.pdf` | Plan vélo 2019 — Montreal's comprehensive cycling infrastructure strategy with network maps, priorities, and investment targets (French, 9.3 MB) |
| `montreal_vision_velo_2023_2027_projects.pdf` | Vision vélo 2023-2027 — Map and project list for planned cycling network expansion including the REV express bike network (French, 300 KB) |
| `montreal_census_sociodemographic_profile.pdf` | Profil sociodémographique — Montreal CMA demographics, income, commute patterns, and housing from the 2021 Census (French, 848 KB) |
| `quebec_health_services_annual_report_2024_2025.pdf` | MSSS Rapport annuel de gestion 2024-2025 — Quebec Ministry of Health annual management report covering health network structure, service delivery, and performance indicators (French, 2.7 MB) |

### Supporting CSVs

Data source inventories, record layouts, and classification dictionaries for each dataset are also included in the volume for additional reference.
