# Montreal Hackathon - March 2026

## Quick Start

1. Import `01_setup_data.py` as a Databricks notebook
2. Attach to a cluster with DBR 14.3+ (or any recent serverless runtime)
3. Run All — takes ~10 minutes
4. All tables land in `montreal_hackathon.quebec_data`

## What Gets Created

**Catalog:** `montreal_hackathon`
**Schema:** `montreal_hackathon.quebec_data`

### Tables (all filtered to Quebec)

| Table | Description | Use Case |
|-------|-------------|----------|
| `education_facilities` | Schools, post-secondary institutions | Genie, MAS |
| `healthcare_facilities` | Hospitals, clinics, care homes | Genie, MAS |
| `cultural_art_facilities` | Museums, galleries, theatres | Genie, MAS |
| `recreation_sport_facilities` | Parks, arenas, pools, trails | Genie, MAS |
| `bridges_tunnels` | Bridge & tunnel infrastructure | Genie, MAS |
| `cycling_network` | Cycling paths & lanes | Genie, MAS |
| `pedestrian_network` | Sidewalks, paths, crosswalks | Genie, MAS |
| `transit_stops` | Province-wide transit stops | Genie, MAS |
| `transit_routes` | Province-wide transit routes | Genie, MAS |
| `transit_stm_stops` | Montreal STM stops | Genie, MAS |
| `transit_stm_routes` | Montreal STM routes | Genie, MAS |
| `transit_stm_trips` | Montreal STM trips | Genie, MAS |
| `transit_stm_stop_times` | Montreal STM schedules | Genie, MAS |
| `transit_stl_stops` | Laval STL stops | Genie, MAS |
| `transit_stl_routes` | Laval STL routes | Genie, MAS |

### Volume (for Knowledge Assistant / RAG)

`/Volumes/montreal_hackathon/quebec_data/reference_docs/`

Contains 8 metadata PDFs + supporting CSVs describing every dataset in detail.

## Hackathon Tracks

### Track 1: Knowledge Assistant (KA)
Build a RAG-powered assistant using the PDFs and supporting docs in the volume. Participants should be able to ask questions about data sources, methodology, and coverage.

### Track 2: Genie Space
Create a Genie Space over `montreal_hackathon.quebec_data` for natural language SQL exploration. Example queries:
- "How many schools in Montreal offer French immersion?"
- "Show me all hospitals within 5km of a transit stop"
- "What types of recreational facilities are most common?"

### Track 3: Multi-Agent Supervisor
Build domain-specific agents (education, health, culture, recreation, transit, infrastructure) and a supervisor that routes queries to the right specialist. Cross-domain queries like "Find schools near transit stops with nearby parks" require coordination.

## Data Sources

All data sourced from Statistics Canada open data initiatives (2020-2025). See metadata PDFs in the volume for full provenance and methodology.
