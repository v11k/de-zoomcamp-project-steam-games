# Steam Games Data Pipeline

A batch data engineering project that ingests Steam game metadata and review summaries, stores raw data in Google Cloud Storage, loads it into BigQuery, transforms it with dbt, and visualizes insights in a dashboard.

---

## Problem description

The Steam platform contains a massive and constantly changing catalog of games, but the raw API data is not structured for analysis.

This project solves the following:

- Collect Steam game catalog data daily
- Enrich new games with detailed metadata
- Retrieve review summary metrics (total, positive, negative reviews)
- Store raw data in a cloud data lake (GCS)
- Load and structure the data in a cloud data warehouse (BigQuery)
- Transform raw data into analytics-ready tables using dbt
- Visualize trends such as genres, releases, and reviews

### Key challenge

Steam’s API endpoints for game details and reviews are rate-limited, making full refreshes inefficient.

### Solution

This pipeline uses an incremental append-only approach:

- Full catalog is fetched daily
- Details and reviews are fetched only for new appids
- Previously processed appids are skipped
- Failed requests are retried in future runs

This allows the pipeline to scale while respecting API limits.

---

## Architecture

Steam API  
→ ingest_game_list.py → steam_catalog_YYYY-MM-DD.jsonl  
→ ingest_game_details.py (incremental) → steam_appdetails_YYYY-MM-DD.jsonl  
→ ingest_game_reviews.py (incremental) → steam_reviews_YYYY-MM-DD.jsonl  
→ upload_raw_to_gcs.py → Google Cloud Storage  
→ gcs_to_bq.py → BigQuery (raw tables)  
→ dbt → marts → dashboard  

---

## Technologies

- Python
- uv (environment management)
- Google Cloud Storage
- BigQuery
- dbt
- cron (scheduling)
- JSONL (raw storage)

---

## Data ingestion

### Steam catalog

Endpoint:
IStoreService/GetAppList

Output:
data/raw/steam_catalog/steam_catalog_YYYY-MM-DD.jsonl

Contains:
- appid
- name
- last_modified
- price_change_number
- raw payload

---

### Steam app details

Endpoint:
/api/appdetails

Output:
data/raw/steam_appdetails/steam_appdetails_YYYY-MM-DD.jsonl

Fields include:
- name
- type
- developers
- publishers
- genres
- price
- release date
- metacritic
- recommendations

---

### Steam review summaries

Endpoint:
/appreviews/{appid}

Output:
data/raw/steam_reviews/steam_reviews_YYYY-MM-DD.jsonl

Fields include:
- total_reviews
- good_reviews
- bad_reviews
- review_score
- review_score_desc

---

## Incremental logic

This is the most important design decision.

### Behavior

- Catalog is fetched fully every day
- Details and reviews:
  - scan all historical outputs
  - only fetch appids not processed before
- Failed appids are retried later

### Example

Day 1: 160000 apps processed  
Day 2: 160200 apps → only 200 new processed  

### Tradeoff

- Efficient and scalable
- Old data is not automatically refreshed

---

## Cloud

### Google Cloud Storage (data lake)

Stores raw JSONL files:

data/raw/steam_catalog/  
data/raw/steam_appdetails/  
data/raw/steam_reviews/  

---

### BigQuery (data warehouse)

Tables:
- raw_steam.steam_catalog
- raw_steam.steam_appdetails
- raw_steam.steam_reviews

---

## Data warehouse design

### Partitioning

All tables are partitioned by ingestion date.

### Clustering

All tables are clustered by appid.

### Why this works

- Queries filter by date → partitioning reduces scan cost
- Joins use appid → clustering improves performance

---

## Transformations (dbt)

### Layers

Staging:
- clean raw data
- extract JSON fields
- standardize schema

Intermediate:
- flatten arrays (genres, categories)
- normalize structure

Marts:
- game-level dataset
- genre distribution
- release trends
- review metrics

---

## Dashboard

At least 2 tiles:

- Genre distribution
- Release trend or review analysis

Optional:
- free vs paid games
- average price by genre
- review score distribution

---

## Workflow orchestration

This is a batch pipeline executed with cron.

### Execution order

ingest_game_list.py  
→ ingest_game_details.py + ingest_game_reviews.py  
→ upload_raw_to_gcs.py  
→ gcs_to_bq.py  
→ dbt build  

### Why cron

- simple
- reliable
- sufficient for batch pipelines
- avoids unnecessary Airflow complexity

---

## Cron scheduling

Example file:
infra/cron/crontab.txt

Install:

crontab infra/cron/crontab.txt

Example entry:

0 2 * * * cd /path/to/project && /path/to/project/.venv/bin/python scripts/ingest_game_list.py --date $(date -u +\%F)

Meaning:
- runs daily at 02:00 UTC
- uses project virtual environment
- passes current date

---

## Repository structure

.
├── data/
├── dbt/
├── infra/cron/
├── scripts/
├── .env.example
├── pyproject.toml
└── README.md

---

## Reproducibility

### 1. Clone repo

git clone <repo-url>  
cd <repo>  

---

### 2. Setup ingestion environment

uv venv  
source .venv/bin/activate  
uv sync  

---

### 3. Setup dbt

python -m venv dbt_venv  
source dbt_venv/bin/activate  
pip install dbt-bigquery  

---

### 4. Configure environment

cp .env.example .env  

Fill:
- Steam API key
- GCP credentials
- BigQuery config

---

## Run pipeline manually

python scripts/ingest_game_list.py --date 2026-03-28  
python scripts/ingest_game_details.py --date 2026-03-28  
python scripts/ingest_game_reviews.py --date 2026-03-28  
python scripts/upload_raw_to_gcs.py --date 2026-03-28  
python scripts/gcs_to_bq.py --date 2026-03-28  

cd dbt  
dbt build --profiles-dir .  

---

## Important BigQuery note

BigQuery load jobs do not apply default column values.

Therefore:
- ingestion timestamps must be added in Python
- or handled after load with SQL

---

## Limitations

- no automatic refresh for old games
- initial backfill is slow due to API limits
- infrastructure is manually configured (no IaC)
- cron does not enforce strict dependencies

---

## Future improvements

- add Terraform
- implement refresh logic
- add dbt tests
- add monitoring
- expand dashboard

---

## Evaluation criteria mapping

Problem description:
Clear explanation of the data problem and solution.

Cloud:
Uses Google Cloud Storage and BigQuery.

Batch pipeline:
End-to-end ingestion → storage → warehouse → transformation.

Data warehouse:
Partitioned and clustered tables with justification.

Transformations:
Implemented with dbt.

Dashboard:
Includes multiple analytical tiles.

Reproducibility:
Complete setup and execution instructions provided.

---

## Author

Data Engineering Zoomcamp Project