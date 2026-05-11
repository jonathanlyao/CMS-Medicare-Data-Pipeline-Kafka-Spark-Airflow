# CMS Medicare Data Pipeline — Kafka, Spark, Airflow

A near-real-time streaming pipeline that pulls Medicare hospital charge data from the CMS public API, processes it through Kafka and Spark Structured Streaming, and surfaces DRG-level cost anomalies in Snowflake — fully orchestrated by Airflow.

---

## Architecture
<img width="806" height="722" alt="image" src="https://github.com/user-attachments/assets/576d1419-bf69-4771-b2c3-3fe8d37f49d3" />

```
CMS Public API (data.cms.gov)
        │
        ▼
  Kafka Producer          ← cms_producer.py: pulls 145k rows via REST, publishes
        │                    each row as a message keyed by DRG code
        ▼
  Kafka Broker            ← 3 partitions, key = DRG_Cd (same DRG → same partition)
        │
        ▼
  Spark Structured        ← reads from Kafka, casts fields, aggregates by DRG:
  Streaming               │  avg submitted charge, avg Medicare payment,
        │                 │  charge ratio (charge / Medicare payment)
        │                 └─ writes every 30s via foreachBatch → Snowflake
        ▼
  Snowflake               ← STREAMING.DRG_CHARGE_ANALYSIS (raw streaming output)
        │
        ▼
  dbt (3-layer model)
  ├── staging             ← stg_drg_charge_analysis: clean + rename fields
  ├── intermediate        ← int_drg_outlier_flagged: 2-sigma outlier flag,
  │                          charge_severity tier (Critical/High/Medium/Normal)
  └── mart                ← mart_drg_spending_quality: final analytical table,
                             ranked by charge ratio within severity tier
        │
        ▼
  dbt test                ← 9 tests: not_null, unique, accepted_values
        │
        ▼
  Airflow DAG             ← daily schedule 06:00 UTC
  run_cms_producer >> run_spark_streaming >> run_dbt_models >> run_dbt_tests
```

---

## What This Pipeline Does

Medicare hospital charge data has a well-known anomaly: hospitals submit charges that can be 10–20x what Medicare actually pays. This pipeline makes that visible at scale.

The core metric is `avg_charge_ratio = avg_submitted_charge / avg_medicare_payment`. A ratio of 11 means the hospital's billed amount is 11 times Medicare's reimbursement for the same DRG. This pipeline:

1. Identifies which DRGs have the highest charge ratios across all participating hospitals
2. Flags statistical outliers (>2 standard deviations above the mean)
3. Assigns severity tiers: `Critical` (≥15x), `High` (≥10x), `Medium` (≥5x), `Normal`
4. Ranks DRGs within each severity tier

**Sample output from `mart_drg_spending_quality`:**

| DRG | Description | Hospitals | Avg charge | Avg Medicare payment | Charge ratio | Severity |
|-----|-------------|-----------|------------|----------------------|--------------|----------|
| 006 | Liver transplant without MCC | 2 | $384,902 | $27,621 | 21.0x | Critical |
| 658 | Kidney/ureter procedures without CC/MCC | 1 | $189,300 | $9,015 | 21.0x | Critical |
| 459 | Spinal fusion except cervical with MCC | 2 | $524,181 | $46,022 | 11.2x | High |
| 275 | Cardiac defibrillator implant with cath and MCC | 1 | $542,649 | $47,854 | 11.3x | High |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data source | CMS public API (`data.cms.gov/data-api/v1`) |
| Message queue | Apache Kafka 7.4 (Confluent) |
| Stream processing | Apache Spark 3.5.1 — Structured Streaming |
| Data warehouse | Snowflake |
| Transformation | dbt 1.11 with `dbt-snowflake` adapter |
| Orchestration | Apache Airflow 2.10.4 |
| Infrastructure | Docker + Docker Compose |
| Language | Python 3.11 |

---

## Project Structure

```
CMS_project/
├── docker-compose.yml          # Kafka (3 listeners) + Spark containers
├── cms_producer.py             # CMS API → Kafka producer
├── spark-apps/
│   ├── cms_streaming.py        # Spark Structured Streaming job
│   └── snowflake_writer.py     # foreachBatch sink: aggregated results → Snowflake
└── cms_dbt/
    ├── dbt_project.yml
    ├── models/
    │   ├── staging/
    │   │   ├── sources.yml
    │   │   ├── schema.yml
    │   │   └── stg_drg_charge_analysis.sql
    │   ├── intermediate/
    │   │   └── int_drg_outlier_flagged.sql
    │   └── marts/
    │       └── mart_drg_spending_quality.sql
    └── ...

DOT_project/airflow/
├── docker-compose.yml          # Airflow (webserver + scheduler + postgres)
├── Dockerfile                  # Extends apache/airflow:2.10.4-python3.11
├── dags/
│   └── cms_medicare_pipeline.py
└── dbt_profiles/
    └── profiles.yml            # Snowflake connection for dbt inside Airflow
```

---

## Key Design Decisions

**Kafka partition key = DRG code**
All records for the same DRG code hash to the same partition. This means Spark consumers reading a partition see all records for a given DRG together, avoiding cross-partition shuffles during aggregation.

**Three Kafka listeners**
Kafka is configured with three separate listeners: `INTERNAL` (port 29092) for same-Compose-project containers like Spark, `EXTERNAL` (port 9092) for the host machine, and `DOCKER` (port 39092) for cross-project containers like Airflow. This avoids `NoBrokersAvailable` errors when Airflow connects to a Kafka broker running in a different Docker Compose project.

**`foreachBatch` over native Snowflake connector**
Spark's native Snowflake connector requires JDBC JARs and has version compatibility issues. `foreachBatch` with `snowflake-connector-python` keeps the dependency footprint small and is straightforward to debug.

**Streaming truncate-and-reload**
Each Spark micro-batch (every 30 seconds) truncates and rewrites `DRG_CHARGE_ANALYSIS`. This is intentional for this use case: the aggregation is a full-dataset summary, not an incremental append. For time-series accumulation, an append or merge strategy would be more appropriate.

**dbt `table` materialization for the mart**
The staging and intermediate layers are `view` (lightweight, always fresh). The mart is materialized as a `table` so downstream queries don't re-compute the aggregation chain on every read.

---

## Setup

### Prerequisites

- Docker Desktop
- Python 3.11 with `kafka-python-ng`, `requests`, `snowflake-connector-python`
- Snowflake account with `CMS_MEDICARE` database and `CMS_MEDICARE_WH` warehouse
- dbt with `dbt-snowflake` adapter

### 1. Start Kafka and Spark

```bash
cd CMS_project
docker-compose up -d
```

Verify all three containers are running:

```bash
docker ps
# cms_project-kafka-1, cms_project-zookeeper-1, cms_project-spark-1
```

### 2. Create the Kafka topic

```bash
docker exec cms_project-kafka-1 kafka-topics \
  --create --topic cms-inpatient-stream \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### 3. Set up Snowflake

Run this in Snowflake:

```sql
CREATE DATABASE IF NOT EXISTS CMS_MEDICARE;
CREATE WAREHOUSE IF NOT EXISTS CMS_MEDICARE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE DATABASE CMS_MEDICARE;
CREATE SCHEMA IF NOT EXISTS STREAMING;

CREATE OR REPLACE TABLE STREAMING.DRG_CHARGE_ANALYSIS (
    DRG_Cd              STRING,
    DRG_Desc            STRING,
    hospital_count      INT,
    avg_charge          FLOAT,
    avg_medicare_pymt   FLOAT,
    avg_total_pymt      FLOAT,
    avg_charge_ratio    FLOAT,
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 4. Run the pipeline manually

```bash
# Step 1: push CMS data into Kafka
python cms_producer.py

# Step 2: run Spark Structured Streaming (processes and writes to Snowflake)
docker exec cms_project-spark-1 \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/cms_streaming.py

# Step 3: run dbt models
cd cms_dbt
dbt run
dbt test
```

### 5. Start Airflow (for scheduled runs)

```bash
cd DOT_project/airflow
docker-compose up -d
```

Open `http://localhost:8080` (admin / admin), enable the `cms_medicare_pipeline` DAG, and trigger it manually or let it run on its daily schedule.

---

## CMS Data

All data is sourced from the CMS public API — no API key required.

| Dataset | CMS GUID | Rows | Refresh |
|---------|----------|------|---------|
| Medicare Inpatient by Provider & Service | `690ddc6c-...` | ~145,000 | Annual |
| Medicare Inpatient by Provider | `ee6fb1a5-...` | ~3,000 | Annual |
| Medicare Part D Spending by Drug | `7e0b4365-...` | ~14,000 | Annual |

The pipeline uses `inpatient_by_provider_service` as its primary source. The DRG-level granularity (hospital × DRG code) is what enables the charge ratio analysis.

---

## dbt Tests

```
9 tests, 0 failures

not_null:  drg_code, avg_submitted_charge, avg_medicare_payment,
           avg_charge_ratio (staging)
           drg_code, charge_severity (mart)

unique:    drg_code (staging), drg_code (mart)

accepted_values: charge_severity in [Critical, High, Medium, Normal]
```

---

## Related Posts

- [Why My Spark Container Keeps Exiting — Docker PID 1 and the Daemon Trap](https://dev.to/lee_yao_cfeb14fb9b141b8c5/why-my-spark-container-keeps-exiting-docker-pid-1-and-the-daemon-trap-dgf)
- [Debugging a Multi-Container Airflow Pipeline — Kafka Network Isolation and the YAML Indentation Trap](https://dev.to/lee_yao_cfeb14fb9b141b8c5/debugging-a-multi-container-airflow-pipeline-kafka-network-isolation-and-the-yaml-indentation-trap-5595)
