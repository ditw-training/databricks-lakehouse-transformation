# Databricks Lakehouse & Transformation

**Two-day advanced Databricks training** — production-grade data pipelines with Lakeflow, Delta optimization, and enterprise governance.

Business scenario: fictional company **RetailHub** — continuing from Databricks Fundamental, now professionalizing the data platform.

---

## Table of Contents

- [About the Training](#about-the-training)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Training Schedule](#training-schedule)
- [Repository Structure](#repository-structure)
- [Datasets](#datasets)
- [Additional Materials](#additional-materials)

---

## About the Training

| | |
|---|---|
| **Duration** | 2 days (13:00–17:00 each day, ~3h net content per day) |
| **Level** | Intermediate (post-Fundamental) |
| **Notebook language** | Python (PySpark) + SQL |
| **Environment** | Databricks on Azure (Unity Catalog + Lakeflow Pipelines) |
| **Scenario** | RetailHub — customers, orders, products |
| **Format** | ~30% presentation · ~40% live demo · ~30% hands-on workshops |

### Learning Objectives

After completing Day 1 (Data Processing & Transformation Pipelines), participants will be able to:

- Design professional data flows across Bronze, Silver, and Gold layers with clear separation of concerns
- Compare batch vs streaming ingestion and choose the right pattern for each use case
- Use **COPY INTO** for idempotent batch loading and **Auto Loader** for scalable file ingestion
- Configure Auto Loader in batch-oriented mode using `trigger(availableNow=True)`
- Build declarative pipelines with **Lakeflow Declarative Pipelines** (formerly Delta Live Tables) — `STREAMING TABLE` and `MATERIALIZED VIEW`
- Implement data quality gates using **Expectations** (`CONSTRAINT EXPECT ... ON VIOLATION`)
- Use **AUTO CDC** (`APPLY CHANGES INTO`) for automatic SCD Type 1/2 processing
- Create and monitor multi-task **Lakeflow Jobs** with dependencies, scheduling, and repair runs
- Understand **Serverless compute** and **Lakeflow Connect** for modern ingestion patterns

After completing Day 2 (Lakehouse Architecture & Optimization), participants will be able to:

- Use **Change Data Feed** (CDF) to track row-level changes in Delta tables
- Use **RESTORE** for disaster recovery and **Deletion Vectors** for fast DML operations
- Apply **OPTIMIZE**, **VACUUM**, **Z-ORDER**, and **Liquid Clustering** for table maintenance
- Leverage **Predictive Optimization** for automated maintenance (GA 2025)
- Analyze query plans with `EXPLAIN` and identify performance bottlenecks
- Understand the small file problem and apply compaction strategies
- Evaluate compute and storage cost trade-offs including **Serverless compute**
- Configure **Unity Catalog** governance: GRANT/REVOKE, row-level security, column masking
- Use **Delta Sharing** for cross-organization data sharing
- Navigate data lineage and audit logs for compliance

---

## Prerequisites

- **Completed Databricks Fundamental training** (or equivalent knowledge of):
  - Databricks workspace navigation (notebooks, clusters, SQL Warehouse)
  - Reading CSV/JSON files with `spark.read`
  - Basic PySpark transformations (`withColumn`, `filter`, `groupBy`)
  - Delta Lake fundamentals (ACID, Time Travel, MERGE, Managed vs External tables)
  - Basic Databricks Workflows and Unity Catalog concepts
- Access to a Databricks environment with **Unity Catalog** and **Lakeflow Pipelines** enabled

---

## Quick Start

### 1. Environment setup (trainer)

```
notebooks/setup/00_pre_config.ipynb
```

Run once before the training — creates Unity Catalog catalogs for all participants.

### 2. Participant setup

```
notebooks/setup/00_setup.ipynb
```

Run at the beginning of the session — validates the catalog, schemas, and Volume paths.

### 3. Environment verification

```python
username = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")
catalog  = f"retailhub_{username}"
spark.sql(f"USE CATALOG {catalog}")
print(f"Working catalog: {catalog}")
```

Expected: catalog `retailhub_<your_username>` with schemas: `bronze`, `silver`, `gold`.

---

## Training Schedule

### Day 1 — Data Processing & Transformation Pipelines

| Block | Type | Topics |
|-------|------|--------|
| **01 Medallion Architecture** | Demo | Bronze-Silver-Gold recap in pipeline context, data flow design, modularity (load → transform → save) |
| **02 Batch & Stream Ingestion** | Demo | CTAS, COPY INTO, Auto Loader (cloudFiles), schema evolution, `availableNow` trigger, error handling |
| **01 Workshop: Ingestion** | Hands-on | COPY INTO, Auto Loader batch mode, schema evolution, DESCRIBE DETAIL metrics |
| **03 Lakeflow Pipelines** | Demo | STREAMING TABLE vs MATERIALIZED VIEW, Expectations, Quarantine pattern, `expect_all`, AUTO CDC (SCD 1/2) |
| **02 Workshop: Lakeflow** | Hands-on | Bronze/Silver/Gold declarations, expectations, pipeline creation in UI, event_log queries |
| **04 Orchestration** | Demo | Multi-task Jobs, CRON scheduling, retry policies, repair runs |

### Day 2 — Lakehouse Architecture & Optimization

| Block | Type | Topics |
|-------|------|--------|
| **01 Delta Lake Advanced** | Demo | Change Data Feed (CDF), advanced MERGE patterns, RESTORE, Shallow/Deep Clone |
| **02 Optimization & Maintenance** | Demo | OPTIMIZE, VACUUM, Z-ORDER, Liquid Clustering, Predictive Optimization, EXPLAIN, small file problem |
| **01 Workshop: Optimization** | Hands-on | File compaction, Z-ORDER, Liquid Clustering, query plan analysis |
| **03 Cost Management** | Demo | Compute vs storage costs, optimization impact on costs, Bronze-Silver-Gold cost model |
| **04 Security & Governance** | Demo | Unity Catalog governance, GRANT/REVOKE, row-level security, column masking, Delta Sharing, lineage |
| **02 Workshop: Security** | Hands-on | Access control, row filters, column masks, audit queries |

---

## Repository Structure

```
Databricks Lakehouse & Transformation/
│
├── notebooks/
│   ├── Day1/                                    # Day 1: Data Processing & Pipelines
│   │   ├── Demo/
│   │   │   ├── 01_medallion_architecture_demo.ipynb   # Medallion recap, data flow design
│   │   │   ├── 02_batch_stream_ingestion_demo.ipynb   # COPY INTO, Auto Loader, streaming
│   │   │   ├── 03_lakeflow_pipelines_demo.ipynb       # Lakeflow: ST, MV, Expectations, CDC
│   │   │   └── 04_orchestration_demo.ipynb            # Jobs, scheduling, repair runs
│   │   └── Workshop/
│   │       ├── 01_batch_stream_ingestion_workshop.ipynb  # Hands-on ingestion exercises
│   │       └── 02_lakeflow_pipelines_workshop.ipynb      # Hands-on Lakeflow pipeline building
│   │
│   ├── Day2/                                    # Day 2: Architecture & Optimization
│   │   ├── Demo/
│   │   │   ├── 01_delta_advanced_demo.ipynb           # CDF, advanced MERGE, SCD patterns
│   │   │   ├── 02_optimization_demo.ipynb             # OPTIMIZE, VACUUM, Z-ORDER, Liquid Clustering
│   │   │   ├── 03_cost_management_demo.ipynb          # Compute/storage costs, cost architecture
│   │   │   └── 04_security_governance_demo.ipynb      # UC, GRANT/REVOKE, RLS, column masking
│   │   └── Workshop/
│   │       ├── 01_optimization_workshop.ipynb         # Hands-on optimization exercises
│   │       └── 02_security_governance_workshop.ipynb   # Hands-on security exercises
│   │
│   └── setup/
│       ├── 00_pre_config.ipynb                  # Trainer: create catalogs/schemas
│       └── 00_setup.ipynb                       # Participant: validate environment
│
├── materials/
│   ├── lakeflow/                                # Lakeflow pipeline SQL/Python examples
│   │   ├── lakeflow_demo/                       # Educational Bronze→Silver→Gold pipeline
│   │   ├── lakeflow_bonus/                      # Production-ready patterns (SCD, star schema)
│   │   └── lakeflow_ny_demo/                    # Python DSL showcase
│   │
│   └── orchestration/                           # Workflow task scripts
│       ├── task_01_validate.py                  # Validation gate
│       ├── task_02_transform.py                 # Bronze→Silver transform
│       ├── task_03_report.py                    # Gold aggregation report
│       ├── file_arrival_trigger_demo.py         # File-based trigger example
│       └── table_update_trigger_demo.py         # Table update trigger example
│
├── dataset/                                     # RetailHub scenario data
│   ├── customers/                               # Customer master data (CSV)
│   ├── orders/                                  # Orders batch (JSON) + stream files
│   ├── products/                                # Product catalog (CSV + Parquet)
│   └── workshop/                                # AdventureWorks Lite (star schema demos)
│
├── assets/images/                               # Diagrams and screenshots
│
└── docs/                                        # Reference materials (PDF)
    └── Day1/
        ├── day1_cheatsheet.pdf                  # Day 1 quick reference
        └── day1_quiz.pdf                        # Day 1 knowledge check (15 questions)
```

---

## Datasets

All data belongs to the fictional company **RetailHub** — continuing the scenario from Databricks Fundamental.

| File | Format | Records | Description |
|------|--------|---------|-------------|
| `dataset/customers/customers.csv` | CSV | 10,000 | Customer master data (ID, name, email, segment) |
| `dataset/customers/customers_new.csv` | CSV | ~100 | New/updated customers — for MERGE and CDC demos |
| `dataset/orders/orders_batch.json` | JSON | 50,000 | Historical orders (batch format) |
| `dataset/orders/stream/*.json` | JSON | Multiple files | Simulated daily order arrivals (streaming demos) |
| `dataset/products/products.csv` | CSV | ~500 | Product catalog |
| `dataset/products/products.parquet` | Parquet | ~500 | Product catalog (Parquet format demo) |
| `dataset/workshop/` | CSV | Multiple files | AdventureWorks Lite — star schema scenario |

---

## Additional Materials

### Lakeflow Pipeline Examples (`materials/lakeflow/`)

Pre-built SQL and Python pipeline definitions demonstrating:
- Bronze layer: Auto Loader with `read_files()` and metadata tracking
- Silver layer: SCD Type 1/2 with `AUTO CDC`, data quality expectations
- Gold layer: Star schema with `MATERIALIZED VIEW` joins

These files are uploaded to the Databricks workspace during the Lakeflow demo and workshop.

### Orchestration Scripts (`materials/orchestration/`)

Python scripts demonstrating the 3-task production pattern:
1. **Validate** — check source data quality, exit with JSON status
2. **Transform** — read upstream task values, apply business logic
3. **Report** — aggregate results, log to event table

Includes file arrival and table update trigger demonstrations.

---

## Continuation Path

```
Databricks Fundamental          →  This training  →  Databricks Data Engineer Associate
(Platform, Spark, Delta basics)    (Pipelines,       (Full exam prep: 14 modules,
                                    Optimization,      advanced streaming, CI/CD,
                                    Governance)        Automation Bundles, AI/ML)
```
