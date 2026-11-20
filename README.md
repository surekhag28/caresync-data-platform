# CareSync – End-to-End Healthcare Data Engineering Pipeline (Azure)

An end-to-end, metadata-driven data platform on Azure that ingests healthcare data from a PostgreSQL database and CSV files, processes it through a **medallion architecture** (bronze → silver → gold) in **Azure Databricks**, and produces a business-ready **Patient 360** table for reporting.

> **Code repositories**
> - ADF pipelines: [`adf-data-engineering-caresync`](https://github.com/surekhag28/adf-data-engineering-caresync)
> - Databricks notebooks: [`adb-caresync`](https://github.com/surekhag28/adb-caresync)
> - This repo: project documentation, architecture and SQL scripts

---

## 1. Business goal

Give care teams and managers **one trusted view of every patient** so they can:

- spot patients with **critical or abnormal lab results** early
- see patient load and risk **by hospital and insurer**
- understand the **patient population** (age, gender, location)

---

## 2. Architecture

```
SOURCES                      INGESTION (ADF)                 STORAGE (ADLS Gen2)
Neon PostgreSQL  ─┐                                          landing container
 (hospitals,       │   Lookup config → ForEach table          ├── neon_postgres/<table>/<load_folder>/  (Parquet)
  patients, ...)   ├─► Copy activity (FULL / incremental) ──► └── adls_csv/<table>/<load_folder>/       (CSV)
ADLS CSV files    ─┘   watermark-based incremental loads

PROCESSING (Azure Databricks + Unity Catalog: adb_caresync)
landing volume ──► BRONZE (raw, append, audit columns)
                ──► SILVER (cleaned, deduplicated, FULL / APPEND / MERGE)
                ──► GOLD   (patient_360 business table)

MONITORING
ADF ──► Logic App (HTTP trigger) ──► email run summary (SUCCESS / FAILED + status table)
```

_Add an architecture diagram image here: `docs/architecture.png`_

---

## 3. Tech stack

| Area | Tools |
|---|---|
| Source | PostgreSQL (Neon), CSV files in ADLS |
| Ingestion & orchestration | Azure Data Factory (metadata-driven, Lookup + ForEach, watermarks) |
| Storage | Azure Data Lake Storage Gen2 (`landing`, `medallion` containers) |
| Processing | Azure Databricks (PySpark, Spark SQL, Delta Lake) |
| Governance | Unity Catalog (storage credential, external locations, volumes) |
| Alerts | Azure Logic Apps (email) |
| Version control | GitHub |

---

## 4. Data model

**Source tables:** `hospitals`, `patients`, `insurance_providers`, `lab_results` _(add/remove to match your project)_

**Unity Catalog layout**

```
adb_caresync (catalog)
 ├── landing  → landing-volume (files from ADF)
 ├── bronze   → raw Delta tables (managed location: medallion/bronze)
 ├── silver   → cleaned Delta tables (managed location: medallion/silver)
 └── gold     → patient_360 (managed location: medallion/gold)
```

**Gold: `patient_360`** – one row per patient with demographics, age, hospital name, insurer name, total tests, abnormal and critical test counts, and last test date.

---

## 5. Pipeline design

### Metadata-driven ingestion
A config table controls which tables are loaded and how:

| Column | Purpose |
|---|---|
| `source_table_name` | Table or folder to load |
| `source_type` | e.g. `postgres` or `adls_csv` |
| `load_type` | `FULL`, `APPEND` or `MERGE` |
| `merge_keys` | Key columns for MERGE (comma-separated) |
| `watermark_column` | Column (or file LastModified) used for incremental loads |

### Load types
| Load type | Bronze | Silver |
|---|---|---|
| FULL | Append with load timestamp | Overwrite |
| APPEND | Append | Append |
| MERGE | Append | Delta `MERGE` on `merge_keys` (latest row per key) |

### Incremental loads
- **Database tables:** `WHERE <watermark_column> > <last watermark>`
- **CSV files:** filter by file *last modified* between the last watermark and `pipeline().TriggerTime`
- The watermark is updated only after a successful run.

### Error handling & alerts
- Notebooks return a status/record count via `dbutils.notebook.exit()`
- Missing paths (no new data) are skipped instead of failing the run
- A Logic App emails a run summary: `<ENV> | SUCCESS/FAILED | CARESYNC MASTER RUN`

---

## 6. Repository contents (this repo)

```
├── README.md
├── docs/
│   └── architecture.png
└── sql/
    ├── source/
    │   ├── 01_create_source_tables.sql
    │   └── 02_insert_sample_data.sql
    └── metadata/
        ├── 01_create_config_table.sql
        └── 02_create_watermark_table.sql
```

---

## 7. How to run

1. **Source:** run the scripts in `sql/source/` on PostgreSQL (in order).
2. **Metadata:** run the scripts in `sql/metadata/` to create the config and watermark tables.
3. **Azure setup:** resource group, ADLS Gen2 (`landing`, `medallion`), Databricks workspace (Premium), Access Connector with *Storage Blob Data Contributor*.
4. **Unity Catalog:** storage credential → external locations → catalog `adb_caresync` → schemas → landing volume.
5. **ADF:** import the pipelines from the ADF repo, set the global parameter `env`, update linked services.
6. **Run** the master pipeline and check the email summary.

---

## 8. Key learnings

- Unity Catalog setup: credentials, external locations, managed vs external storage, volumes
- Metadata-driven ADF pipelines with watermarks
- Delta Lake `MERGE`, schema evolution and time travel
- Designing a gold table for a clear business use case

## 9. Future improvements

- Streaming ingestion with Auto Loader / Event Hubs
- Data quality checks and an audit log table
- CI/CD with GitHub Actions and Databricks Asset Bundles
- Power BI / Databricks SQL dashboard on `patient_360`
