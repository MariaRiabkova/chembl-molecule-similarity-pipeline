# ChEMBL Molecule Similarity Pipeline

Data Engineering School 2026 final project.

The project implements an Apache Airflow pipeline for ingesting ChEMBL data, storing versioned raw datasets in AWS S3, and loading them into a PostgreSQL DWH. The next stages generate Morgan fingerprints, calculate Tanimoto similarities, build a dimensional data mart, and expose analytical views.

## Project goal

For a selected set of source molecules, the final pipeline must identify the 10 most similar ChEMBL molecules using Morgan fingerprints and Tanimoto similarity.

The solution includes:

- automated ingestion from the ChEMBL REST API;
- versioned raw storage in AWS S3;
- PostgreSQL DWH with `ods` and `data_mart` layers;
- Morgan fingerprint generation with RDKit;
- full similarity results stored as Parquet in S3;
- top-10 similarity selection;
- dimensional data mart;
- analytical PostgreSQL views;
- Airflow orchestration;
- data-quality validation;
- failure notifications;
- automated tests;
- a recorded project demo.

## Technology stack

- Python 3.13
- Apache Airflow 3
- PostgreSQL 16
- Docker Compose
- AWS S3
- AWS SSO
- PyArrow
- Psycopg 3
- RDKit
- SQL
- Pytest

## Architecture

```text
ChEMBL REST API
        |
        v
chembl_raw_ingestion
        |
        v
AWS S3 raw layer
raw/chembl_<release>/
        |
        |  Airflow Asset event
        v
chembl_ods_ingestion
        |
        v
PostgreSQL ODS
        |
        v
Morgan fingerprint generation
        |
        v
AWS S3 fingerprint layer
        |
        v
Tanimoto similarity calculation
        |
        +--> full similarity Parquet files in S3
        |
        v
PostgreSQL data mart
        |
        v
Analytical views
```

Airflow and the DWH use separate PostgreSQL services:

- `postgres` — Airflow metadata database;
- `local-db` — project DWH.

Both databases use named Docker volumes, so data survives `docker compose stop` and ordinary `docker compose down`.

## Repository structure

```text
.
├── dags/
│   ├── chembl_raw_ingestion_dag.py
│   ├── chembl_ods_ingestion_dag.py
│   ├── lib/
│   │   ├── assets.py
│   │   ├── chembl/
│   │   │   ├── constants.py
│   │   │   ├── raw_ingestion.py
│   │   │   └── ods_ingestion.py
│   │   └── utils/
│   │       ├── s3.py
│   │       └── teams.py
│   └── sql/
│       └── ods/
├── scripts/
│   └── compact_raw_parquet_fixed_windows.py
├── tests/
├── data/
├── config/
├── logs/
├── plugins/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

## S3 layout

All project objects are stored under:

```text
s3://de-school-educational-data/final_task/riabkova_maria/
```

Raw releases are isolated by ChEMBL release number:

```text
final_task/riabkova_maria/
└── raw/
    ├── chembl_35/
    │   ├── chembl_id_lookup.parquet
    │   ├── molecule_dictionary.parquet
    │   ├── compound_properties.parquet
    │   ├── compound_structures.parquet
    │   └── metadata.json
    └── chembl_37/
        ├── chembl_id_lookup.parquet
        ├── molecule_dictionary.parquet
        ├── compound_properties.parquet
        ├── compound_structures.parquet
        └── metadata.json
```

Old releases are not deleted automatically.

## Raw ingestion DAG

DAG ID:

```text
chembl_raw_ingestion
```

Runtime parameters:

```json
{
  "chembl_release": 37,
  "max_records": 1000,
  "overwrite": false
}
```

Parameter behavior:

- `chembl_release` — release identifier used in S3 paths and metadata;
- `max_records` — maximum number of records for a test load;
- `max_records = 0` — full API load;
- `overwrite = true` — reload even when the release is already marked complete.

Expected S3 prefix:

```text
final_task/riabkova_maria/raw/chembl_<release>/
```

The DAG:

1. checks whether the requested release is already complete;
2. extracts `chembl_id_lookup`;
3. extracts molecule dictionary, properties, and structures;
4. writes Parquet files locally;
5. uploads all files into the release-specific S3 prefix;
6. validates the uploaded objects;
7. writes completion metadata;
8. publishes the raw release as an Airflow Asset.

Example completion metadata:

```json
{
  "chembl_release": 37,
  "status": "complete",
  "source": "ChEMBL REST API",
  "load_scope": "full",
  "max_records": 0,
  "files": [
    "chembl_id_lookup.parquet",
    "molecule_dictionary.parquet",
    "compound_properties.parquet",
    "compound_structures.parquet"
  ]
}
```

### Release handling

The release number is passed explicitly into the export, upload, and validation functions.

Parquet files are uploaded to:

```text
raw/chembl_<release>/<file_name>
```

They are not written directly into the shared `raw/` root.

The public ChEMBL REST API exposes the current release. The `chembl_release` parameter controls project versioning and metadata; it does not force the API to return an arbitrary historical database release. Historical releases should be loaded from an official versioned ChEMBL dump when exact historical content is required.

## Raw datasets

The project ingests the four datasets required by the assignment:

- `chembl_id_lookup`;
- `molecule_dictionary`;
- `compound_properties`;
- `compound_structures`.

Large raw files may be compacted before upload, but the resulting schemas must remain compatible with the ODS loader.

### Compact `compound_structures`

The compact file keeps:

```text
molecule_chembl_id
canonical_smiles
standard_inchi
standard_inchi_key
```

The large `molfile` column is removed because Morgan fingerprints are calculated from SMILES and the field greatly increases file size.

### Compact `compound_properties`

The compact file keeps the columns expected by the ODS transformation:

```text
molecule_chembl_id
alogp
aromatic_rings
full_molformula
full_mwt
hba
hbd
heavy_atoms
mw_freebase
np_likeness_score
num_ro5_violations
psa
qed_weighted
ro3_pass
rtb
cx_logp
molecular_species
```

The one-time compaction utility processes the source file in PyArrow batches and writes Zstandard-compressed Parquet without loading the complete dataset into memory.

## ODS ingestion DAG

DAG ID:

```text
chembl_ods_ingestion
```

The DAG is scheduled by the raw release Airflow Asset. It reads the release metadata from the triggering asset event, including:

- ChEMBL release;
- load scope;
- maximum record count;
- S3 bucket;
- S3 release prefix.

The ODS DAG:

1. resolves the raw release context;
2. creates required ODS objects;
3. registers the release as loading;
4. loads active compounds;
5. loads molecule dictionary, compound properties, and compound structures;
6. validates release-specific row counts;
7. finalizes the release;
8. rebuilds current/master ODS tables;
9. publishes the completed ODS release as an Airflow Asset.

The three main molecule datasets are loaded in parallel after active compounds are available.

### ODS load implementation

Parquet files are:

1. downloaded from S3;
2. read in configurable batches;
3. checked against the required schema;
4. streamed into temporary PostgreSQL staging tables;
5. transformed into versioned ODS tables.

Loading uses:

```text
PostgresHook
    -> psycopg connection
    -> cursor.copy(...)
    -> write_row(...)
```

Temporary CSV files are not used.

Default batch size:

```text
100000 rows
```

Transformation scripts containing multiple SQL statements are executed using Psycopg client-side parameter binding.

## DWH layers

The warehouse contains two logical schemas:

```text
ods
data_mart
```

### ODS

The ODS layer stores versioned ChEMBL data and current/master tables derived from completed releases.

Core datasets:

- active ChEMBL compounds;
- molecule dictionary;
- compound properties;
- compound structures;
- release metadata.

### Data mart

The planned data mart contains:

- molecule dimension;
- molecule-similarity fact table;
- analytical views.

The molecule dimension must include:

- `chembl_id`;
- `molecule_type`;
- `mw_freebase`;
- `alogp`;
- `psa`;
- `cx_logp`;
- `molecular_species`;
- `full_mwt`;
- `aromatic_rings`;
- `heavy_atoms`.

Only molecules referenced by the similarity fact table should be included.

## Fingerprints

Morgan fingerprints are generated with RDKit using:

```text
radius = 2
nBits = 2048
```

Fingerprints are calculated for valid compound structures and stored as files in S3.

Invalid or missing structures are excluded from fingerprint and similarity processing and must be reported by data-quality checks.

## Similarity calculation

Tanimoto similarity is calculated between each selected source molecule and available ChEMBL target fingerprints.

For every source molecule:

- the full similarity result is written to Parquet in S3;
- the 10 highest-scoring targets are loaded into the fact table;
- ties at the top-10 boundary are marked with `has_duplicates_of_last_largest_score`.

The flag is set when additional molecules outside the selected ten have the same score as the final included target.

## Required analytical views

The final project must provide:

- average similarity score per source molecule;
- average absolute deviation of target `alogp` from source `alogp`;
- pivot output for 10 selected source molecules;
- source and target ranking using window functions;
- average similarity grouped by:
  - source molecule;
  - source aromatic rings and heavy atoms;
  - source heavy atoms;
  - the complete dataset.

The complete-dataset aggregation must replace aggregation-generated `NULL` values with `TOTAL` and must not use `UNION` or `UNION ALL`.

## Local setup

### Requirements

Install:

- Docker Desktop;
- Docker Compose;
- AWS CLI v2;
- Git.

### AWS authentication

Authenticate before running S3 tasks:

```powershell
aws sso login --profile De-School-students
```

Verify the active identity:

```powershell
aws sts get-caller-identity --profile De-School-students
```

The local AWS configuration directory is mounted read-only into Airflow containers.

### Environment variables

Create `.env` in the project root.

Example:

```dotenv
AIRFLOW_UID=50000

AWS_PROFILE=De-School-students
AWS_DEFAULT_REGION=eu-central-1

AIRFLOW_CONN_AWS_S3=aws://?region_name=eu-central-1
AIRFLOW_CONN_DWH_POSTGRES=postgresql://postgres:123456@local-db:5432/postgres

CHEMBL_AWS_CONN_ID=aws_s3
CHEMBL_S3_BUCKET=de-school-educational-data
CHEMBL_S3_PREFIX=final_task/riabkova_maria
CHEMBL_LOCAL_DATA_DIR=/opt/airflow/data/chembl
```

Do not commit access keys, session tokens, passwords, or webhook URLs.

### Start the project

Build and start the environment:

```powershell
docker compose up -d --build
```

Check service status:

```powershell
docker compose ps
```

Check DAG import errors:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected output:

```text
No data found
```

Airflow UI:

```text
http://localhost:8081
```

Local DWH connection:

```text
Host: localhost
Port: 5433
Database: postgres
User: postgres
Password: 123456
SSL: disable
```

Internal Airflow DWH connection:

```text
Host: local-db
Port: 5432
Database: postgres
```

## Docker data persistence

The Docker Compose environment uses named volumes:

```text
chembl-molecule-similarity-pipeline_dwh-db-volume
chembl-molecule-similarity-pipeline_postgres-db-volume
```

Safe commands:

```powershell
docker compose stop
docker compose down
docker compose up -d
```

These commands preserve PostgreSQL data.

Do not use the following unless database deletion is intended:

```powershell
docker compose down -v
docker volume rm <volume>
docker system prune --volumes
```

## Useful validation commands

List release files in S3:

```powershell
aws s3 ls `
  s3://de-school-educational-data/final_task/riabkova_maria/raw/chembl_37/ `
  --profile De-School-students
```

Inspect Parquet columns:

```powershell
python -c "import pyarrow.parquet as pq; print(pq.ParquetFile(r'.\data\chembl\raw\compound_structures.parquet').schema_arrow.names)"
```

List ODS tables:

```powershell
docker compose exec local-db psql `
  -U postgres `
  -d postgres `
  -c "\dt ods.*"
```

Check release row counts:

```sql
SELECT chembl_release, COUNT(*)
FROM ods.molecule_dictionary
GROUP BY chembl_release
ORDER BY chembl_release;
```

## Tests

Run:

```powershell
pytest
```

The test suite should cover:

- normalization functions;
- API retries and pagination;
- release-specific S3 key construction;
- metadata validation;
- release skip/reload logic;
- compact Parquet schemas;
- ODS staging and transformation logic;
- data-quality validation;
- Morgan fingerprint generation;
- Tanimoto similarity;
- top-10 boundary tie logic;
- analytical SQL assumptions.

## Failure notifications

Airflow failure callbacks may send alerts to the shared MS Teams channel through a webhook stored in an Airflow Connection or environment variable.

Webhook URLs must never be committed to Git.

## Current implementation status

Completed:

- Docker Compose environment;
- Airflow 3 deployment;
- separate Airflow metadata and DWH PostgreSQL services;
- persistent PostgreSQL volumes;
- AWS SSO integration;
- versioned ChEMBL raw ingestion;
- release-specific S3 paths;
- full and sample raw loading;
- compact Parquet utility;
- raw release metadata;
- Airflow Asset publication;
- asset-triggered ODS ingestion;
- batch Parquet-to-PostgreSQL loading;
- versioned ODS tables;
- ODS validation and finalization;
- successful end-to-end raw-to-ODS test.

In progress:

- Morgan fingerprint generation;
- similarity calculation;
- full similarity Parquet output;
- top-10 fact loading;
- dimensional data mart;
- analytical views;
- complete automated test coverage;
- final notification workflow;
- demo recording.

## Benchmark and limitations

To be completed before final submission.

The final README should include:

- raw ingestion duration;
- ODS load duration;
- fingerprint generation duration;
- similarity comparisons per second;
- peak memory consumption;
- expected full-run duration;
- expected full-result storage size;
- configuration used for the final demo.

## Demo

The recorded demo must be no longer than 10 minutes and should show:

1. architecture;
2. Docker startup;
3. raw and ODS Airflow DAGs;
4. release-specific S3 data;
5. PostgreSQL DWH layers;
6. fingerprint generation;
7. similarity processing;
8. top-10 output;
9. analytical views;
10. validation and failure handling.

Demo link:

```text
TODO: add OneDrive shareable link
```

## Author

Maria Riabkova  
Data Engineering School 2026
