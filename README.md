# ChEMBL Molecule Similarity Pipeline

Data Engineering School 2026 final project.

The project builds an Airflow-orchestrated pipeline that ingests ChEMBL data, stores the raw layer in AWS S3, loads a PostgreSQL DWH, computes Morgan fingerprints and Tanimoto similarities, and exposes the top-10 most similar molecules through a data mart and analytical views.

## Project goal

For a given set of source molecules, the pipeline identifies the 10 most similar ChEMBL molecules using Morgan fingerprints and Tanimoto similarity.

The solution includes:

- automated ChEMBL ingestion;
- versioned raw storage in AWS S3;
- PostgreSQL DWH with at least two layers;
- Morgan fingerprint generation with RDKit;
- full similarity result storage in Parquet;
- top-10 similarity selection;
- dimensional data mart;
- analytical PostgreSQL views;
- Airflow orchestration;
- data quality checks;
- failure notifications;
- tests;
- a short recorded demo.

## Technology stack

- Python 3
- Apache Airflow 3
- PostgreSQL 16
- AWS S3
- AWS SSO
- Docker Compose
- PyArrow
- Pandas
- RDKit
- SQL
- Pytest

## Architecture

```text
ChEMBL REST API
        |
        v
Airflow raw ingestion DAG
        |
        v
AWS S3 raw layer
final_task/riabkova_maria/raw/chembl_<release>/
        |
        v
PostgreSQL ODS
        |
        v
Fingerprint generation
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

The PostgreSQL DWH is launched locally through Docker Compose. Airflow uses a separate PostgreSQL database for its internal metadata.

## DWH layers

The warehouse uses at least two logical layers:

```text
ods
data_mart
```

### ODS

The ODS layer stores the four required ChEMBL datasets:

- `ods.chembl_id_lookup`
- `ods.molecule_dictionary`
- `ods.compound_properties`
- `ods.compound_structures`

### Data mart

The data mart contains:

- a molecule dimension with the properties required by the assignment;
- a similarity fact table with source molecule, target molecule, Tanimoto score and the boundary duplicate flag;
- analytical views for similarity and molecule-property analysis.

## S3 structure

All project objects are stored under:

```text
s3://de-school-educational-data/final_task/riabkova_maria/
```

Current raw structure:

```text
final_task/riabkova_maria/
└── raw/
    └── chembl_37/
        ├── chembl_id_lookup.parquet
        ├── molecule_dictionary.parquet
        ├── compound_properties.parquet
        ├── compound_structures.parquet
        ├── metadata.json
        └── _SUCCESS
```

Future releases are stored separately:

```text
raw/chembl_38/
raw/chembl_39/
```

Old releases are not deleted automatically.

## ChEMBL ingestion strategy

The ingestion DAG accepts two runtime parameters:

```json
{
  "chembl_release": 37,
  "max_records": 1000
}
```

Parameters:

- `chembl_release` — ChEMBL release number used for versioned S3 storage;
- `max_records` — maximum number of records to ingest;
- `max_records = 0` — ingest the full dataset.

The DAG checks:

```text
raw/chembl_<release>/metadata.json
```

If metadata contains a matching release, `status = complete`, a suitable `max_records` value, and all four required Parquet files exist, ingestion is skipped and the DAG finishes successfully.

If the folder or metadata does not exist, metadata is incomplete, files are missing, or the stored sample does not satisfy the requested load size, the release is ingested again and existing files are replaced.

A completed release has metadata similar to:

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

The `_SUCCESS` zero-byte object is written only after all files have been uploaded and validated.

## Compact Parquet files

Only columns required for downstream processing and the final data mart are kept in the compact raw files.

This is especially important for `compound_structures`. Large text fields such as `molfile` are not required for Morgan fingerprint calculation and significantly increase storage size.

The compact structure dataset keeps the identifiers and structure representation needed by RDKit, including:

- `molecule_chembl_id`;
- `canonical_smiles`;
- `standard_inchi_key`.

Parquet files are written with Zstandard compression.

## API reliability

ChEMBL data is fetched through the ChEMBL REST API.

The ingestion implementation uses:

- persistent HTTP sessions;
- pagination;
- batching;
- retries for temporary server errors;
- exponential backoff;
- request timeouts;
- streaming writes to Parquet.

Temporary API errors such as HTTP 500 do not immediately fail the task. The failed page is retried.

For a production-scale historical reload, an official ChEMBL database dump may be preferable to the REST API because it avoids REST pagination and request-rate limitations. The ingestion operation must still remain automated inside the pipeline.

## ChEMBL release limitation

The public ChEMBL REST API exposes the currently active release. The `chembl_release` DAG parameter controls versioned storage and expected metadata; it does not make the API serve an arbitrary historical release.

Historical releases should be loaded from an official versioned ChEMBL dump if required.

## Missing ChEMBL 37 properties

The assignment requires these dimension columns:

- `cx_logp`
- `molecular_species`

They are not available in ChEMBL release 37.

The columns are retained in the molecule dimension to satisfy the required schema and are populated with `NULL`.

An optional enhancement would be to backfill these properties from an earlier ChEMBL release.

## Fingerprints

Morgan fingerprints are generated with RDKit using:

```text
radius = 2
nBits = 2048
```

Fingerprints are calculated for all valid compound structures and stored in S3.

Invalid or missing structures are excluded from fingerprint and similarity calculations and are reported by data quality checks.

## Similarity calculation

Tanimoto similarity is calculated between each source molecule and the available ChEMBL target fingerprints.

For each source molecule:

- the full similarity result is saved as Parquet in S3;
- the highest-scoring 10 target molecules are loaded into the fact table;
- boundary ties are marked with `has_duplicates_of_last_largest_score`.

The duplicate flag applies only when additional target molecules have the same similarity score as the last included row in the top-10.

## Full and sample calculation modes

Full ChEMBL similarity computation can be expensive in execution time, memory and output size.

The project supports:

- full calculation mode;
- configurable sample calculation mode.

Fingerprints should normally be calculated for all valid ChEMBL structures. Similarity calculation may use a documented subset when full execution is not practical in the available course environment.

Benchmark results, limitations and the selected compromise must be documented in this README before final delivery.

## Analytical views

The final PostgreSQL views include:

- average similarity score per source molecule;
- average absolute deviation of target `alogp` from source `alogp`;
- pivot output for 10 selected source molecules;
- source and target ranking with the next and second-most-similar target identifiers;
- average similarity grouped by:
  - source molecule;
  - source aromatic rings and heavy atoms;
  - source heavy atoms;
  - whole dataset.

The whole-dataset aggregation uses `TOTAL` instead of aggregation-generated `NULL` values and does not use `UNION` or `UNION ALL`.

## Local setup

### Requirements

Install:

- Docker Desktop;
- Docker Compose;
- AWS CLI v2;
- AWS Session Manager / SSO support;
- Git.

### AWS authentication

Authenticate before starting S3 tasks:

```powershell
aws sso login --profile De-School-students
```

Verify the session:

```powershell
aws sts get-caller-identity --profile De-School-students
```

The local AWS configuration directory is mounted read-only into the Airflow containers.

### Environment variables

Create `.env` in the project root.

Example:

```dotenv
AIRFLOW_UID=50000

AWS_PROFILE=De-School-students
AWS_DEFAULT_REGION=eu-central-1

AIRFLOW_CONN_AWS_S3=aws://?region_name=eu-central-1

CHEMBL_AWS_CONN_ID=aws_s3
CHEMBL_S3_BUCKET=de-school-educational-data
CHEMBL_S3_PREFIX=final_task/riabkova_maria
CHEMBL_LOCAL_DATA_DIR=/opt/airflow/data/chembl

AIRFLOW_CONN_DWH_POSTGRES=postgresql://postgres:123456@local-db:5432/postgres
```

Do not commit secrets, access keys, passwords or webhook URLs.

### Start the project

```powershell
docker compose up -d --build
```

Check services:

```powershell
docker compose ps
```

Airflow UI:

```text
http://localhost:8081
```

Local DWH connection from the host machine:

```text
Host: localhost
Port: 5433
Database: postgres
User: postgres
Password: 123456
SSL: disable
```

Airflow connects to the same DWH internally using:

```text
Host: local-db
Port: 5432
```

## Airflow DAGs

### `chembl_raw_ingestion`

Purpose:

- check whether the requested ChEMBL release already exists;
- ingest lookup and molecule datasets;
- write compact Parquet files;
- upload files to S3;
- validate uploaded objects;
- write `metadata.json`;
- write `_SUCCESS`.

Expected skip path:

```text
start
  -> check_release_status
  -> release_already_complete
  -> finish
```

Expected ingestion path:

```text
start
  -> check_release_status
  -> export_molecule_tables
  -> export_chembl_id_lookup
  -> upload_raw_files_to_s3
  -> validate_raw_files_in_s3
  -> mark_release_complete
  -> finish
```

### Planned downstream DAGs

```text
chembl_ods_load
fingerprint_generation
molecule_similarity
data_mart_build
```

These may be implemented as separate DAGs or as clearly separated task groups.

## PostgreSQL schemas

Create the schemas before loading data:

```sql
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS data_mart;
```

Check them:

```sql
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('ods', 'data_mart')
ORDER BY schema_name;
```

## Tests

Run tests inside the project environment:

```powershell
pytest
```

The test suite should cover:

- normalization functions;
- retry behavior;
- metadata validation;
- release skip/reload logic;
- S3 key construction;
- compact column selection;
- fingerprint generation;
- Tanimoto similarity;
- top-10 boundary duplicate logic;
- SQL data quality assumptions.

## Failure notifications

Airflow failure callbacks send notifications to the shared MS Teams test-alert channel through a webhook stored in an Airflow Connection or environment secret.

The webhook must never be committed to Git.

## Repository structure

```text
.
├── dags/
│   ├── chembl_raw_ingestion_dag.py
│   ├── lib/
│   │   ├── chembl/
│   │   │   ├── constants.py
│   │   │   └── raw_ingestion.py
│   │   └── utils/
│   │       ├── s3.py
│   │       └── teams.py
│   └── sql/
├── scripts/
│   └── compact_raw_parquet.py
├── tests/
├── data/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

## Current implementation status

Completed:

- Docker Compose environment;
- Airflow deployment;
- local PostgreSQL DWH service;
- AWS SSO integration;
- versioned ChEMBL raw ingestion;
- full ChEMBL 37 extraction;
- compact Parquet conversion;
- raw data upload to S3;
- `metadata.json` and `_SUCCESS` convention;
- release skip/reload branching logic;
- creation of `ods` and `data_mart` schemas.

In progress:

- loading raw Parquet files into ODS;
- fingerprint generation;
- similarity calculation;
- dimensional data mart;
- analytical views;
- complete automated test coverage;
- final notifications;
- demo recording.

## Benchmark and limitations

To be completed before final submission.

The final README should include measured values for:

- fingerprint generation time;
- similarity comparisons per second;
- memory usage;
- expected full-run duration;
- expected full-result storage size;
- selected sample/full configuration for the demo environment.

## Example results

To be added after the data mart and views are complete.

Example sections should include:

- several source molecules;
- their top-10 target molecules;
- similarity scores;
- duplicate-boundary flag;
- output from each required analytical view.

## Demo

The final demo video must be no longer than 10 minutes and should show:

1. architecture;
2. project launch;
3. Airflow DAGs;
4. S3 raw and result structure;
5. PostgreSQL DWH layers;
6. fingerprint and similarity workflow;
7. top-10 results;
8. analytical views;
9. data quality and failure notification behavior.

Demo link:

```text
TODO: add OneDrive shareable link
```

## Author

Maria Riabkova  
Data Engineering School 2026
