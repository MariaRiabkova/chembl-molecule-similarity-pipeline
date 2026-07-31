from __future__ import annotations

import os
from pathlib import Path


CHEMBL_API_BASE_URL = "https://www.ebi.ac.uk/chembl/api/data"

CHEMBL_RELEASE = os.getenv("CHEMBL_RELEASE", "37")

AWS_CONN_ID = os.getenv(
    "CHEMBL_AWS_CONN_ID",
    "aws_s3",
)

S3_BUCKET_NAME = os.getenv(
    "CHEMBL_S3_BUCKET",
    "de-school-educational-data",
)

S3_PROJECT_PREFIX = os.getenv(
    "CHEMBL_S3_PREFIX",
    "final_task/riabkova_maria",
)

S3_RAW_PREFIX = f"{S3_PROJECT_PREFIX}/raw"

LOCAL_DATA_DIR = Path(
    os.getenv(
        "CHEMBL_LOCAL_DATA_DIR",
        "/opt/airflow/data/chembl",
    )
)

LOCAL_RAW_DIR = LOCAL_DATA_DIR / "raw"

RAW_FILE_NAMES = (
    "chembl_id_lookup.parquet",
    "molecule_dictionary.parquet",
    "compound_properties.parquet",
    "compound_structures.parquet",
)
