from __future__ import annotations

import os

from airflow.sdk import Asset


S3_BUCKET = os.getenv(
    "CHEMBL_S3_BUCKET",
    "de-school-educational-data",
)

S3_PROJECT_PREFIX = os.getenv(
    "CHEMBL_S3_PREFIX",
    "final_task/riabkova_maria",
).rstrip("/")


CHEMBL_RAW_RELEASE_ASSET = Asset(
    name="chembl_raw_release",
    uri=(
        f"s3://{S3_BUCKET}/"
        f"{S3_PROJECT_PREFIX}/raw/chembl_release"
    ),
)


CHEMBL_ODS_RELEASE_ASSET = Asset(
    name="chembl_ods_release",
    uri="chembl://ods/release",
)
