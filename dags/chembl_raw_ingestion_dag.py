from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import DAG, Param
from airflow.utils.trigger_rule import TriggerRule

from lib.chembl.raw_ingestion import (
    export_chembl_id_lookup,
    export_molecule_tables,
    upload_raw_files_to_s3,
    validate_raw_files_in_s3,
)


DAG_TIMEZONE = pendulum.timezone("Asia/Yerevan")

AWS_CONN_ID = os.getenv("CHEMBL_AWS_CONN_ID", "aws_s3")
S3_BUCKET = os.getenv(
    "CHEMBL_S3_BUCKET",
    "de-school-educational-data",
)
S3_PROJECT_PREFIX = os.getenv(
    "CHEMBL_S3_PREFIX",
    "final_task/riabkova_maria",
).rstrip("/")

REQUIRED_PARQUET_FILES = (
    "chembl_id_lookup.parquet",
    "molecule_dictionary.parquet",
    "compound_properties.parquet",
    "compound_structures.parquet",
)


def build_release_prefix(chembl_release: int) -> str:
    """Return the versioned S3 prefix for a ChEMBL release."""
    return (
        f"{S3_PROJECT_PREFIX}/raw/"
        f"chembl_{int(chembl_release)}"
    )


def read_release_metadata(
    hook: S3Hook,
    chembl_release: int,
) -> dict[str, Any] | None:
    """Read metadata.json for a release, if it exists and is valid JSON."""
    metadata_key = (
        f"{build_release_prefix(chembl_release)}/metadata.json"
    )

    if not hook.check_for_key(
        key=metadata_key,
        bucket_name=S3_BUCKET,
    ):
        return None

    s3_object = hook.get_key(
        key=metadata_key,
        bucket_name=S3_BUCKET,
    )

    body = s3_object.get()["Body"].read().decode("utf-8-sig")

    try:
        metadata = json.loads(body)
    except json.JSONDecodeError:
        return None

    if not isinstance(metadata, dict):
        return None

    return metadata


def all_required_files_exist(
    hook: S3Hook,
    chembl_release: int,
) -> bool:
    """Check that all four expected Parquet files exist in S3."""
    prefix = build_release_prefix(chembl_release)

    return all(
        hook.check_for_key(
            key=f"{prefix}/{file_name}",
            bucket_name=S3_BUCKET,
        )
        for file_name in REQUIRED_PARQUET_FILES
    )


def metadata_satisfies_request(
    metadata: dict[str, Any],
    chembl_release: int,
    max_records: int,
) -> bool:
    """
    Return True when existing metadata describes a completed dataset
    suitable for the requested release and record limit.

    A full load (max_records=0) can satisfy a later sample request.
    A sample load only satisfies a request with the same sample size.
    """
    if metadata.get("status") != "complete":
        return False

    try:
        stored_release = int(metadata["chembl_release"])
        stored_max_records = int(metadata["max_records"])
    except (KeyError, TypeError, ValueError):
        return False

    requested_release = int(chembl_release)
    requested_max_records = int(max_records)

    if stored_release != requested_release:
        return False

    if stored_max_records == 0:
        return True

    return stored_max_records == requested_max_records


def choose_ingestion_path(
    chembl_release: int,
    max_records: int,
) -> str | list[str]:
    """
    Skip ingestion when S3 already contains a complete suitable load.

    Otherwise run both extraction tasks. Existing files for the same
    release are overwritten by the upload step.
    """
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    metadata = read_release_metadata(
        hook=hook,
        chembl_release=int(chembl_release),
    )

    if (
        metadata is not None
        and metadata_satisfies_request(
            metadata=metadata,
            chembl_release=int(chembl_release),
            max_records=int(max_records),
        )
        and all_required_files_exist(
            hook=hook,
            chembl_release=int(chembl_release),
        )
    ):
        return "release_already_complete"

    return [
        "export_molecule_tables",
        "export_chembl_id_lookup",
    ]


def mark_release_complete(
    chembl_release: int,
    max_records: int,
) -> None:
    """
    Write metadata.json and _SUCCESS only after successful validation.
    """
    release = int(chembl_release)
    record_limit = int(max_records)
    prefix = build_release_prefix(release)

    metadata: dict[str, Any] = {
        "chembl_release": release,
        "status": "complete",
        "source": "ChEMBL REST API",
        "load_scope": (
            "full" if record_limit == 0 else "sample"
        ),
        "max_records": record_limit,
        "files": list(REQUIRED_PARQUET_FILES),
    }

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    hook.load_string(
        string_data=json.dumps(
            metadata,
            ensure_ascii=False,
            indent=2,
        ),
        key=f"{prefix}/metadata.json",
        bucket_name=S3_BUCKET,
        replace=True,
    )

    hook.load_string(
        string_data="",
        key=f"{prefix}/_SUCCESS",
        bucket_name=S3_BUCKET,
        replace=True,
    )


with DAG(
    dag_id="chembl_raw_ingestion",
    description=(
        "Extract a selected ChEMBL release to compact Parquet "
        "and upload it to a versioned S3 raw folder."
    ),
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        7,
        1,
        tz=DAG_TIMEZONE,
    ),
    catchup=False,
    render_template_as_native_obj=True,
    dagrun_timeout=timedelta(hours=8),
    tags=[
        "chembl",
        "raw",
        "s3",
        "de_school",
    ],
    params={
        "chembl_release": Param(
            default=37,
            type="integer",
            minimum=1,
            description=(
                "ChEMBL release number. Data is stored under "
                "raw/chembl_<release>/."
            ),
        ),
        "max_records": Param(
            default=1000,
            type="integer",
            minimum=0,
            description=(
                "Maximum number of records to extract. "
                "Use 0 to load the full dataset."
            ),
        ),
    },
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    check_release_status = BranchPythonOperator(
        task_id="check_release_status",
        python_callable=choose_ingestion_path,
        op_kwargs={
            "chembl_release": "{{ params.chembl_release }}",
            "max_records": "{{ params.max_records }}",
        },
    )

    release_already_complete = EmptyOperator(
        task_id="release_already_complete",
    )

    export_molecule_tables_task = PythonOperator(
        task_id="export_molecule_tables",
        python_callable=export_molecule_tables,
        op_kwargs={
            "chembl_release": "{{ params.chembl_release }}",
            "max_records": "{{ params.max_records }}",
        },
    )

    export_chembl_id_lookup_task = PythonOperator(
        task_id="export_chembl_id_lookup",
        python_callable=export_chembl_id_lookup,
        op_kwargs={
            "chembl_release": "{{ params.chembl_release }}",
            "max_records": "{{ params.max_records }}",
        },
    )

    upload_raw_files = PythonOperator(
        task_id="upload_raw_files_to_s3",
        python_callable=upload_raw_files_to_s3,
        op_kwargs={
            "chembl_release": "{{ params.chembl_release }}",
        },
    )

    validate_raw_files = PythonOperator(
        task_id="validate_raw_files_in_s3",
        python_callable=validate_raw_files_in_s3,
        op_kwargs={
            "chembl_release": "{{ params.chembl_release }}",
        },
    )

    mark_complete = PythonOperator(
        task_id="mark_release_complete",
        python_callable=mark_release_complete,
        op_kwargs={
            "chembl_release": "{{ params.chembl_release }}",
            "max_records": "{{ params.max_records }}",
        },
    )

    finish = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> check_release_status

    check_release_status >> release_already_complete >> finish

    check_release_status >> [
        export_molecule_tables_task,
        export_chembl_id_lookup_task,
    ]

    [
        export_molecule_tables_task,
        export_chembl_id_lookup_task,
    ] >> upload_raw_files

    upload_raw_files >> validate_raw_files >> mark_complete >> finish
