from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, get_current_context

from lib.assets import (
    CHEMBL_ODS_RELEASE_ASSET,
    CHEMBL_RAW_RELEASE_ASSET,
)
from lib.chembl.ods_ingestion import (
    DEFAULT_BATCH_SIZE,
    create_ods_schema,
    finalize_release_and_rebuild_master,
    load_ods_dataset,
    mark_release_loading,
    resolve_raw_asset_event,
    validate_versioned_ods,
)


DAG_TIMEZONE = pendulum.timezone("Asia/Yerevan")


def get_raw_release_context() -> dict:
    """Read metadata from the raw asset event that triggered this DAG."""
    return resolve_raw_asset_event(CHEMBL_RAW_RELEASE_ASSET)


def publish_ods_asset(
    release_context: dict,
    master_result: dict,
) -> dict:
    """Attach release metadata to the completed ODS asset event."""
    context = get_current_context()

    event_extra = {
        "loaded_release": int(
            release_context["chembl_release"]
        ),
        "load_scope": release_context["load_scope"],
        "max_records": int(release_context["max_records"]),
        "source_s3_prefix": release_context["s3_prefix"],
        "master_base_release": int(
            master_result["master_base_release"]
        ),
        "master_rows": int(master_result["master_rows"]),
    }

    context["outlet_events"][CHEMBL_ODS_RELEASE_ASSET].extra = (
        event_extra
    )

    return event_extra


with DAG(
    dag_id="chembl_ods_ingestion",
    description=(
        "Load a completed ChEMBL raw release from S3 into "
        "versioned PostgreSQL ODS tables and rebuild the "
        "cross-release molecule master."
    ),
    schedule=[CHEMBL_RAW_RELEASE_ASSET],
    start_date=pendulum.datetime(
        2026,
        7,
        1,
        tz=DAG_TIMEZONE,
    ),
    catchup=False,
    render_template_as_native_obj=True,
    dagrun_timeout=timedelta(hours=8),
    max_active_runs=1,
    tags=[
        "chembl",
        "ods",
        "postgres",
        "assets",
        "de_school",
    ],
    params={
        "batch_size": Param(
            default=DEFAULT_BATCH_SIZE,
            type="integer",
            minimum=1,
            description=(
                "Number of Parquet rows copied to PostgreSQL "
                "per staging batch."
            ),
        ),
    },
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:
    start = EmptyOperator(task_id="start")

    resolve_release = PythonOperator(
        task_id="resolve_raw_release_context",
        python_callable=get_raw_release_context,
        inlets=[CHEMBL_RAW_RELEASE_ASSET],
    )

    create_schema = PythonOperator(
        task_id="create_ods_schema",
        python_callable=create_ods_schema,
    )

    register_release = PythonOperator(
        task_id="mark_release_loading",
        python_callable=mark_release_loading,
        op_kwargs={
            "release_context": (
                "{{ ti.xcom_pull("
                "task_ids='resolve_raw_release_context') }}"
            ),
        },
    )

    load_active = PythonOperator(
        task_id="load_active_compounds",
        python_callable=load_ods_dataset,
        op_kwargs={
            "dataset_name": "active_compounds",
            "release_context": (
                "{{ ti.xcom_pull("
                "task_ids='resolve_raw_release_context') }}"
            ),
            "batch_size": "{{ params.batch_size }}",
        },
    )

    load_dictionary = PythonOperator(
        task_id="load_molecule_dictionary",
        python_callable=load_ods_dataset,
        op_kwargs={
            "dataset_name": "molecule_dictionary",
            "release_context": (
                "{{ ti.xcom_pull("
                "task_ids='resolve_raw_release_context') }}"
            ),
            "batch_size": "{{ params.batch_size }}",
        },
    )

    load_properties = PythonOperator(
        task_id="load_compound_properties",
        python_callable=load_ods_dataset,
        op_kwargs={
            "dataset_name": "compound_properties",
            "release_context": (
                "{{ ti.xcom_pull("
                "task_ids='resolve_raw_release_context') }}"
            ),
            "batch_size": "{{ params.batch_size }}",
        },
    )

    load_structures = PythonOperator(
        task_id="load_compound_structures",
        python_callable=load_ods_dataset,
        op_kwargs={
            "dataset_name": "compound_structures",
            "release_context": (
                "{{ ti.xcom_pull("
                "task_ids='resolve_raw_release_context') }}"
            ),
            "batch_size": "{{ params.batch_size }}",
        },
    )

    validate_release = PythonOperator(
        task_id="validate_versioned_ods",
        python_callable=validate_versioned_ods,
        op_kwargs={
            "release_context": (
                "{{ ti.xcom_pull("
                "task_ids='resolve_raw_release_context') }}"
            ),
        },
    )

    rebuild_master = PythonOperator(
        task_id="finalize_release_and_rebuild_master",
        python_callable=finalize_release_and_rebuild_master,
        op_kwargs={
            "release_context": (
                "{{ ti.xcom_pull("
                "task_ids='resolve_raw_release_context') }}"
            ),
        },
    )

    publish_asset = PythonOperator(
        task_id="publish_ods_release_asset",
        python_callable=publish_ods_asset,
        op_kwargs={
            "release_context": (
                "{{ ti.xcom_pull("
                "task_ids='resolve_raw_release_context') }}"
            ),
            "master_result": (
                "{{ ti.xcom_pull("
                "task_ids='finalize_release_and_rebuild_master') }}"
            ),
        },
        outlets=[CHEMBL_ODS_RELEASE_ASSET],
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> resolve_release
        >> create_schema
        >> register_release
        >> load_active
    )

    load_active >> [
        load_dictionary,
        load_properties,
        load_structures,
    ]

    [
        load_dictionary,
        load_properties,
        load_structures,
    ] >> validate_release >> rebuild_master >> publish_asset >> finish
