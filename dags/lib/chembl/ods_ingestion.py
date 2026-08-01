from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from psycopg import ClientCursor, sql
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import get_current_context


logger = logging.getLogger(__name__)

AWS_CONN_ID = os.getenv("CHEMBL_AWS_CONN_ID", "aws_s3")
POSTGRES_CONN_ID = os.getenv(
    "CHEMBL_DWH_POSTGRES_CONN_ID",
    "dwh_postgres",
)

SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "ods"

DEFAULT_BATCH_SIZE = 100_000


@dataclass(frozen=True)
class OdsLoadConfig:
    file_name: str
    staging_table: str
    columns: tuple[str, ...]
    transform_sql_file: str


LOAD_CONFIGS: dict[str, OdsLoadConfig] = {
    "active_compounds": OdsLoadConfig(
        file_name="chembl_id_lookup.parquet",
        staging_table="stg_chembl_id_lookup",
        columns=(
            "chembl_id",
            "entity_type",
            "last_active",
            "status",
        ),
        transform_sql_file="replace_active_compounds.sql",
    ),
    "molecule_dictionary": OdsLoadConfig(
        file_name="molecule_dictionary.parquet",
        staging_table="stg_molecule_dictionary",
        columns=(
            "molecule_chembl_id",
            "molecule_type",
            "pref_name",
            "max_phase",
            "first_approval",
            "structure_type",
            "natural_product",
            "therapeutic_flag",
            "oral",
            "parenteral",
            "topical",
            "black_box_warning",
            "chemical_probe",
            "chirality",
            "dosed_ingredient",
            "first_in_class",
            "inorganic_flag",
            "orphan",
            "polymer_flag",
            "prodrug",
            "veterinary",
            "withdrawn_flag",
        ),
        transform_sql_file="replace_molecule_dictionary.sql",
    ),
    "compound_properties": OdsLoadConfig(
        file_name="compound_properties.parquet",
        staging_table="stg_compound_properties",
        columns=(
            "molecule_chembl_id",
            "alogp",
            "aromatic_rings",
            "full_molformula",
            "full_mwt",
            "hba",
            "hbd",
            "heavy_atoms",
            "mw_freebase",
            "np_likeness_score",
            "num_ro5_violations",
            "psa",
            "qed_weighted",
            "ro3_pass",
            "rtb",
            "cx_logp",
            "molecular_species",
        ),
        transform_sql_file="replace_compound_properties.sql",
    ),
    "compound_structures": OdsLoadConfig(
        file_name="compound_structures.parquet",
        staging_table="stg_compound_structures",
        columns=(
            "molecule_chembl_id",
            "canonical_smiles",
            "standard_inchi",
            "standard_inchi_key",
        ),
        transform_sql_file="replace_compound_structures.sql",
    ),
}


def read_sql(file_name: str) -> str:
    """Read SQL stored under dags/sql/ods."""
    path = SQL_DIR / file_name

    if not path.exists():
        raise FileNotFoundError(f"SQL file does not exist: {path}")

    return path.read_text(encoding="utf-8")


def resolve_raw_asset_event(raw_asset) -> dict[str, Any]:
    """Return metadata from the raw asset event that triggered this DAG."""
    context = get_current_context()
    events = context["inlet_events"][raw_asset]

    if not events:
        raise ValueError("No raw ChEMBL asset event is available")

    event = events[-1]
    extra = dict(event.extra or {})

    required_keys = {
        "chembl_release",
        "load_scope",
        "max_records",
        "s3_bucket",
        "s3_prefix",
    }
    missing = sorted(required_keys - extra.keys())

    if missing:
        raise ValueError(
            f"Raw asset event is missing fields: {missing}"
        )

    return extra


def create_ods_schema() -> None:
    """Create ODS objects using the version-controlled SQL file."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(read_sql("create_ods_schema.sql"))


def mark_release_loading(
    release_context: dict[str, Any],
) -> None:
    """Register the release before loading its versioned ODS rows."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        read_sql("mark_release_loading.sql"),
        parameters={
            "chembl_release": int(
                release_context["chembl_release"]
            ),
            "load_scope": str(release_context["load_scope"]),
            "max_records": int(release_context["max_records"]),
            "source_s3_prefix": str(
                release_context["s3_prefix"]
            ),
        },
    )


def _download_source_file(
    release_context: dict[str, Any],
    file_name: str,
    destination: Path,
) -> None:
    bucket = str(release_context["s3_bucket"])
    key = f"{str(release_context['s3_prefix']).rstrip('/')}/{file_name}"

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    if not hook.check_for_key(key=key, bucket_name=bucket):
        raise FileNotFoundError(f"s3://{bucket}/{key}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    hook.get_conn().download_file(
        bucket,
        key,
        str(destination),
    )


def _create_text_staging_table(
    cursor,
    table_name: str,
    columns: tuple[str, ...],
) -> None:
    """Create a transaction-scoped staging table with text columns."""
    column_definitions = sql.SQL(", " ).join(
        sql.SQL("{} text").format(sql.Identifier(column))
        for column in columns
    )

    query = sql.SQL(
        "CREATE TEMP TABLE {} ({}) ON COMMIT DROP"
    ).format(
        sql.Identifier(table_name),
        column_definitions,
    )

    cursor.execute(query)


def _to_staging_text(value: Any) -> str | None:
    """Normalize a PyArrow/Python value for a text staging column."""
    if value is None:
        return None

    if isinstance(value, bool):
        return "true" if value else "false"

    return str(value)


def _copy_record_batch(
    cursor,
    table_name: str,
    columns: tuple[str, ...],
    record_batch,
) -> int:
    """Stream one PyArrow record batch into PostgreSQL via psycopg 3 COPY."""
    if not hasattr(cursor, "copy"):
        raise TypeError(
            "PostgresHook returned a non-psycopg3 cursor. "
            "Enable psycopg 3 for the Airflow PostgreSQL provider."
        )

    copy_query = sql.SQL(
        "COPY {} ({}) FROM STDIN"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", " ).join(
            sql.Identifier(column)
            for column in columns
        ),
    )

    rows = record_batch.to_pylist()

    with cursor.copy(copy_query) as copy:
        for record in rows:
            copy.write_row(
                tuple(
                    _to_staging_text(record[column])
                    for column in columns
                )
            )

    return len(rows)


def load_ods_dataset(
    dataset_name: str,
    release_context: dict[str, Any],
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, Any]:
    """
    Load one raw Parquet file into a temporary text staging table and
    execute its release-aware transformation SQL.
    """
    if dataset_name not in LOAD_CONFIGS:
        raise ValueError(f"Unknown dataset: {dataset_name}")

    config = LOAD_CONFIGS[dataset_name]
    release = int(release_context["chembl_release"])
    normalized_batch_size = int(batch_size)

    if normalized_batch_size < 1:
        raise ValueError("batch_size must be positive")

    with tempfile.TemporaryDirectory(
        prefix=f"chembl_{release}_{dataset_name}_"
    ) as temporary_directory:
        local_path = (
            Path(temporary_directory) / config.file_name
        )

        _download_source_file(
            release_context=release_context,
            file_name=config.file_name,
            destination=local_path,
        )

        parquet_file = pq.ParquetFile(local_path)
        available_columns = set(parquet_file.schema_arrow.names)
        missing_columns = sorted(
            set(config.columns) - available_columns
        )

        if missing_columns:
            raise ValueError(
                f"{config.file_name} is missing columns: "
                f"{missing_columns}"
            )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = hook.get_conn()
        connection.autocommit = False
        staged_rows = 0

        try:
            with connection.cursor() as cursor:
                _create_text_staging_table(
                    cursor=cursor,
                    table_name=config.staging_table,
                    columns=config.columns,
                )

                for batch in parquet_file.iter_batches(
                    batch_size=normalized_batch_size,
                    columns=list(config.columns),
                ):
                    staged_rows += _copy_record_batch(
                        cursor=cursor,
                        table_name=config.staging_table,
                        columns=config.columns,
                        record_batch=batch,
                    )

                cursor.execute(
                    f"SELECT COUNT(*) "
                    f"FROM {config.staging_table}"
                )
                actual_staged_rows = int(cursor.fetchone()[0])

                if actual_staged_rows != staged_rows:
                    raise ValueError(
                        "Staging row count mismatch: "
                        f"expected={staged_rows}, "
                        f"actual={actual_staged_rows}"
                    )

                with ClientCursor(connection) as script_cursor:
                    script_cursor.execute(
                        read_sql(config.transform_sql_file),
                        {"chembl_release": release},
                    )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    logger.info(
        "Loaded ChEMBL ODS dataset. release=%s dataset=%s "
        "staged_rows=%s",
        release,
        dataset_name,
        staged_rows,
    )

    return {
        "chembl_release": release,
        "dataset": dataset_name,
        "staged_rows": staged_rows,
    }


def validate_versioned_ods(
    release_context: dict[str, Any],
) -> dict[str, int]:
    """Validate release-specific ODS tables before master rebuild."""
    release = int(release_context["chembl_release"])
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    queries = {
        "active_compounds": (
            "SELECT COUNT(*) "
            "FROM ods.chembl_compounds_active "
            "WHERE chembl_release = %s"
        ),
        "molecule_dictionary": (
            "SELECT COUNT(*) "
            "FROM ods.molecule_dictionary "
            "WHERE chembl_release = %s"
        ),
        "compound_properties": (
            "SELECT COUNT(*) "
            "FROM ods.compound_properties "
            "WHERE chembl_release = %s"
        ),
        "compound_structures": (
            "SELECT COUNT(*) "
            "FROM ods.compound_structures "
            "WHERE chembl_release = %s"
        ),
    }

    counts: dict[str, int] = {}

    with hook.get_conn() as connection:
        with connection.cursor() as cursor:
            for name, query in queries.items():
                cursor.execute(query, (release,))
                counts[name] = int(cursor.fetchone()[0])

            if counts["active_compounds"] == 0:
                raise ValueError(
                    f"No active compounds loaded for release {release}"
                )

            for child_table in (
                "molecule_dictionary",
                "compound_properties",
                "compound_structures",
            ):
                if counts[child_table] > counts["active_compounds"]:
                    raise ValueError(
                        f"{child_table} contains more rows than "
                        "the active-compound population"
                    )

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM ods.compound_structures
                WHERE chembl_release = %s
                  AND canonical_smiles IS NOT NULL
                  AND btrim(canonical_smiles) <> ''
                """,
                (release,),
            )
            counts["structures_with_smiles"] = int(
                cursor.fetchone()[0]
            )

            if counts["structures_with_smiles"] == 0:
                raise ValueError(
                    f"No fingerprint candidates for release {release}"
                )

    return counts


def finalize_release_and_rebuild_master(
    release_context: dict[str, Any],
) -> dict[str, int]:
    """
    Mark the loaded release complete and atomically rebuild the
    cross-release golden record.
    """
    release = int(release_context["chembl_release"])
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    connection = hook.get_conn()
    connection.autocommit = False

    try:
        with connection.cursor() as cursor:
            with ClientCursor(connection) as script_cursor:
                script_cursor.execute(
                    read_sql(
                        "finalize_release_and_rebuild_master.sql"
                    ),
                    {"chembl_release": release},
                )

            cursor.execute(
                """
                SELECT MAX(chembl_release)
                FROM ods.chembl_releases
                WHERE status = 'complete'
                """
            )
            base_release = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(*) "
                "FROM ods.chembl_compounds_master"
            )
            master_rows = int(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM ods.chembl_compounds_active
                WHERE chembl_release = %s
                """,
                (base_release,),
            )
            expected_rows = int(cursor.fetchone()[0])

            if master_rows != expected_rows:
                raise ValueError(
                    "Master row count does not match the active "
                    "population of the newest complete release: "
                    f"master={master_rows}, expected={expected_rows}"
                )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return {
        "loaded_release": release,
        "master_base_release": int(base_release),
        "master_rows": master_rows,
    }
