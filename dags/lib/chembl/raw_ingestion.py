from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from lib.chembl.constants import (
    AWS_CONN_ID,
    CHEMBL_API_BASE_URL,
    LOCAL_RAW_DIR,
    RAW_FILE_NAMES,
    S3_BUCKET_NAME,
    S3_RAW_PREFIX,
)
from lib.utils.s3 import (
    object_exists,
    upload_file,
)


logger = logging.getLogger(__name__)

PAGE_SIZE = 1000
REQUEST_TIMEOUT_SECONDS = 60
REQUEST_DELAY_SECONDS = 0.2
MAX_RETRIES = 5


CHEMBL_ID_LOOKUP_SCHEMA = pa.schema(
    [
        ("chembl_id", pa.string()),
        ("entity_type", pa.string()),
        ("last_active", pa.int64()),
        ("resource_url", pa.string()),
        ("status", pa.string()),
    ]
)


MOLECULE_DICTIONARY_SCHEMA = pa.schema(
    [
        ("molecule_chembl_id", pa.string()),
        ("molecule_type", pa.string()),
        ("pref_name", pa.string()),
        ("max_phase", pa.float64()),
        ("first_approval", pa.int64()),
        ("structure_type", pa.string()),
        ("natural_product", pa.int64()),
        ("therapeutic_flag", pa.bool_()),
        ("oral", pa.bool_()),
        ("parenteral", pa.bool_()),
        ("topical", pa.bool_()),
        ("black_box_warning", pa.int64()),
        ("chemical_probe", pa.int64()),
        ("chirality", pa.int64()),
        ("dosed_ingredient", pa.bool_()),
        ("first_in_class", pa.int64()),
        ("inorganic_flag", pa.int64()),
        ("orphan", pa.int64()),
        ("polymer_flag", pa.int64()),
        ("prodrug", pa.int64()),
        ("veterinary", pa.int64()),
        ("withdrawn_flag", pa.bool_()),
    ]
)


COMPOUND_PROPERTIES_SCHEMA = pa.schema(
    [
        ("molecule_chembl_id", pa.string()),
        ("alogp", pa.string()),
        ("aromatic_rings", pa.int64()),
        ("full_molformula", pa.string()),
        ("full_mwt", pa.string()),
        ("hba", pa.int64()),
        ("hbd", pa.int64()),
        ("heavy_atoms", pa.int64()),
        ("mw_freebase", pa.string()),
        ("np_likeness_score", pa.string()),
        ("num_ro5_violations", pa.int64()),
        ("psa", pa.string()),
        ("qed_weighted", pa.string()),
        ("ro3_pass", pa.string()),
        ("rtb", pa.int64()),
        ("cx_logp", pa.string()),
        ("molecular_species", pa.string()),
    ]
)


COMPOUND_STRUCTURES_SCHEMA = pa.schema(
    [
        ("molecule_chembl_id", pa.string()),
        ("canonical_smiles", pa.string()),
        ("molfile", pa.string()),
        ("standard_inchi", pa.string()),
        ("standard_inchi_key", pa.string()),
    ]
)


def _request_json(
    session: requests.Session,
    endpoint: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    url = f"{CHEMBL_API_BASE_URL}/{endpoint}.json"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                retry_after = int(
                    response.headers.get(
                        "Retry-After",
                        10,
                    )
                )

                logger.warning(
                    "ChEMBL rate limit reached. "
                    "Waiting %s seconds.",
                    retry_after,
                )

                time.sleep(retry_after)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise

            wait_seconds = 2**attempt

            logger.exception(
                "Request failed. Attempt %s/%s. "
                "Retrying in %s seconds.",
                attempt,
                MAX_RETRIES,
                wait_seconds,
            )

            time.sleep(wait_seconds)

    raise RuntimeError(
        "Unexpected ChEMBL request failure"
    )


def _iter_endpoint_pages(
    endpoint: str,
    records_key: str,
    max_records: int | None,
) -> Iterator[list[dict[str, Any]]]:
    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": (
                "quantori-final-project/"
                "maria-riabkova"
            )
        }
    )

    offset = 0
    processed = 0

    while True:
        if max_records is not None:
            remaining = max_records - processed

            if remaining <= 0:
                break

            limit = min(PAGE_SIZE, remaining)
        else:
            limit = PAGE_SIZE

        payload = _request_json(
            session=session,
            endpoint=endpoint,
            params={
                "limit": limit,
                "offset": offset,
            },
        )

        records = payload.get(
            records_key,
            [],
        )

        if not records:
            break

        yield records

        received = len(records)
        processed += received
        offset += received

        logger.info(
            "Endpoint=%s, received=%s, processed=%s",
            endpoint,
            received,
            processed,
        )

        if not payload.get(
            "page_meta",
            {},
        ).get("next"):
            break

        time.sleep(REQUEST_DELAY_SECONDS)


def _normalize_dictionary(
    molecule: dict[str, Any],
) -> dict[str, Any]:
    return {
        "molecule_chembl_id": molecule.get(
            "molecule_chembl_id"
        ),
        "molecule_type": molecule.get(
            "molecule_type"
        ),
        "pref_name": molecule.get(
            "pref_name"
        ),
        "max_phase": _to_float(
            molecule.get("max_phase")
        ),
        "first_approval": _to_int(
            molecule.get("first_approval")
        ),
        "structure_type": molecule.get(
            "structure_type"
        ),
        "natural_product": _to_int(
            molecule.get("natural_product")
        ),
        "therapeutic_flag": _to_bool(
            molecule.get("therapeutic_flag")
        ),
        "oral": _to_bool(
            molecule.get("oral")
        ),
        "parenteral": _to_bool(
            molecule.get("parenteral")
        ),
        "topical": _to_bool(
            molecule.get("topical")
        ),
        "black_box_warning": _to_int(
            molecule.get("black_box_warning")
        ),
        "chemical_probe": _to_int(
            molecule.get("chemical_probe")
        ),
        "chirality": _to_int(
            molecule.get("chirality")
        ),
        "dosed_ingredient": _to_bool(
            molecule.get("dosed_ingredient")
        ),
        "first_in_class": _to_int(
            molecule.get("first_in_class")
        ),
        "inorganic_flag": _to_int(
            molecule.get("inorganic_flag")
        ),
        "orphan": _to_int(
            molecule.get("orphan")
        ),
        "polymer_flag": _to_int(
            molecule.get("polymer_flag")
        ),
        "prodrug": _to_int(
            molecule.get("prodrug")
        ),
        "veterinary": _to_int(
            molecule.get("veterinary")
        ),
        "withdrawn_flag": _to_bool(
            molecule.get("withdrawn_flag")
        ),
    }


def _normalize_properties(
    molecule: dict[str, Any],
) -> dict[str, Any] | None:
    properties = molecule.get(
        "molecule_properties"
    )

    if not properties:
        return None

    row = {
        field.name: properties.get(field.name)
        for field in COMPOUND_PROPERTIES_SCHEMA
        if field.name != "molecule_chembl_id"
    }

    row["molecule_chembl_id"] = molecule.get(
        "molecule_chembl_id"
    )

    return row


def _normalize_structures(
    molecule: dict[str, Any],
) -> dict[str, Any] | None:
    structures = molecule.get(
        "molecule_structures"
    )

    if not structures:
        return None

    row = {
        field.name: structures.get(field.name)
        for field in COMPOUND_STRUCTURES_SCHEMA
        if field.name != "molecule_chembl_id"
    }

    row["molecule_chembl_id"] = molecule.get(
        "molecule_chembl_id"
    )

    return row


def _write_rows(
    writer: pq.ParquetWriter,
    rows: list[dict[str, Any]],
    schema: pa.Schema,
) -> int:
    if not rows:
        return 0

    table = pa.Table.from_pylist(
        rows,
        schema=schema,
    )

    writer.write_table(table)

    return table.num_rows


def export_molecule_tables(
    max_records: int = 1000,
) -> list[str]:
    """Export three molecule datasets to Parquet."""

    record_limit = (
        None
        if int(max_records) == 0
        else int(max_records)
    )

    LOCAL_RAW_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    dictionary_path = (
        LOCAL_RAW_DIR
        / "molecule_dictionary.parquet"
    )
    properties_path = (
        LOCAL_RAW_DIR
        / "compound_properties.parquet"
    )
    structures_path = (
        LOCAL_RAW_DIR
        / "compound_structures.parquet"
    )

    for path in (
        dictionary_path,
        properties_path,
        structures_path,
    ):
        path.unlink(
            missing_ok=True,
        )

    counts = {
        "dictionary": 0,
        "properties": 0,
        "structures": 0,
    }

    with (
        pq.ParquetWriter(
            dictionary_path,
            MOLECULE_DICTIONARY_SCHEMA,
            compression="snappy",
        ) as dictionary_writer,
        pq.ParquetWriter(
            properties_path,
            COMPOUND_PROPERTIES_SCHEMA,
            compression="snappy",
        ) as properties_writer,
        pq.ParquetWriter(
            structures_path,
            COMPOUND_STRUCTURES_SCHEMA,
            compression="snappy",
        ) as structures_writer,
    ):
        for molecules in _iter_endpoint_pages(
            endpoint="molecule",
            records_key="molecules",
            max_records=record_limit,
        ):
            dictionary_rows = [
                _normalize_dictionary(molecule)
                for molecule in molecules
            ]

            properties_rows = [
                row
                for molecule in molecules
                if (
                    row := _normalize_properties(
                        molecule
                    )
                )
                is not None
            ]

            structures_rows = [
                row
                for molecule in molecules
                if (
                    row := _normalize_structures(
                        molecule
                    )
                )
                is not None
            ]

            counts["dictionary"] += _write_rows(
                dictionary_writer,
                dictionary_rows,
                MOLECULE_DICTIONARY_SCHEMA,
            )

            counts["properties"] += _write_rows(
                properties_writer,
                properties_rows,
                COMPOUND_PROPERTIES_SCHEMA,
            )

            counts["structures"] += _write_rows(
                structures_writer,
                structures_rows,
                COMPOUND_STRUCTURES_SCHEMA,
            )

    logger.info(
        "Molecule export completed: %s",
        counts,
    )

    return [
        str(dictionary_path),
        str(properties_path),
        str(structures_path),
    ]


def export_chembl_id_lookup(
    max_records: int = 1000,
) -> str:
    """Export chembl_id_lookup to Parquet."""

    record_limit = (
        None
        if int(max_records) == 0
        else int(max_records)
    )

    LOCAL_RAW_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        LOCAL_RAW_DIR
        / "chembl_id_lookup.parquet"
    )

    output_path.unlink(
        missing_ok=True,
    )

    total_rows = 0

    with pq.ParquetWriter(
        output_path,
        CHEMBL_ID_LOOKUP_SCHEMA,
        compression="snappy",
    ) as writer:
        for records in _iter_endpoint_pages(
            endpoint="chembl_id_lookup",
            records_key="chembl_id_lookups",
            max_records=record_limit,
        ):
            rows = [
                {
                    field.name: record.get(
                        field.name
                    )
                    for field in CHEMBL_ID_LOOKUP_SCHEMA
                }
                for record in records
            ]

            total_rows += _write_rows(
                writer,
                rows,
                CHEMBL_ID_LOOKUP_SCHEMA,
            )

    logger.info(
        "chembl_id_lookup export completed: %s rows",
        total_rows,
    )

    return str(output_path)


def upload_raw_files_to_s3() -> list[str]:
    """Upload four Parquet files to S3 raw."""

    uploaded_keys: list[str] = []

    for file_name in RAW_FILE_NAMES:
        local_path = LOCAL_RAW_DIR / file_name
        s3_key = f"{S3_RAW_PREFIX}/{file_name}"

        upload_file(
            local_path=local_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            aws_conn_id=AWS_CONN_ID,
            replace=True,
        )

        uploaded_keys.append(s3_key)

        logger.info(
            "Uploaded %s to s3://%s/%s",
            local_path,
            S3_BUCKET_NAME,
            s3_key,
        )

    return uploaded_keys


def validate_raw_files_in_s3() -> None:
    """Check that all expected files exist in S3."""

    missing_keys: list[str] = []

    for file_name in RAW_FILE_NAMES:
        s3_key = f"{S3_RAW_PREFIX}/{file_name}"

        if not object_exists(
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            aws_conn_id=AWS_CONN_ID,
        ):
            missing_keys.append(s3_key)

    if missing_keys:
        raise RuntimeError(
            "Missing S3 raw files: "
            + ", ".join(missing_keys)
        )

    logger.info(
        "All expected raw files exist in S3"
    )
def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None

    return float(value)


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None

    return int(float(value))


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if value in (1, "1", "true", "True"):
        return True

    if value in (0, "0", "false", "False"):
        return False

    raise ValueError(
        f"Cannot convert value to bool: {value!r}"
    )
