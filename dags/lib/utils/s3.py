"""Generic S3 helpers (work against any S3-compatible store, incl. MinIO)."""
from __future__ import annotations
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
from typing import TypedDict
from pathlib import Path


def upload_bytes(data, key, bucket_name, aws_conn_id, replace=True):
    S3Hook(aws_conn_id=aws_conn_id).load_bytes(
        data,
        key=key,
        bucket_name=bucket_name,
        replace=replace,
    )


def upload_file(
    local_path: str | Path,
    key: str,
    bucket_name: str,
    aws_conn_id: str,
    replace: bool = True,
) -> None:
    """Upload a local file to S3."""

    path = Path(local_path)

    if not path.is_file():
        raise FileNotFoundError(
            f"Local file does not exist: {path}"
        )

    S3Hook(
        aws_conn_id=aws_conn_id,
    ).load_file(
        filename=str(path),
        key=key,
        bucket_name=bucket_name,
        replace=replace,
    )


def download_object(key, bucket_name, aws_conn_id):
    return S3Hook(aws_conn_id=aws_conn_id).get_key(key, bucket_name=bucket_name).get()['Body'].read()


def object_exists(key, bucket_name, aws_conn_id):
    return S3Hook(aws_conn_id=aws_conn_id).check_for_key(key, bucket_name=bucket_name)


def list_object_keys(prefix, bucket_name, aws_conn_id):
    keys = S3Hook(
        aws_conn_id=aws_conn_id
    ).list_keys(
        bucket_name=bucket_name,
        prefix=prefix,
    )

    return keys or []

class S3ObjectMetadata(TypedDict):
    """Metadata required for S3 object discovery."""

    key: str
    last_modified: datetime


def list_objects(
    prefix: str,
    bucket_name: str,
    aws_conn_id: str,
) -> list[S3ObjectMetadata]:
    """List S3 object keys together with their last-modified timestamps."""
    hook = S3Hook(
        aws_conn_id=aws_conn_id
    )
    client = hook.get_conn()
    paginator = client.get_paginator(
        "list_objects_v2"
    )

    objects: list[S3ObjectMetadata] = []

    for page in paginator.paginate(
        Bucket=bucket_name,
        Prefix=prefix,
    ):
        for item in page.get("Contents", []):
            objects.append(
                {
                    "key": item["Key"],
                    "last_modified": item[
                        "LastModified"
                    ],
                }
            )

    return objects
