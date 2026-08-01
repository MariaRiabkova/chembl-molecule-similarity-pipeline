INSERT INTO ods.chembl_releases (
    chembl_release,
    status,
    load_scope,
    max_records,
    source_s3_prefix,
    loaded_at
)
VALUES (
    %(chembl_release)s,
    'loading',
    %(load_scope)s,
    %(max_records)s,
    %(source_s3_prefix)s,
    now()
)
ON CONFLICT (chembl_release)
DO UPDATE SET
    status = 'loading',
    load_scope = EXCLUDED.load_scope,
    max_records = EXCLUDED.max_records,
    source_s3_prefix = EXCLUDED.source_s3_prefix,
    loaded_at = now();
