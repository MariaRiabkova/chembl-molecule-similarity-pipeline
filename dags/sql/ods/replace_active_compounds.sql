DELETE FROM ods.chembl_compounds_active
WHERE chembl_release = %(chembl_release)s;

INSERT INTO ods.chembl_compounds_active (
    chembl_release,
    molecule_chembl_id,
    last_active
)
SELECT
    %(chembl_release)s,
    btrim(chembl_id),
    NULLIF(btrim(last_active), '')::integer
FROM stg_chembl_id_lookup
WHERE upper(btrim(entity_type)) = 'COMPOUND'
  AND upper(btrim(status)) = 'ACTIVE'
  AND NULLIF(btrim(chembl_id), '') IS NOT NULL;
