DELETE FROM ods.compound_structures
WHERE chembl_release = %(chembl_release)s;

INSERT INTO ods.compound_structures (
    chembl_release,
    molecule_chembl_id,
    canonical_smiles,
    standard_inchi,
    standard_inchi_key
)
SELECT
    %(chembl_release)s,
    btrim(source.molecule_chembl_id),
    NULLIF(btrim(source.canonical_smiles), ''),
    NULLIF(btrim(source.standard_inchi), ''),
    NULLIF(btrim(source.standard_inchi_key), '')
FROM stg_compound_structures AS source
JOIN ods.chembl_compounds_active AS active
  ON active.chembl_release = %(chembl_release)s
 AND active.molecule_chembl_id =
     btrim(source.molecule_chembl_id);
