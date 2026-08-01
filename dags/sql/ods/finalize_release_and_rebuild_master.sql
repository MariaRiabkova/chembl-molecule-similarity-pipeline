UPDATE ods.chembl_releases
SET
    status = 'complete',
    loaded_at = now()
WHERE chembl_release = %(chembl_release)s;

CREATE TEMP TABLE stg_chembl_compounds_master
ON COMMIT DROP
AS
WITH latest_release AS (
    SELECT MAX(chembl_release) AS chembl_release
    FROM ods.chembl_releases
    WHERE status = 'complete'
)
SELECT
    active.molecule_chembl_id,
    latest.chembl_release AS base_release,

    dictionary.molecule_type,
    structures.canonical_smiles,
    structures.standard_inchi_key,

    properties.mw_freebase,
    properties.alogp,
    properties.psa,
    properties.full_mwt,
    properties.aromatic_rings,
    properties.heavy_atoms,

    cx_history.cx_logp,
    cx_history.chembl_release AS cx_logp_source_release,

    species_history.molecular_species,
    species_history.chembl_release
        AS molecular_species_source_release,

    now() AS refreshed_at

FROM latest_release AS latest

JOIN ods.chembl_compounds_active AS active
  ON active.chembl_release = latest.chembl_release

LEFT JOIN ods.molecule_dictionary AS dictionary
  ON dictionary.chembl_release = active.chembl_release
 AND dictionary.molecule_chembl_id =
     active.molecule_chembl_id

LEFT JOIN ods.compound_structures AS structures
  ON structures.chembl_release = active.chembl_release
 AND structures.molecule_chembl_id =
     active.molecule_chembl_id

LEFT JOIN ods.compound_properties AS properties
  ON properties.chembl_release = active.chembl_release
 AND properties.molecule_chembl_id =
     active.molecule_chembl_id

LEFT JOIN LATERAL (
    SELECT
        history.chembl_release,
        history.cx_logp
    FROM ods.compound_properties AS history
    JOIN ods.chembl_releases AS history_release
      ON history_release.chembl_release =
         history.chembl_release
     AND history_release.status = 'complete'
    WHERE history.molecule_chembl_id =
          active.molecule_chembl_id
      AND history.chembl_release <= latest.chembl_release
      AND history.cx_logp IS NOT NULL
    ORDER BY history.chembl_release DESC
    LIMIT 1
) AS cx_history
    ON true

LEFT JOIN LATERAL (
    SELECT
        history.chembl_release,
        history.molecular_species
    FROM ods.compound_properties AS history
    JOIN ods.chembl_releases AS history_release
      ON history_release.chembl_release =
         history.chembl_release
     AND history_release.status = 'complete'
    WHERE history.molecule_chembl_id =
          active.molecule_chembl_id
      AND history.chembl_release <= latest.chembl_release
      AND history.molecular_species IS NOT NULL
      AND btrim(history.molecular_species) <> ''
    ORDER BY history.chembl_release DESC
    LIMIT 1
) AS species_history
    ON true;

TRUNCATE TABLE ods.chembl_compounds_master;

INSERT INTO ods.chembl_compounds_master (
    molecule_chembl_id,
    base_release,
    molecule_type,
    canonical_smiles,
    standard_inchi_key,
    mw_freebase,
    alogp,
    psa,
    full_mwt,
    aromatic_rings,
    heavy_atoms,
    cx_logp,
    cx_logp_source_release,
    molecular_species,
    molecular_species_source_release,
    refreshed_at
)
SELECT
    molecule_chembl_id,
    base_release,
    molecule_type,
    canonical_smiles,
    standard_inchi_key,
    mw_freebase,
    alogp,
    psa,
    full_mwt,
    aromatic_rings,
    heavy_atoms,
    cx_logp,
    cx_logp_source_release,
    molecular_species,
    molecular_species_source_release,
    refreshed_at
FROM stg_chembl_compounds_master;
