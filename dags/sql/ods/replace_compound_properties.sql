DELETE FROM ods.compound_properties
WHERE chembl_release = %(chembl_release)s;

INSERT INTO ods.compound_properties (
    chembl_release,
    molecule_chembl_id,
    alogp,
    aromatic_rings,
    full_molformula,
    full_mwt,
    hba,
    hbd,
    heavy_atoms,
    mw_freebase,
    np_likeness_score,
    num_ro5_violations,
    psa,
    qed_weighted,
    ro3_pass,
    rtb,
    cx_logp,
    molecular_species
)
SELECT
    %(chembl_release)s,
    btrim(source.molecule_chembl_id),
    NULLIF(btrim(source.alogp), '')::double precision,
    NULLIF(btrim(source.aromatic_rings), '')::integer,
    NULLIF(btrim(source.full_molformula), ''),
    NULLIF(btrim(source.full_mwt), '')::double precision,
    NULLIF(btrim(source.hba), '')::integer,
    NULLIF(btrim(source.hbd), '')::integer,
    NULLIF(btrim(source.heavy_atoms), '')::integer,
    NULLIF(btrim(source.mw_freebase), '')::double precision,
    NULLIF(btrim(source.np_likeness_score), '')::double precision,
    NULLIF(btrim(source.num_ro5_violations), '')::integer,
    NULLIF(btrim(source.psa), '')::double precision,
    NULLIF(btrim(source.qed_weighted), '')::double precision,
    NULLIF(btrim(source.ro3_pass), ''),
    NULLIF(btrim(source.rtb), '')::integer,
    NULLIF(btrim(source.cx_logp), '')::double precision,
    NULLIF(btrim(source.molecular_species), '')
FROM stg_compound_properties AS source
JOIN ods.chembl_compounds_active AS active
  ON active.chembl_release = %(chembl_release)s
 AND active.molecule_chembl_id =
     btrim(source.molecule_chembl_id);
