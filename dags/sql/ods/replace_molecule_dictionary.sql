DELETE FROM ods.molecule_dictionary
WHERE chembl_release = %(chembl_release)s;

INSERT INTO ods.molecule_dictionary (
    chembl_release,
    molecule_chembl_id,
    molecule_type,
    pref_name,
    max_phase,
    first_approval,
    structure_type,
    natural_product,
    therapeutic_flag,
    oral,
    parenteral,
    topical,
    black_box_warning,
    chemical_probe,
    chirality,
    dosed_ingredient,
    first_in_class,
    inorganic_flag,
    orphan,
    polymer_flag,
    prodrug,
    veterinary,
    withdrawn_flag
)
SELECT
    %(chembl_release)s,
    btrim(source.molecule_chembl_id),
    NULLIF(btrim(source.molecule_type), ''),
    NULLIF(btrim(source.pref_name), ''),
    NULLIF(btrim(source.max_phase), '')::double precision,
    NULLIF(btrim(source.first_approval), '')::integer,
    NULLIF(btrim(source.structure_type), ''),
    NULLIF(btrim(source.natural_product), '')::integer,
    NULLIF(btrim(source.therapeutic_flag), '')::boolean,
    NULLIF(btrim(source.oral), '')::boolean,
    NULLIF(btrim(source.parenteral), '')::boolean,
    NULLIF(btrim(source.topical), '')::boolean,
    NULLIF(btrim(source.black_box_warning), '')::integer,
    NULLIF(btrim(source.chemical_probe), '')::integer,
    NULLIF(btrim(source.chirality), '')::integer,
    NULLIF(btrim(source.dosed_ingredient), '')::boolean,
    NULLIF(btrim(source.first_in_class), '')::integer,
    NULLIF(btrim(source.inorganic_flag), '')::integer,
    NULLIF(btrim(source.orphan), '')::integer,
    NULLIF(btrim(source.polymer_flag), '')::integer,
    NULLIF(btrim(source.prodrug), '')::integer,
    NULLIF(btrim(source.veterinary), '')::integer,
    NULLIF(btrim(source.withdrawn_flag), '')::boolean
FROM stg_molecule_dictionary AS source
JOIN ods.chembl_compounds_active AS active
  ON active.chembl_release = %(chembl_release)s
 AND active.molecule_chembl_id =
     btrim(source.molecule_chembl_id);
