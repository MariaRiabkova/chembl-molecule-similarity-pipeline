-- ChEMBL ODS schema
--
-- Versioned tables preserve release history.
-- chembl_compounds_master is a cross-release golden record:
--   * molecule population comes from the newest complete release;
--   * the main record comes from that same release;
--   * only cx_logp and molecular_species may be backfilled from an
--     older complete release.

CREATE SCHEMA IF NOT EXISTS ods;


CREATE TABLE IF NOT EXISTS ods.chembl_releases (
    chembl_release   integer PRIMARY KEY,
    status           text NOT NULL,
    load_scope       text NOT NULL,
    max_records      integer NOT NULL,
    source_s3_prefix text NOT NULL,
    loaded_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT chk_chembl_releases_release
        CHECK (chembl_release > 0),

    CONSTRAINT chk_chembl_releases_status
        CHECK (status IN ('loading', 'complete', 'failed')),

    CONSTRAINT chk_chembl_releases_scope
        CHECK (load_scope IN ('sample', 'full')),

    CONSTRAINT chk_chembl_releases_max_records
        CHECK (max_records >= 0)
);


CREATE TABLE IF NOT EXISTS ods.chembl_compounds_active (
    chembl_release       integer NOT NULL,
    molecule_chembl_id   text NOT NULL,
    last_active          integer,
    loaded_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pk_chembl_compounds_active
        PRIMARY KEY (
            chembl_release,
            molecule_chembl_id
        ),

    CONSTRAINT fk_chembl_compounds_active_release
        FOREIGN KEY (chembl_release)
        REFERENCES ods.chembl_releases (chembl_release)
        ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS ods.molecule_dictionary (
    chembl_release       integer NOT NULL,
    molecule_chembl_id   text NOT NULL,

    molecule_type        text,
    pref_name            text,
    max_phase            double precision,
    first_approval       integer,
    structure_type       text,
    natural_product      integer,
    therapeutic_flag     boolean,
    oral                 boolean,
    parenteral           boolean,
    topical              boolean,
    black_box_warning    integer,
    chemical_probe       integer,
    chirality            integer,
    dosed_ingredient     boolean,
    first_in_class       integer,
    inorganic_flag       integer,
    orphan               integer,
    polymer_flag         integer,
    prodrug              integer,
    veterinary           integer,
    withdrawn_flag       boolean,

    loaded_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pk_molecule_dictionary
        PRIMARY KEY (
            chembl_release,
            molecule_chembl_id
        ),

    CONSTRAINT fk_molecule_dictionary_active_compound
        FOREIGN KEY (
            chembl_release,
            molecule_chembl_id
        )
        REFERENCES ods.chembl_compounds_active (
            chembl_release,
            molecule_chembl_id
        )
        ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS ods.compound_properties (
    chembl_release       integer NOT NULL,
    molecule_chembl_id   text NOT NULL,

    alogp                double precision,
    aromatic_rings       integer,
    full_molformula      text,
    full_mwt             double precision,
    hba                  integer,
    hbd                  integer,
    heavy_atoms          integer,
    mw_freebase          double precision,
    np_likeness_score    double precision,
    num_ro5_violations   integer,
    psa                  double precision,
    qed_weighted         double precision,
    ro3_pass             text,
    rtb                  integer,
    cx_logp              double precision,
    molecular_species    text,

    loaded_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pk_compound_properties
        PRIMARY KEY (
            chembl_release,
            molecule_chembl_id
        ),

    CONSTRAINT fk_compound_properties_active_compound
        FOREIGN KEY (
            chembl_release,
            molecule_chembl_id
        )
        REFERENCES ods.chembl_compounds_active (
            chembl_release,
            molecule_chembl_id
        )
        ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS ods.compound_structures (
    chembl_release       integer NOT NULL,
    molecule_chembl_id   text NOT NULL,

    canonical_smiles     text,
    standard_inchi       text,
    standard_inchi_key   text,

    loaded_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pk_compound_structures
        PRIMARY KEY (
            chembl_release,
            molecule_chembl_id
        ),

    CONSTRAINT fk_compound_structures_active_compound
        FOREIGN KEY (
            chembl_release,
            molecule_chembl_id
        )
        REFERENCES ods.chembl_compounds_active (
            chembl_release,
            molecule_chembl_id
        )
        ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS ods.chembl_compounds_master (
    molecule_chembl_id                 text PRIMARY KEY,
    base_release                       integer NOT NULL,

    molecule_type                      text,
    canonical_smiles                   text,
    standard_inchi_key                 text,

    mw_freebase                        double precision,
    alogp                              double precision,
    psa                                double precision,
    full_mwt                           double precision,
    aromatic_rings                     integer,
    heavy_atoms                        integer,

    cx_logp                            double precision,
    cx_logp_source_release             integer,

    molecular_species                  text,
    molecular_species_source_release   integer,

    refreshed_at                       timestamptz NOT NULL DEFAULT now()
);


CREATE INDEX IF NOT EXISTS idx_compound_structures_fingerprint_input
    ON ods.compound_structures (
        chembl_release,
        molecule_chembl_id
    )
    WHERE canonical_smiles IS NOT NULL
      AND btrim(canonical_smiles) <> '';


CREATE INDEX IF NOT EXISTS idx_compound_structures_inchi_key
    ON ods.compound_structures (
        chembl_release,
        standard_inchi_key
    )
    WHERE standard_inchi_key IS NOT NULL
      AND btrim(standard_inchi_key) <> '';


CREATE INDEX IF NOT EXISTS idx_compounds_master_base_release
    ON ods.chembl_compounds_master (base_release);
