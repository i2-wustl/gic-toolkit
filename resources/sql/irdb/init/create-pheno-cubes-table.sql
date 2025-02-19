CREATE TABLE IF NOT EXISTS pheno_cubes (
    id INTEGER DEFAULT nextval('pheno_cubes_sequence'),
    concept_path TEXT UNIQUE,
    partition INTEGER,
    is_alpha BOOLEAN,
    observation_count INTEGER,
    column_width INTEGER,
    loading_map BLOB
);
