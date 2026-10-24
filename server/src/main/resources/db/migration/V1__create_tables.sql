CREATE TABLE IF NOT EXISTS patients (
    subject_id BIGINT PRIMARY KEY,
    gender TEXT,
    anchor_age INTEGER,
    anchor_year INTEGER,
    anchor_year_group TEXT,
    dod DATE
);

CREATE TABLE IF NOT EXISTS admissions (
    hadm_id BIGINT PRIMARY KEY,
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    admittime TIMESTAMP,
    dischtime TIMESTAMP,
    deathtime TIMESTAMP,
    admission_type TEXT,
    admit_provider_id TEXT,
    admission_location TEXT,
    discharge_location TEXT,
    insurance TEXT,
    language TEXT,
    marital_status TEXT,
    race TEXT,
    edregtime TIMESTAMP,
    edouttime TIMESTAMP,
    hospital_expire_flag SMALLINT
    CHECK (hospital_expire_flag IS NULL OR hospital_expire_flag IN (0, 1))
);

CREATE TABLE IF NOT EXISTS icustays (
    stay_id BIGINT PRIMARY KEY,
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT NOT NULL REFERENCES admissions(hadm_id),
    bed_id INTEGER,
    first_careunit TEXT,
    last_careunit TEXT,
    intime TIMESTAMP,
    outtime TIMESTAMP,
    los DOUBLE PRECISION CHECK (los IS NULL OR los >= 0)
);

CREATE TABLE IF NOT EXISTS d_items (
    itemid INTEGER PRIMARY KEY,
    label TEXT,
    abbreviation TEXT,
    linksto TEXT,
    category TEXT,
    unitname TEXT,
    param_type TEXT,
    lownormalvalue DOUBLE PRECISION,
    highnormalvalue DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS d_labitems (
    itemid INTEGER PRIMARY KEY,
    label TEXT,
    fluid TEXT,
    category TEXT
);