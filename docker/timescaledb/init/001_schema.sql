-- Idempotent local schema. Source reference clocks are deidentified and have
-- no stated timezone; their columns intentionally use timestamp without zone.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS patients (
    subject_id BIGINT PRIMARY KEY,
    demo_id TEXT NOT NULL UNIQUE,
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
    admission_type TEXT,
    admission_location TEXT,
    discharge_location TEXT
);
CREATE TABLE IF NOT EXISTS icustays (
    stay_id BIGINT PRIMARY KEY,
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT NOT NULL REFERENCES admissions(hadm_id),
    bed_id TEXT NOT NULL UNIQUE,
    first_careunit TEXT,
    last_careunit TEXT,
    intime TIMESTAMP,
    outtime TIMESTAMP,
    los DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS d_items (
    itemid INTEGER PRIMARY KEY,
    label TEXT,
    abbreviation TEXT,
    category TEXT,
    unitname TEXT,
    param_type TEXT
);
CREATE TABLE IF NOT EXISTS d_labitems (
    itemid INTEGER PRIMARY KEY,
    label TEXT,
    fluid TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXISTS chartevents (
    event_time TIMESTAMPTZ NOT NULL,
    event_id TEXT NOT NULL,
    schema_version TEXT NOT NULL CHECK (schema_version = '2.0'),
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT NOT NULL REFERENCES admissions(hadm_id),
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),
    bed_id TEXT NOT NULL,
    itemid INTEGER NOT NULL REFERENCES d_items(itemid),
    signal_name TEXT NOT NULL,
    value TEXT,
    valuenum DOUBLE PRECISION NOT NULL,
    valueuom TEXT,
    quality_status TEXT NOT NULL CHECK (quality_status = 'VALID'),
    ordering_status TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_time, event_id)
);
CREATE TABLE IF NOT EXISTS labevents (
    event_time TIMESTAMPTZ NOT NULL,
    event_id TEXT NOT NULL,
    schema_version TEXT NOT NULL CHECK (schema_version = '2.0'),
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT NOT NULL REFERENCES admissions(hadm_id),
    mapped_stay_id BIGINT REFERENCES icustays(stay_id),
    bed_id TEXT,
    itemid INTEGER NOT NULL REFERENCES d_labitems(itemid),
    lab_name TEXT NOT NULL,
    value TEXT,
    valuenum DOUBLE PRECISION,
    valueuom TEXT,
    quality_status TEXT NOT NULL CHECK (quality_status = 'VALID'),
    ordering_status TEXT,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_time, event_id)
);
CREATE TABLE IF NOT EXISTS vitals_15m (
    window_end TIMESTAMPTZ NOT NULL,
    aggregate_id TEXT NOT NULL,
    schema_version TEXT NOT NULL CHECK (schema_version = '2.0'),
    window_start TIMESTAMPTZ NOT NULL,
    window_minutes SMALLINT NOT NULL CHECK (window_minutes = 15),
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT NOT NULL REFERENCES admissions(hadm_id),
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),
    bed_id TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    count BIGINT NOT NULL CHECK (count > 0),
    mean DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    variance DOUBLE PRECISION,
    emitted_at TIMESTAMPTZ,
    PRIMARY KEY (window_end, aggregate_id)
);
CREATE TABLE IF NOT EXISTS alerts (
    trigger_time TIMESTAMPTZ NOT NULL,
    alert_id TEXT NOT NULL,
    schema_version TEXT NOT NULL CHECK (schema_version = '2.0'),
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT NOT NULL REFERENCES admissions(hadm_id),
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),
    bed_id TEXT NOT NULL,
    alert_type TEXT NOT NULL CHECK (alert_type = 'SUSTAINED_HYPOTENSION'),
    severity TEXT NOT NULL,
    signal_name TEXT NOT NULL CHECK (signal_name = 'MAP'),
    trigger_value DOUBLE PRECISION NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    episode_start TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    alert_emitted_at TIMESTAMPTZ,
    PRIMARY KEY (trigger_time, alert_id)
);
