-- Idempotent local schema. Source reference clocks are deidentified and have
-- no stated timezone; their columns intentionally use timestamp without zone.
CREATE EXTENSION IF NOT EXISTS timescaledb;

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
    admission_type TEXT,
    admission_location TEXT,
    discharge_location TEXT
);
CREATE TABLE IF NOT EXISTS icustays (
    stay_id BIGINT PRIMARY KEY,
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT NOT NULL REFERENCES admissions(hadm_id),
    bed_id INTEGER NOT NULL UNIQUE,
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

-- Source placeholders: deidentified MIMIC clocks, no streaming writes.
CREATE TABLE IF NOT EXISTS chartevents (
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT NOT NULL REFERENCES admissions(hadm_id),
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),
    charttime TIMESTAMP NOT NULL,
    itemid INTEGER NOT NULL REFERENCES d_items(itemid),
    value TEXT, valuenum DOUBLE PRECISION, valueuom TEXT
);
CREATE TABLE IF NOT EXISTS labevents (
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT REFERENCES admissions(hadm_id),
    charttime TIMESTAMP NOT NULL,
    itemid INTEGER NOT NULL REFERENCES d_labitems(itemid),
    value TEXT, valuenum DOUBLE PRECISION, valueuom TEXT, flags TEXT
);
CREATE TABLE IF NOT EXISTS patient_telemetry_stream (
    sample_time TIMESTAMPTZ NOT NULL,
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),
    heart_rate DOUBLE PRECISION CHECK (heart_rate IS NULL OR heart_rate BETWEEN 0 AND 350),
    map DOUBLE PRECISION CHECK (map IS NULL OR map BETWEEN 0 AND 300),
    sbp DOUBLE PRECISION CHECK (sbp IS NULL OR sbp BETWEEN 0 AND 350),
    spo2 DOUBLE PRECISION CHECK (spo2 IS NULL OR spo2 BETWEEN 0 AND 100),
    PRIMARY KEY (sample_time, stay_id)
);
CREATE TABLE IF NOT EXISTS vitals_sliding_window_5m (
    window_end TIMESTAMPTZ NOT NULL,
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),
    avg_heart_rate DOUBLE PRECISION, avg_map DOUBLE PRECISION,
    avg_sbp DOUBLE PRECISION, avg_spo2 DOUBLE PRECISION,
    PRIMARY KEY (window_end, stay_id)
);
CREATE TABLE IF NOT EXISTS sepsis_alert_events (
    alert_time TIMESTAMPTZ NOT NULL,
    alert_id UUID NOT NULL,
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),
    alert_type TEXT NOT NULL CHECK (alert_type IN ('HYPOTENSION_SUSTAINED','QSOFA_HIGH_RISK','SENSOR_DISCONNECTED')),
    severity TEXT NOT NULL CHECK (severity IN ('WARNING','CRITICAL')),
    trigger_value DOUBLE PRECISION, trigger_message TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','ACKNOWLEDGED','FALSE_ALARM')),
    PRIMARY KEY (alert_time, alert_id)
);
-- Contract specifies metadata only; no waveform producer or writer.
CREATE TABLE IF NOT EXISTS waveform_segments (
    segment_start TIMESTAMPTZ NOT NULL,
    segment_id UUID NOT NULL,
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),
    PRIMARY KEY (segment_start, segment_id)
);
ALTER TABLE admissions ADD COLUMN IF NOT EXISTS deathtime TIMESTAMP;
ALTER TABLE admissions ADD COLUMN IF NOT EXISTS admit_provider_id TEXT;
ALTER TABLE admissions ADD COLUMN IF NOT EXISTS insurance TEXT;
ALTER TABLE admissions ADD COLUMN IF NOT EXISTS language TEXT;
ALTER TABLE admissions ADD COLUMN IF NOT EXISTS marital_status TEXT;
ALTER TABLE admissions ADD COLUMN IF NOT EXISTS race TEXT;
ALTER TABLE admissions ADD COLUMN IF NOT EXISTS edregtime TIMESTAMP;
ALTER TABLE admissions ADD COLUMN IF NOT EXISTS edouttime TIMESTAMP;
ALTER TABLE admissions ADD COLUMN IF NOT EXISTS hospital_expire_flag SMALLINT CHECK (hospital_expire_flag IN (0,1));
ALTER TABLE d_items ADD COLUMN IF NOT EXISTS linksto TEXT;
ALTER TABLE d_items ADD COLUMN IF NOT EXISTS lownormalvalue DOUBLE PRECISION;
ALTER TABLE d_items ADD COLUMN IF NOT EXISTS highnormalvalue DOUBLE PRECISION;
