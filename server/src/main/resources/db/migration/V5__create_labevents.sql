-- Lab source table (legacy MIMIC-IV shape): deidentified clock, no streaming writes.
CREATE TABLE IF NOT EXISTS labevents (
    subject_id BIGINT NOT NULL REFERENCES patients(subject_id),
    hadm_id BIGINT REFERENCES admissions(hadm_id),
    charttime TIMESTAMP NOT NULL,
    itemid INTEGER NOT NULL REFERENCES d_labitems(itemid),
    value TEXT,
    valuenum DOUBLE PRECISION,
    valueuom TEXT,
    flags TEXT
);

CREATE INDEX IF NOT EXISTS idx_labevents_hadm_time
    ON labevents (hadm_id, charttime DESC);

CREATE INDEX IF NOT EXISTS idx_labevents_item_time
    ON labevents (itemid, charttime DESC);
