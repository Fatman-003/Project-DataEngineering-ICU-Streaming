CREATE INDEX IF NOT EXISTS idx_admissions_subject
    ON admissions (subject_id);

CREATE INDEX IF NOT EXISTS idx_icustays_subject
    ON icustays (subject_id);

CREATE INDEX IF NOT EXISTS idx_icustays_hadm
    ON icustays (hadm_id);

CREATE INDEX IF NOT EXISTS idx_icustays_bed
    ON icustays (bed_id);