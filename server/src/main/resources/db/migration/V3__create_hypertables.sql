CREATE TABLE IF NOT EXISTS patient_telemetry_stream (
    sample_time TIMESTAMPTZ NOT NULL,
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),

    heart_rate DOUBLE PRECISION
    CHECK (heart_rate IS NULL OR heart_rate BETWEEN 0 AND 350),
    map DOUBLE PRECISION
    CHECK (map IS NULL OR map BETWEEN 0 AND 300),
    sbp DOUBLE PRECISION
    CHECK (sbp IS NULL OR sbp BETWEEN 0 AND 350),
    spo2 DOUBLE PRECISION
    CHECK (spo2 IS NULL OR spo2 BETWEEN 0 AND 100),

    PRIMARY KEY (sample_time, stay_id)
);

CREATE TABLE IF NOT EXISTS vitals_sliding_window_5m (
    window_end TIMESTAMPTZ NOT NULL,
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),

    avg_heart_rate DOUBLE PRECISION,
    avg_map DOUBLE PRECISION,
    avg_sbp DOUBLE PRECISION,
    avg_spo2 DOUBLE PRECISION,

    PRIMARY KEY (window_end, stay_id)
);

-- Include variance for fresh schemas and preserve rows when upgrading local schemas.
ALTER TABLE vitals_sliding_window_5m
    ADD COLUMN IF NOT EXISTS var_heart_rate DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS var_map DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS var_sbp DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS var_spo2 DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS sepsis_alert_events (
    alert_time TIMESTAMPTZ NOT NULL,
    alert_id UUID NOT NULL,
    stay_id BIGINT NOT NULL REFERENCES icustays(stay_id),

    alert_type TEXT NOT NULL
    CHECK (alert_type IN (
           'HYPOTENSION_SUSTAINED',
           'QSOFA_HIGH_RISK',
           'SENSOR_DISCONNECTED'
    )),
    severity TEXT NOT NULL
    CHECK (severity IN ('WARNING', 'CRITICAL')),

    trigger_value DOUBLE PRECISION,
    trigger_message TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE'
    CHECK (status IN ('ACTIVE', 'ACKNOWLEDGED', 'FALSE_ALARM')),

    PRIMARY KEY (alert_time, alert_id)
);

SELECT create_hypertable(
    'patient_telemetry_stream',
    by_range('sample_time', INTERVAL '1 hour'),
    if_not_exists => TRUE,
    create_default_indexes => FALSE
);

SELECT create_hypertable(
    'vitals_sliding_window_5m',
    by_range('window_end', INTERVAL '1 day'),
    if_not_exists => TRUE,
    create_default_indexes => FALSE
);

SELECT create_hypertable(
    'sepsis_alert_events',
    by_range('alert_time', INTERVAL '1 day'),
    if_not_exists => TRUE,
    create_default_indexes => FALSE
);
