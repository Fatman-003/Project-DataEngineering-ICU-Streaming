SELECT create_hypertable('patient_telemetry_stream', by_range('sample_time', INTERVAL '1 hour'), if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('vitals_sliding_window_5m', by_range('window_end', INTERVAL '1 day'), if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('sepsis_alert_events', by_range('alert_time', INTERVAL '1 day'), if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('waveform_segments', by_range('segment_start', INTERVAL '1 day'), if_not_exists => TRUE, create_default_indexes => FALSE);
