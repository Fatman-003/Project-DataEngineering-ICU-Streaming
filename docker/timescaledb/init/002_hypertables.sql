-- All physical unique keys include their range partition column.
SELECT create_hypertable('chartevents', by_range('event_time'), if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('labevents', by_range('event_time'), if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('vitals_15m', by_range('window_end'), if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('alerts', by_range('trigger_time'), if_not_exists => TRUE, create_default_indexes => FALSE);
