CREATE INDEX IF NOT EXISTS chartevents_stay_time_idx ON chartevents (stay_id, event_time DESC);
CREATE INDEX IF NOT EXISTS chartevents_item_time_idx ON chartevents (itemid, event_time DESC);
CREATE INDEX IF NOT EXISTS chartevents_subject_time_idx ON chartevents (subject_id, event_time DESC);
CREATE INDEX IF NOT EXISTS labevents_stay_time_idx ON labevents (mapped_stay_id, event_time DESC);
CREATE INDEX IF NOT EXISTS labevents_subject_time_idx ON labevents (subject_id, event_time DESC);
CREATE INDEX IF NOT EXISTS labevents_item_time_idx ON labevents (itemid, event_time DESC);
CREATE INDEX IF NOT EXISTS vitals_15m_stay_signal_time_idx ON vitals_15m (stay_id, signal_name, window_end DESC);
CREATE INDEX IF NOT EXISTS alerts_stay_time_idx ON alerts (stay_id, trigger_time DESC);
CREATE INDEX IF NOT EXISTS alerts_type_time_idx ON alerts (alert_type, trigger_time DESC);
