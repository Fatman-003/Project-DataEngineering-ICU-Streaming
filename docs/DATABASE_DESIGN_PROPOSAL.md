# DATABASE DESIGN PROPOSAL — ICU TELEMETRY & BI

**Status:** Current database design proposal  
**Primary database:** TimescaleDB  
**Visualization:** Power BI / Tableau  
**Alternative storage benchmark:** Cassandra  
**Scope:** On-prem ICU telemetry Data Engineering project, designed to stay implementable by a single developer.

---

# 1. Design principle

The database uses a small dimensional model:

- **Dimensions** store descriptive/stable data.
- **Facts** store time-dependent measurements/events.
- **TimescaleDB hypertables** are used for time-series facts.
- **Continuous aggregates** are used for 5-minute and 15-minute trends.
- **SQL views** are used as BI-facing datasets.
- Raw MIMIC Clinical and Waveform files remain outside the runtime database.

The project does **not** create denormalized physical dashboard tables unless later benchmarks show they are necessary.

## Real-time detection responsibility

The main objective of the project is to detect dangerous patient conditions as early as possible. For that reason, **TimescaleDB and Power BI are not placed on the critical path of alert detection**.

```text
MIMIC Clinical / Waveform
           │
           ▼
     Replay Producer
           │
           ▼
  Kafka — RAW INPUT TOPICS
           │
           ▼
         Flink
 validate / dedup / event-time
 quality / windows / alert rules
           │
           ▼
 Kafka — OUTPUT TOPICS
     ┌─────┼──────────────┐
     │     │              │
     ▼     ▼              ▼
processed alerts         DLQ
 topics   topic     invalid/rejected
     │     │
     │     ├──────────────→ Alert Receiver
     │     │                 immediate warning
     │     │
     └─────┴──────────────→ Storage Consumer
                              │
                              ▼
                         TimescaleDB
                              │
                              ▼
                           Power BI
```

Responsibilities are separated as follows:

| Component | Primary responsibility |
|---|---|
| Kafka | Event transport/buffering for both raw input topics and processed/output topics |
| Flink | Validation, event-time processing, deduplication, late/out-of-order handling, windows, signal-quality logic, and early-warning rules |
| TimescaleDB | Persistence of processed telemetry, clinical context, alert history, time-range queries, and BI-serving queries; populated by an independent storage consumer |
| Power BI / Tableau | Visualization and reporting; not alert decision logic |

For example, a sustained hypotension rule such as `MAP < 65` for a defined window must be evaluated in Flink. The resulting alert can then be persisted to `fact_alert_event` and displayed in BI.

```text
Kafka raw event
    ↓
Flink state/window
    ↓
alert condition satisfied
    ↓
Kafka alert topic
    ├──→ Alert Receiver
    └──→ Storage Consumer → TimescaleDB

Clean processed events
    ↓
Kafka processed topics
    ↓
Storage Consumer → TimescaleDB
```

Therefore, database write latency must not be interpreted as the alert-detection latency. The storage benchmark between TimescaleDB and Cassandra evaluates persistence/query trade-offs. The main early-warning performance metric should be measured from event availability/producer time to the point where Flink emits the alert.

A slow database sink can still cause backpressure if sustained write throughput is insufficient, so sink throughput, Kafka lag, and Flink backpressure remain part of performance evaluation. However, the alert logic must not require a database query before producing an alert.

---

# 2. High-level schema

```text
                       dim_patient
                           │
                           ▼
                        dim_stay
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
     fact_vital_event fact_clinical  fact_alert_event
             │             ▲
             ▼             │
        dim_signal   dim_clinical_item

             │
             ▼
      continuous aggregates
        ├── vital_5m
        └── vital_15m

             │
             ▼
           SQL Views
        ├── vw_icu_overview
        ├── vw_alert_quality
        └── vw_shift_summary

             │
             ▼
         POWER BI
```

Waveform metadata is indexed separately:

```text
dim_stay
   │
   └── waveform_segment
          │
          └── points to raw WFDB .hea/.dat files
```

Raw ECG / ABP / Pleth samples are not duplicated into TimescaleDB.

---

# 3. Database objects

## Dimension tables

```text
dim_patient
dim_stay
dim_signal
dim_clinical_item
```

## Fact tables

```text
fact_vital_event
fact_clinical_event
fact_alert_event
```

## Derived time-series objects

```text
vital_5m
vital_15m
```

## BI-facing views

```text
vw_icu_overview
vw_alert_quality
vw_shift_summary
```

## Waveform metadata

```text
waveform_segment
```

---

# 4. `dim_patient`

Stores patient-level descriptive data.

```sql
CREATE TABLE dim_patient (
    patient_key         TEXT PRIMARY KEY,
    subject_id          BIGINT NOT NULL UNIQUE,

    gender              TEXT,
    approximate_age     INTEGER,

    dataset_type        TEXT NOT NULL,
    is_virtual          BOOLEAN NOT NULL DEFAULT FALSE
);
```

### Main fields

| Field | Meaning |
|---|---|
| `patient_key` | Project-level stable patient ID, e.g. `P001` |
| `subject_id` | Original MIMIC patient ID |
| `gender` | Patient gender if available |
| `approximate_age` | Approximate age derived from MIMIC anchors |
| `dataset_type` | `REFERENCE` or generated test dataset |
| `is_virtual` | Whether the patient is generated for testing |

Only project-relevant metadata is stored.

---

# 5. `dim_stay`

Stores ICU-stay metadata.

To keep the project simple, admission and ICU stay are not split into separate dimensions.

```sql
CREATE TABLE dim_stay (
    stay_key            BIGSERIAL PRIMARY KEY,

    stay_id             BIGINT NOT NULL UNIQUE,
    hadm_id             BIGINT,

    patient_key         TEXT NOT NULL
                        REFERENCES dim_patient(patient_key),

    care_unit           TEXT,
    bed_id              TEXT,

    icu_intime          TIMESTAMP NOT NULL,
    icu_outtime         TIMESTAMP NOT NULL,

    mapping_status      TEXT,
    mapping_reason      TEXT
);
```

Recommended indexes:

```sql
CREATE INDEX idx_dim_stay_patient
ON dim_stay (patient_key);

CREATE INDEX idx_dim_stay_interval
ON dim_stay (icu_intime, icu_outtime);
```

### Notes

- `stay_id` remains the main clinical identity.
- `hadm_id` preserves admission traceability.
- `bed_id` is project-simulated, not a real MIMIC bed identifier.
- `stay_key` is the database surrogate key referenced by facts.

---

# 6. `dim_signal`

Stores canonical vital-signal definitions.

```sql
CREATE TABLE dim_signal (
    signal_key          SMALLSERIAL PRIMARY KEY,

    signal_name         TEXT NOT NULL UNIQUE,
    category            TEXT,
    canonical_unit      TEXT
);
```

Initial values:

| Signal | Category | Unit |
|---|---|---|
| HR | CARDIAC | bpm |
| MAP | BLOOD_PRESSURE | mmHg |
| SBP | BLOOD_PRESSURE | mmHg |
| DBP | BLOOD_PRESSURE | mmHg |
| SPO2 | OXYGENATION | % |
| RR | RESPIRATORY | breaths/min |

This avoids repeating signal metadata in every fact row.

---

# 7. `dim_clinical_item`

Stores metadata for clinical measurements/events.

```sql
CREATE TABLE dim_clinical_item (
    clinical_item_key   BIGSERIAL PRIMARY KEY,

    source_item_id      BIGINT,
    item_name           TEXT NOT NULL,
    event_type          TEXT NOT NULL,

    canonical_unit      TEXT,
    source_table        TEXT,

    UNIQUE (
        source_table,
        source_item_id,
        item_name
    )
);
```

Examples:

| Source Item | Item | Event Type | Unit | Source |
|---:|---|---|---|---|
| 50912 | Creatinine | LAB | mg/dL | labevents |
| 50813 | Lactate | LAB | mmol/L | labevents |
| 220739 | GCS Eye Opening | GCS | score | chartevents |
| 226559 | Foley | URINE | mL | outputevents |

---

# 8. `fact_vital_event`

Primary time-series fact table for numeric ICU telemetry.

Signals include:

```text
HR
MAP
SBP
DBP
SPO2
RR
```

```sql
CREATE TABLE fact_vital_event (
    event_time           TIMESTAMP NOT NULL,

    event_id             TEXT NOT NULL,

    stay_key             BIGINT NOT NULL
                         REFERENCES dim_stay(stay_key),

    signal_key           SMALLINT NOT NULL
                         REFERENCES dim_signal(signal_key),

    value                DOUBLE PRECISION NOT NULL,
    quality_flag         TEXT,

    source_event_time    TIMESTAMP,
    producer_time        TIMESTAMPTZ,
    processed_time       TIMESTAMPTZ,

    sequence_number      BIGINT,
    run_id               TEXT,

    source_type          TEXT,
    source_table         TEXT,
    source_record_id     TEXT,

    dataset_type         TEXT NOT NULL,
    test_mode            TEXT,
    scenario_type        TEXT,

    is_virtual           BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (event_id, event_time)
);
```

Convert to hypertable:

```sql
SELECT create_hypertable(
    'fact_vital_event',
    by_range('event_time'),
    if_not_exists => TRUE
);
```

Recommended index:

```sql
CREATE INDEX idx_vital_stay_signal_time
ON fact_vital_event (
    stay_key,
    signal_key,
    event_time DESC
);
```

Typical historical query:

```sql
SELECT
    v.event_time,
    s.signal_name,
    v.value,
    s.canonical_unit,
    v.quality_flag
FROM fact_vital_event v
JOIN dim_signal s
  ON v.signal_key = s.signal_key
WHERE v.stay_key = ?
  AND v.event_time BETWEEN ? AND ?
ORDER BY v.event_time;
```

---

# 9. `fact_clinical_event`

Stores clinical context used mainly by BR-6.

Current event types:

```text
LAB
GCS
URINE
```

A future `VASOPRESSOR` type is added only if source data is actually verified.

```sql
CREATE TABLE fact_clinical_event (
    chart_time            TIMESTAMP NOT NULL,

    event_id              TEXT NOT NULL,

    stay_key              BIGINT NOT NULL
                          REFERENCES dim_stay(stay_key),

    clinical_item_key     BIGINT NOT NULL
                          REFERENCES dim_clinical_item(clinical_item_key),

    available_time        TIMESTAMP,

    value                 DOUBLE PRECISION,
    value_text            TEXT,

    availability_status   TEXT,
    validation_flag       TEXT,

    source_record_id      TEXT,
    run_id                TEXT,

    dataset_type          TEXT NOT NULL,
    test_mode             TEXT,
    scenario_type         TEXT,

    is_virtual            BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (event_id, chart_time)
);
```

Convert to hypertable:

```sql
SELECT create_hypertable(
    'fact_clinical_event',
    by_range('chart_time'),
    if_not_exists => TRUE
);
```

Recommended indexes:

```sql
CREATE INDEX idx_clinical_stay_time
ON fact_clinical_event (
    stay_key,
    chart_time DESC
);

CREATE INDEX idx_clinical_stay_item_time
ON fact_clinical_event (
    stay_key,
    clinical_item_key,
    chart_time DESC
);
```

### Availability rule

`chart_time` is when the observation/sample belongs clinically.

`available_time` is when the result becomes usable by the streaming system.

A lab result must never be treated as known before `available_time`.

If availability cannot be verified:

```text
available_time = NULL
```

The project must not silently replace it with `chart_time`.

---

# 10. `fact_alert_event`

Stores alerts generated by stream-processing rules.

Examples:

```text
SUSTAINED_HYPOTENSION
STALE_SIGNAL
SUSPECTED_ARTIFACT
```

```sql
CREATE TABLE fact_alert_event (
    trigger_time          TIMESTAMPTZ NOT NULL,

    alert_id              TEXT NOT NULL,

    stay_key              BIGINT NOT NULL
                          REFERENCES dim_stay(stay_key),

    signal_key            SMALLINT
                          REFERENCES dim_signal(signal_key),

    alert_type            TEXT NOT NULL,
    severity              TEXT NOT NULL,

    rule_name             TEXT,
    rule_version          TEXT,

    trigger_value         DOUBLE PRECISION,
    threshold_value       DOUBLE PRECISION,

    quality_status        TEXT,

    status                TEXT NOT NULL DEFAULT 'ACTIVE',

    acknowledged_at       TIMESTAMPTZ,
    acknowledged_by       TEXT,

    run_id                TEXT,
    dataset_type          TEXT,

    PRIMARY KEY (alert_id, trigger_time)
);
```

Convert to hypertable:

```sql
SELECT create_hypertable(
    'fact_alert_event',
    by_range('trigger_time'),
    if_not_exists => TRUE
);
```

Recommended indexes:

```sql
CREATE INDEX idx_alert_stay_time
ON fact_alert_event (
    stay_key,
    trigger_time DESC
);

CREATE INDEX idx_alert_status_time
ON fact_alert_event (
    status,
    trigger_time DESC
);
```

---

# 11. `waveform_segment`

Stores waveform metadata only.

It does **not** store every ECG, ABP or Pleth sample as a SQL row.

```sql
CREATE TABLE waveform_segment (
    segment_id            TEXT PRIMARY KEY,

    stay_key              BIGINT NOT NULL
                          REFERENCES dim_stay(stay_key),

    signal_name           TEXT NOT NULL,

    waveform_record_id    TEXT NOT NULL,

    start_time            TIMESTAMP NOT NULL,
    end_time              TIMESTAMP NOT NULL,

    sampling_rate_hz      DOUBLE PRECISION,
    unit                  TEXT,

    source_header_path    TEXT NOT NULL,
    source_data_path      TEXT,

    frame_offset          BIGINT,
    sample_offset         BIGINT,

    dataset_type          TEXT NOT NULL
);
```

Recommended index:

```sql
CREATE INDEX idx_waveform_stay_signal_time
ON waveform_segment (
    stay_key,
    signal_name,
    start_time
);
```

Waveform flow:

```text
waveform_segment
       ↓
resolve raw .hea / .dat
       ↓
WFDB reader
       ↓
Replay / quality processing
       ↓
Kafka / Flink
       ↓
fact_vital_event or fact_alert_event
```

The raw waveform remains outside TimescaleDB.

---

# 12. `vital_5m`

TimescaleDB continuous aggregate for 5-minute statistics.

```sql
CREATE MATERIALIZED VIEW vital_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(
        INTERVAL '5 minutes',
        event_time
    ) AS bucket_time,

    stay_key,
    signal_key,

    COUNT(*)        AS sample_count,
    AVG(value)      AS avg_value,
    MIN(value)      AS min_value,
    MAX(value)      AS max_value,
    VAR_SAMP(value) AS variance_value

FROM fact_vital_event

GROUP BY
    bucket_time,
    stay_key,
    signal_key

WITH NO DATA;
```

Logical fields:

```text
bucket_time
stay_key
signal_key
sample_count
avg_value
min_value
max_value
variance_value
```

Used for:

```text
BR-2
5-minute trend
Power BI patient detail
historical analysis
```

---

# 13. `vital_15m`

Continuous aggregate for 15-minute trends.

```sql
CREATE MATERIALIZED VIEW vital_15m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(
        INTERVAL '15 minutes',
        event_time
    ) AS bucket_time,

    stay_key,
    signal_key,

    COUNT(*)        AS sample_count,
    AVG(value)      AS avg_value,
    MIN(value)      AS min_value,
    MAX(value)      AS max_value,
    VAR_SAMP(value) AS variance_value

FROM fact_vital_event

GROUP BY
    bucket_time,
    stay_key,
    signal_key

WITH NO DATA;
```

---

# 14. BI view — `vw_icu_overview`

Main source for the ICU Overview page.

Expected columns:

```text
bed_id
stay_id
patient_key
care_unit

latest_hr
latest_map
latest_spo2
latest_rr

last_update_time

active_alert_count
risk_level
quality_status
```

Logical source:

```text
dim_patient
+
dim_stay
+
fact_vital_event
+
fact_alert_event
```

This is a SQL view, not a source-of-truth table.

If future measurements show it is too slow, it can later become a materialized view or cache.

---

# 15. BI view — `vw_alert_quality`

Main source for the Alerts & Quality dashboard page.

Expected columns:

```text
trigger_time
stay_id
bed_id
patient_key

alert_type
severity

signal_name
quality_status

status
acknowledged_at
```

Logical source:

```text
fact_alert_event
+
dim_stay
+
dim_patient
+
dim_signal
```

---

# 16. BI view — `vw_shift_summary`

Main source for BR-8 operational reporting.

Expected columns:

```text
shift_date
shift_name
care_unit

normal_count
warning_count
high_risk_count

alert_count
artifact_count
stale_count
```

Start with a normal SQL view.

Only change it to a materialized view or physical summary table if performance measurements justify that optimization.

---

# 17. BI dashboard structure

## Page 1 — ICU Overview

Primary source:

```text
vw_icu_overview
```

Shows:

- monitored beds;
- latest HR/MAP/SpO2/RR;
- last update time;
- active alerts;
- data-quality status;
- current operational risk level.

Main BRs:

```text
BR-1
BR-4
BR-8
```

---

## Page 2 — Patient Detail

Sources:

```text
fact_vital_event
vital_5m
vital_15m
fact_clinical_event
dim_signal
dim_clinical_item
dim_stay
```

Shows:

- patient/stay context;
- HR/MAP/SpO2/RR trends;
- 5-minute and 15-minute aggregates;
- alert markers;
- latest verified labs;
- GCS / urine context;
- selectable historical time range.

Main BRs:

```text
BR-2
BR-3
BR-5
BR-6
```

---

## Page 3 — Alerts & Quality

Sources:

```text
vw_alert_quality
fact_alert_event
fact_vital_event.quality_flag
```

Shows:

- alert counts;
- severity;
- suspected artifacts;
- stale signals;
- historical alerts;
- acknowledgement state.

Main BRs:

```text
BR-3
BR-7
```

---

# 18. Business Requirement mapping

| BR | Main objects |
|---|---|
| BR-1 Real-time Metric Ingestion | `fact_vital_event`, `waveform_segment`, pipeline metrics |
| BR-2 Sliding Window Vital Aggregations | `fact_vital_event`, `vital_5m`, `vital_15m` |
| BR-3 Early Sepsis / Shock Alerting | `fact_alert_event`, `fact_vital_event` |
| BR-4 Patient Multi-bed Live Dashboard | `vw_icu_overview` |
| BR-5 Historical Telemetry Replay | `fact_vital_event`, `waveform_segment`, BI time slicer |
| BR-6 Correlated Lab & Vital Inspection | `fact_vital_event`, `fact_clinical_event`, dimensions |
| BR-7 Alarm Quality & False Alarm Reduction | `quality_flag`, `fact_alert_event`, `vw_alert_quality` |
| BR-8 ICU Acuity & Risk Stratification Report | `vw_shift_summary`, `vw_icu_overview` |

---

# 19. Data kept outside TimescaleDB

## Raw MIMIC Clinical

```text
clinical/
├── icu/
│   ├── icustays.csv.gz
│   ├── chartevents.csv.gz
│   ├── d_items.csv.gz
│   ├── inputevents.csv.gz
│   └── outputevents.csv.gz
│
└── hosp/
    ├── patients.csv.gz
    ├── admissions.csv.gz
    ├── labevents.csv.gz
    └── d_labitems.csv.gz
```

Purpose:

```text
source-of-truth
audit
rebuild
provenance
```

These files remain read-only.

---

## Raw MIMIC Waveform

```text
waveform/
└── ...
    ├── *.hea
    └── *.dat
```

Raw waveform is not imported sample-by-sample into SQL.

`waveform_segment` only stores metadata and file references.

---

## Canonical Reference Dataset

```text
data/reference/
├── patient_stay.parquet
├── telemetry_event.parquet
├── clinical_event.parquet
├── waveform_manifest.parquet
├── reference_dataset_summary.md
└── reference_validation.json
```

This remains the deterministic input/rebuild source for the project.

---

## Generated Engineering Test Dataset

Planned:

```text
data/test/
├── scenarios/
│   ├── normal/
│   ├── duplicate/
│   ├── late_event/
│   ├── out_of_order/
│   ├── hypotension_5m/
│   ├── hypotension_short/
│   ├── artifact/
│   ├── stale/
│   └── missing_lab/
│
└── workload/
    ├── 10_beds/
    ├── 20_beds/
    ├── 50_beds/
    └── 100_beds/
```

Used for:

- deterministic logic tests;
- late/out-of-order testing;
- duplicate testing;
- alert testing;
- scalability/load testing.

---

# 20. Overall processing and storage flow

The architecture deliberately returns Flink output to **Kafka output topics** before persistence or notification. This decouples real-time detection from database availability and gives independent consumers access to the same processed event stream.

Only **one Kafka broker** is required for the local prototype. “Kafka before Flink” and “Kafka after Flink” refer to different topics on the same broker, not two Kafka clusters.

```text
RAW / REFERENCE FILES
│
├── MIMIC Clinical CSV.GZ
├── MIMIC Waveform .hea/.dat
├── Canonical Reference Parquet
└── Generated Test Data
        │
        ▼
  Replay Producer
        │
        ▼
 KAFKA — RAW TOPICS
        │
        ▼
      FLINK
        │
        ▼
 KAFKA — OUTPUT TOPICS
        │
        ├── processed vitals
        ├── processed clinical
        ├── vital aggregates
        ├── alerts
        └── deadletter
        │
   ┌────┴─────────────────────┐
   │                          │
   ▼                          ▼
Alert Receiver          Storage Consumer
(event-driven)                 │
                               ▼
                          TIMESCALEDB
                               │
                     ┌─────────┼─────────┐
                     │         │         │
                     ▼         ▼         ▼
                dimensions    facts    aggregates/views
                               │
                               ▼
                            POWER BI
```

Interpretation:

```text
Kafka        = durable event backbone before and after Flink
Flink        = validation + stream processing + early-warning engine
Alert Receiver = immediate notification path from Kafka alert topic
Storage Consumer = independent persistence writer
TimescaleDB  = persistence + history + analytics source
Power BI     = near-real-time visualization/reporting layer
```

The alert is created once by Flink and published to the Kafka alert topic. The same logical `alert_id` can be consumed by both the Alert Receiver and the Storage Consumer, so the immediate warning and later Power BI history refer to the same alert.

A temporary TimescaleDB slowdown should primarily appear as storage-consumer lag rather than forcing the alert path to query or wait for the database. Reliability still requires retention, offsets, retries, idempotency, and checkpointing in later implementation tickets.

TimescaleDB is therefore selected primarily for the project's storage and analytical workload, not because it is expected to outperform Cassandra in maximum raw write throughput.

---

# 21. Alternative storage benchmark

Cassandra remains the alternative storage solution.

Possible equivalent Cassandra query-driven tables:

```text
vitals_by_stay_hour
latest_vitals_by_unit
alerts_by_stay
clinical_context_by_stay
shift_summary_by_unit
```

The benchmark should compare the same logical data and workloads.

Recommended workloads:

```text
A. Continuous writes
B. Latest/current patient state
C. 24-hour patient history
D. Shift/unit aggregation
```

Recommended metrics:

```text
throughput
P50 / P95 / P99 latency
CPU
RAM
storage footprint
implementation/query complexity
```

No database is assumed to win before measurement.

---

# 22. Current architecture decision

```text
Primary stream processor:
Apache Flink

Alternative processor benchmark:
Kafka Streams

Primary persistence / analytical storage:
TimescaleDB

Alternative storage benchmark:
Cassandra

Visualization:
Power BI / Tableau
```

The key architectural rule is:

```text
Replay Producer
      ↓
Kafka raw topics
      ↓
Flink
      ↓
Kafka output topics
   ├──→ alert topic → Alert Receiver
   └──→ processed/aggregate/alert topics → Storage Consumer → TimescaleDB → Power BI
```

This prevents the persistence or BI layer from becoming a mandatory step before alert generation, while keeping Kafka as the pub/sub backbone for both raw input and processed output.

Database strategy:

```text
Dimensions
     +
Time-series Facts
     ↓
TimescaleDB hypertables
     ↓
Continuous aggregates
     ↓
SQL Views
     ↓
Power BI
```

No physical denormalized dashboard table is mandatory at the start.

Dashboard-specific materialization should only be added if benchmark/query measurements justify it.

For evaluation, distinguish two performance questions:

```text
Early-warning performance
= Kafka/Flink processing and alert-emission latency

Storage performance
= TimescaleDB vs Cassandra write/query latency and throughput
```

This separation allows the project to prioritize fast detection while still comparing alternative storage technologies objectively.

---

# 23. Scope boundary

This design intentionally does not attempt to implement:

- a complete hospital EHR;
- a large enterprise warehouse;
- a bedside ECG visualization system;
- sample-by-sample waveform storage in SQL;
- production HA/DR;
- a full clinical SOFA model when required source components are unavailable.

The goal is a small, explainable and implementable database model that supports the ICU streaming pipeline and BI demonstration.
