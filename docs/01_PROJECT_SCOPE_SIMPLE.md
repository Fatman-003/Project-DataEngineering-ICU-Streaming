# 01 — PROJECT SCOPE & CURRENT RUNTIME ARCHITECTURE

> **Status:** Current authoritative scope for implementation  
> **Last scope update:** 2026-10-10  
> **Important:** If older project documents conflict with this file, use this file for the **runtime pipeline and project scope**.  
> `DATABASE_DESIGN_PROPOSAL.md` remains the detailed reference for the TimescaleDB schema.

---

# 1. Project objective

Build an on-prem prototype for **real-time ICU patient telemetry processing and early warning** using historical MIMIC data as a replayable simulation source.

The project focuses on:

```text
data ingestion
→ streaming transport
→ real-time processing
→ early-warning generation
→ persistence
→ BI visualization
→ correctness/performance evaluation
```

The main engineering objective is:

> Detect relevant deterioration/quality conditions in the streaming layer as early as possible, without requiring a database query before an alert can be emitted.

This is an engineering prototype. It does **not** claim to provide a clinically validated sepsis diagnosis/prediction system.

---

# 2. Current primary architecture

## 2.1 Offline preparation

Historical MIMIC data is first prepared into a deterministic canonical dataset.

```text
MIMIC Clinical CSV.GZ
        +
MIMIC Waveform WFDB
        │
        ▼
Inspect / map / normalize
        │
        ▼
Canonical Reference Dataset
Parquet + waveform manifest
```

The Parquet files are a **project replay/reference format**, not a requirement of a real production ICU system.

## 2.2 Runtime streaming pipeline

```text
Canonical Reference / Engineering Test Data
                    │
                    ▼
            Python Replay Producer
                    │
                    ▼
┌─────────────────────────────────────────────┐
│              ONE KAFKA BROKER               │
│                                             │
│  RAW INPUT TOPICS                           │
│  ├── icu.v1.vitals.numeric                  │
│  ├── icu.v1.vitals.waveform                 │
│  └── icu.v1.clinical.events                 │
└────────────────────┬────────────────────────┘
                     │
                     ▼
                   FLINK
     validate / dedup / event-time / quality
          windows / early-warning rules
                     │
                     ▼
┌─────────────────────────────────────────────┐
│      SAME KAFKA BROKER — OUTPUT TOPICS      │
│                                             │
│  ├── processed vitals                       │
│  ├── processed clinical                     │
│  ├── window aggregates                      │
│  ├── alerts                                 │
│  └── deadletter                             │
└───────────────┬─────────────────┬───────────┘
                │                 │
                │                 └────────────→ Alert Receiver
                │                                  popup / console / small UI
                ▼
       Storage Consumer / Writer
                │
                ▼
           TIMESCALEDB
                │
                ▼
             POWER BI
```

**Important:** the two Kafka boxes above are two logical stages of the **same single Kafka broker**. Flink consumes raw topics from Kafka and publishes processed/alert/DLQ events back to Kafka. Flink does not write directly to TimescaleDB in the primary design.

The key architectural rule is:

```text
Kafka = event transport / buffering
Flink = real-time processing + early-warning decision
TimescaleDB = persistence + history + analytical serving
Power BI = near-real-time visualization / reporting
```

**TimescaleDB and Power BI are not on the critical path of alert detection.**

---

# 3. Why Kafka is used before and after Flink

Kafka acts as the event backbone.

Flink consumes raw topics, processes events, and publishes outputs back to Kafka.

This allows independent consumers to subscribe to the same processed result.

Example:

```text
Kafka raw topic
      ↓
    Flink
      ↓
Kafka output topic: icu.v1.primary.alerts
      │
      ├──→ Alert Receiver
      │       immediate notification
      │
      └──→ Storage Consumer
              ↓
          TimescaleDB
              ↓
           Power BI
```

This is still the **same Kafka broker**; the alert topic is simply an output topic produced by Flink.

The Alert Receiver does not need to query TimescaleDB before showing an alert.

A slow/down database may increase persistence lag, but the architecture must not require:

```text
Flink
→ write DB
→ query DB
→ then decide alert
```

---

# 4. Kafka topic scope

## Raw input topics

```text
icu.v1.vitals.numeric
icu.v1.vitals.waveform
icu.v1.clinical.events
```

## Primary processed/output topics

```text
icu.v1.primary.vitals.processed
icu.v1.primary.clinical.processed
icu.v1.primary.vitals.aggregates
icu.v1.primary.alerts
icu.v1.deadletter
```

The exact partition count can remain simple for the local prototype.

Only **one Kafka broker** is required.

---

# 5. Flink responsibility

Flink is the main stream-processing engine and the main component for early-warning latency.

It is responsible for:

```text
schema validation
value validation
normalization required at runtime
deduplication
event-time processing
watermarks
late/out-of-order handling
signal-quality handling
5-minute / 15-minute windows
clinical enrichment where required
early-warning rules
routing invalid data to DLQ
publishing cleaned events
publishing alerts
```

## Processing order

Conceptually:

```text
raw event
    ↓
validate
    ↓
deduplicate
    ↓
event-time / watermark
    ↓
quality handling
    ↓
clean event
    │
    ├──→ processed topic
    │
    └──→ window / alert logic
```

Invalid values must not be allowed to produce clinical alerts.

For example:

```text
MAP = -500
```

should be rejected/flagged, not interpreted as hypotension.

A valid event with:

```text
quality_flag = SUSPECTED_ARTIFACT
```

may still be persisted for audit/history, but the alert rule can suppress or downgrade its use.

---

# 6. Real-time alert path

Flink is the **single source of alert creation**.

Power BI must not independently recreate the clinical alert rule.

Example engineering rule:

```text
MAP < 65 mmHg sustained for 5 minutes
→ SUSTAINED_HYPOTENSION
```

The alert event should be self-contained enough for the Alert Receiver to identify the affected bed without querying the database.

Minimum alert message fields:

```text
alert_id
stay_id
patient_key
bed_id
alert_type
severity
signal_name
trigger_value
threshold_value
trigger_time
quality_status
status
```

Example:

```json
{
  "alert_id": "ALT-000123",
  "stay_id": 35727289,
  "patient_key": "P002",
  "bed_id": "BED-002",
  "alert_type": "SUSTAINED_HYPOTENSION",
  "severity": "HIGH",
  "signal_name": "MAP",
  "trigger_value": 62,
  "threshold_value": 65,
  "trigger_time": "2026-10-10T11:25:05Z",
  "quality_status": "GOOD",
  "status": "ACTIVE"
}
```

The same `alert_id` is later persisted to TimescaleDB so the real-time alert and BI history refer to the same logical alert.

---

# 7. TimescaleDB responsibility

**Primary storage:** TimescaleDB.

TimescaleDB receives processed data through an independent storage consumer/writer.

Main persisted data:

```text
clean numeric vitals
clinical events
alert history
metadata/dimensions
```

It is responsible for:

```text
historical telemetry
latest/history queries
clinical + vital correlation
alert history
5m/15m analytical aggregates
BI-facing SQL views
Power BI data source
```

The current database design is documented in:

```text
DATABASE_DESIGN_PROPOSAL.md
```

Main model:

```text
DIMENSIONS
├── dim_patient
├── dim_stay
├── dim_signal
└── dim_clinical_item

FACTS
├── fact_vital_event
├── fact_clinical_event
└── fact_alert_event

DERIVED / BI
├── vital_5m
├── vital_15m
├── vw_icu_overview
├── vw_alert_quality
└── vw_shift_summary

WAVEFORM METADATA
└── waveform_segment
```

No physical `latest_bed_state` table is required initially.

Use SQL views first; materialize only if performance measurements justify it.

---

# 8. Power BI responsibility

Power BI is the main visualization/reporting tool.

It is **not** the real-time alert engine.

Target behavior:

```text
Alert Receiver        = event-driven / near immediate
Power BI Overview     = near-real-time, target ~30 s refresh
Historical analytics  = 30 s–1 min or slower is acceptable
```

## Page 1 — ICU Overview

Primarily:

```text
BR-1
BR-4
BR-8
```

Show per bed:

```text
HR
MAP
SpO2
RR
alert/risk status
last update time
quality/freshness
```

Keep this page compact.

## Page 2 — Patient Detail

Primarily:

```text
BR-2
BR-3
BR-5
BR-6
```

Show all six canonical numeric vitals:

```text
HR
MAP
SBP
DBP
SpO2
RR
```

For each signal, support:

```text
current/latest value
5-minute AVG / MIN / MAX
15-minute AVG / MIN / MAX
time-series trend
```

Variance/count may be retained for BR-2 correctness/evaluation even if they are not all displayed as cards.

Also show:

```text
alert markers
latest available labs
GCS / urine context where available
historical time slicer
```

## Page 3 — Alerts & Quality

Primarily:

```text
BR-3
BR-7
```

Show:

```text
active/historical alerts
severity
bed/stay
signal
trigger value / threshold
quality status
stale/artifact events
acknowledgement status if implemented
```

---

# 9. Vital signs in scope

Canonical numeric vitals:

```text
HR    Heart Rate                  bpm
MAP   Mean Arterial Pressure      mmHg
SBP   Systolic Blood Pressure     mmHg
DBP   Diastolic Blood Pressure    mmHg
SpO2  Oxygen Saturation           %
RR    Respiratory Rate            breaths/min
```

These are the main numeric signals visualized in Power BI.

Clinical context such as:

```text
Lactate
Creatinine
Platelet
Bilirubin
GCS
Urine Output
```

is displayed separately and must not be mislabeled as vital signs.

---

# 10. Business Requirements

## BR-1 — Real-time Metric Ingestion

Replay ICU telemetry into Kafka and verify that the pipeline handles:

```text
valid events
invalid schema/value
duplicates
late events
out-of-order events
```

A bad event must not stop the whole pipeline.

Waveform provides the high-frequency input case.

## BR-2 — Sliding Window Vital Aggregations

Flink calculates streaming 5-minute and 15-minute windows for numeric vitals.

Target statistics:

```text
count
mean
min
max
variance
```

Power BI displays current values plus selected 5m/15m aggregates/trends.

TimescaleDB continuous aggregates may support historical BI queries, but BR-2 streaming correctness must still be demonstrated in Flink.

## BR-3 — Early Sepsis / Shock Alerting

Engineering early-warning rules.

MVP deterministic rule:

```text
MAP < 65 mmHg sustained for 5 minutes
→ SUSTAINED_HYPOTENSION
```

A short low-MAP period such as approximately 2 minutes must not trigger this sustained rule.

Additional rules may be added only if supported by available data.

This must be described as physiological/hemodynamic screening, not validated sepsis diagnosis.

## BR-4 — Patient Multi-bed Live Dashboard

Power BI provides a near-real-time multi-bed overview.

The separate Alert Receiver provides the immediate event-driven warning path.

## BR-5 — Historical Telemetry Replay

Allow inspection/replay of a selected stay/time range, with a target history window up to approximately 24 hours.

Power BI may implement this using stay/time slicers rather than a video-like animation.

## BR-6 — Correlated Lab & Vital Inspection

Display relevant labs/clinical context alongside vital trends.

Important availability rule:

```text
A lab is not considered known before available_time.
```

Missing components must be shown as `N/A`, never silently converted to zero.

Full SOFA must not be claimed unless all required components are actually verified.

## BR-7 — Alarm Quality & False Alarm Reduction

Engineering quality rules for:

```text
suspected artifact
stale signal
missing signal
invalid values
abnormal jumps
```

Quality flags can influence whether a clinical alert is emitted/suppressed.

No claim of clinically validated signal-quality classification is required.

## BR-8 — ICU Acuity & Risk Stratification Report

Operational shift/unit summary such as:

```text
normal
warning
high risk
active alerts
artifact/stale counts
```

This is a project operational summary, not a clinically validated triage score.

---

# 11. Waveform scope

Waveform remains in the project, but raw samples are not stored sample-by-sample in TimescaleDB.

Raw source:

```text
*.hea
*.dat
```

Canonical reference contains:

```text
waveform_manifest.parquet
```

The manifest points to raw WFDB records.

Runtime concept:

```text
waveform_manifest
       ↓
locate raw .hea/.dat
       ↓
WFDB reader
       ↓
short waveform chunks
       ↓
Kafka waveform topic
       ↓
Flink quality / feature processing
```

Waveform is primarily useful for:

```text
BR-1 high-frequency streaming
BR-7 signal quality / artifact cases
performance/load testing
optional short waveform demonstration
```

Power BI is **not** expected to behave like a full bedside ECG waveform monitor.

---

# 12. Primary vs alternatives

The project separates the two comparisons.

## Primary stream processor

```text
Apache Flink
```

## Alternative stream processor

```text
Kafka Streams
```

Processor benchmark should compare representative equivalent logic, such as:

```text
one streaming window
one alert rule
same input
same workload
```

Metrics:

```text
throughput
processing/detection latency
CPU
RAM
correctness
```

## Primary storage

```text
TimescaleDB
```

## Alternative storage

```text
Cassandra
```

Storage benchmark uses the same logical processed data/workloads.

Representative workloads:

```text
continuous write
latest patient values
24h stay history
shift/unit aggregation
```

Metrics:

```text
write throughput
P50/P95/P99 latency
CPU
RAM
storage footprint
query/implementation complexity
```

Do not assume a winner before measurement.

Cassandra is not required as a multi-node cluster for this local prototype.

---

# 13. Performance interpretation

For the project objective, distinguish:

```text
Detection latency
= event available to pipeline
→ Flink emits alert
```

from:

```text
Persistence latency
= processed event
→ TimescaleDB commit
```

and:

```text
Visualization latency
= persisted data
→ Power BI shows update
```

The main early-warning objective is tied to **Flink detection latency**, not Power BI refresh latency.

Also measure:

```text
Kafka lag
Flink backpressure
storage writer lag
TimescaleDB write throughput
```

to verify that persistence can keep up with the processed stream.

---

# 14. Local deployment scope

Keep deployment minimal:

```text
1 Kafka broker
1 Flink JobManager
1 Flink TaskManager
1 TimescaleDB instance
1 lightweight Storage Consumer / Writer
1 lightweight Alert Receiver
Power BI Desktop
```

Only when storage benchmarking:

```text
1 Cassandra node
```

No requirement for:

```text
Kafka cluster
Cassandra cluster
Kubernetes
cloud deployment
Prometheus/Grafana
complex authentication
production HA/DR
```

---

# 15. Current implementation checkpoint

Completed:

```text
TICKET-001  Raw Data Inspection                 PASS
TICKET-002  Clinical ↔ Waveform Mapping         PASS
TICKET-003  Canonical Reference Dataset         PASS
```

Current canonical Reference Dataset:

```text
3 patients / 3 ICU stays
905 telemetry events
232 clinical events
150 waveform manifest rows
```

Next implementation boundary:

```text
TICKET-004
Canonical Reference/Test Data
        ↓
Python Replay Producer
        ↓
ONE Kafka Broker
        ↓
Consumer verification
```

**Do not implement Flink, TimescaleDB, Power BI, or the Alert Receiver inside TICKET-004 unless the ticket explicitly says so.**

The next tickets will build the runtime pipeline incrementally after Kafka replay is verified.
