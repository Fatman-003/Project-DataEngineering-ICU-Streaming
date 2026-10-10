# 02 — DATA PLAN & IMPLEMENTATION HANDOFF

> **Status:** Current authoritative data/replay handoff for Codex  
> **Last scope update:** 2026-10-10  
> Read together with:
>
> ```text
> 01_PROJECT_SCOPE_SIMPLE.md
> DATABASE_DESIGN_PROPOSAL.md
> ```

---

# 1. Current local project environment

Repository root:

```text
D:\Data Engineering\ICU_Project\icu-streaming-project
```

Python:

```text
3.12.2
```

Virtual environment:

```text
D:\Data Engineering\ICU_Project\icu-streaming-project\.venv
```

Raw Clinical root:

```text
D:/Data Engineering/MIMIC_DATA/clinical/mimic-iv-clinical-database-demo-2.2
```

Raw Waveform root:

```text
D:/Data Engineering/MIMIC_DATA/waveform
```

Project data directories:

```text
data/working
data/reference
data/test
```

Raw MIMIC files remain outside Git and must be treated as read-only.

Docker Desktop is installed for the local stack with application/data intentionally located on drive `D:` because drive `C:` has limited space.

---

# 2. What the source data actually is

The source data is **not Parquet**.

## MIMIC-IV Clinical

Raw format:

```text
*.csv.gz
```

Selected files:

```text
icu/
├── icustays.csv.gz
├── chartevents.csv.gz
├── d_items.csv.gz
├── inputevents.csv.gz
└── outputevents.csv.gz

hosp/
├── patients.csv.gz
├── admissions.csv.gz
├── labevents.csv.gz
└── d_labitems.csv.gz
```

## MIMIC-IV Waveform

Raw format:

```text
*.hea
*.dat
```

Relevant signals include:

```text
ECG leads
ABP
Pleth
Resp
```

Observed sampling frequencies include approximately:

```text
62.4725 Hz
124.945 Hz
249.89 Hz
```

---

# 3. Why the project has Parquet files

Parquet is a **project-created canonical/reference format**.

It is not the format expected from a real ICU monitor.

Offline preparation:

```text
MIMIC historical CSV/WFDB
        ↓
inspect / map / normalize
        ↓
canonical Parquet + waveform manifest
```

Runtime simulation:

```text
canonical historical data
        ↓
Replay Producer
        ↓
Kafka events
```

Production concept:

```text
real ICU monitor / EHR / LIS
        ↓
gateway / integration adapter
        ↓
Kafka Producer
        ↓
Kafka
```

Therefore:

> The Replay Producer simulates the live hospital/device integration layer using historical MIMIC data.

Do **not** redesign runtime as:

```text
live event
→ write Parquet
→ Kafka
```

Parquet is not required on the real-time critical path.

---

# 4. Raw Clinical file roles

## `icustays`

Used for:

```text
subject_id
hadm_id
stay_id
ICU intime/outtime
care-unit context
```

## `chartevents`

Primary/reference source for many bedside observations:

```text
HR
MAP
SBP
DBP
SpO2
RR
GCS components
```

## `d_items`

Resolves `chartevents.itemid`.

## `patients`

Patient metadata.

## `admissions`

Admission identity/context and `subject_id` / `hadm_id` mapping.

## `labevents`

Relevant labs including available reference items such as:

```text
Lactate
Creatinine
Platelet
Bilirubin
```

## `d_labitems`

Resolves lab item IDs.

## `inputevents`

Treatment/input source. Use only when an event required by a rule is actually verified in the selected stays.

## `outputevents`

Output such as urine-output events.

---

# 5. Raw Waveform role

Raw waveform provides:

```text
high-frequency streaming input
signal-quality/artifact cases
load/performance workload
optional short waveform demonstration
```

Raw waveform is **not** converted wholesale into SQL rows.

Raw:

```text
*.hea
*.dat
```

Reference metadata:

```text
waveform_manifest.parquet
```

At runtime the producer can:

```text
read waveform manifest
        ↓
find raw WFDB record
        ↓
decode requested segment
        ↓
build short chunk
        ↓
publish chunk to Kafka
```

The raw waveform files remain the source of samples.

---

# 6. Completed data-preparation work

## TICKET-001 — Raw Data Inspection — PASS

Verified:

```text
100 patients
140 ICU stays
668,862 chartevents
107,727 labevents
all 9 selected Clinical files readable
```

Waveform inventory:

```text
198 patient directories
200 recording groups
198 parent headers
4,042 headers
10,086 .dat files
```

Known raw waveform integrity issues include missing referenced headers/data files and orphan headers.

Raw data must remain unchanged.

## TICKET-002 — Clinical ↔ Waveform Mapping — PASS

Mapping result:

```text
MATCHED      4 recording groups
AMBIGUOUS    0
UNMATCHED  122
INCOMPLETE  74
TOTAL      200
```

Three reference ICU stays were selected:

```text
P001
subject_id 10039708
hadm_id    24928679
stay_id    37323533
waveform   83411188

P002
subject_id 10019003
hadm_id    21457723
stay_id    35727289
waveform   87033314

P003
subject_id 10014354
hadm_id    29600294
stay_id    39864867
waveform   81739927
```

Important limitations:

```text
PaO2 verified count = 0 for all selected stays
no verified vasopressor event in the selected Reference Dataset
```

Therefore full SOFA must not be assumed.

## TICKET-003 — Canonical Reference Dataset — PASS

Canonical schema version:

```text
1.0.0
```

Deterministic run/build ID:

```text
REFERENCE-7e6eb08b675a685957d8bdf2
```

Current outputs:

```text
data/reference/
├── patient_stay.parquet
├── telemetry_event.parquet
├── clinical_event.parquet
├── waveform_manifest.parquet
├── reference_dataset_summary.md
└── reference_validation.json
```

Current counts:

```text
patient_stay.parquet         3 rows
telemetry_event.parquet    905 rows
clinical_event.parquet     232 rows
waveform_manifest.parquet  150 rows
```

Deterministic rerun equality has passed.

---

# 7. Canonical Reference entities

## `patient_stay.parquet`

Purpose:

```text
patient/stay identity
mapping
bed simulation
provenance
```

Contains the three selected reference stays.

`bed_id` is simulated for project visualization.

## `telemetry_event.parquet`

Canonical numeric telemetry.

Signals:

```text
HR
MAP
SBP
DBP
SPO2
RR
```

Important logical fields include:

```text
event_id
run_id

patient_key
subject_id
hadm_id
stay_id

event_time
source_event_time
producer_time

signal_name
value
unit
quality_flag

source_type
source_table
source_record_id

dataset_type
is_virtual
```

`producer_time` is null in the offline canonical dataset and is populated by the runtime Replay Producer.

## `clinical_event.parquet`

Canonical clinical context.

Current real reference types include:

```text
LAB
GCS
URINE
```

Important fields include:

```text
event_id
run_id

patient_key
subject_id
hadm_id
stay_id

event_type
item_id
item_name

chart_time
available_time

value
value_text
unit

availability_status
validation_flag

source_table
source_record_id

dataset_type
is_virtual
```

## `waveform_manifest.parquet`

This is **metadata/index**, not a materialized waveform-sample table.

It identifies:

```text
patient/stay
waveform record
signal
time coverage
sampling rate
raw file/header reference
```

The Replay Producer uses the manifest to locate and decode raw WFDB data on demand.

---

# 8. Timestamp semantics

Keep these timestamps distinct.

## `source_event_time`

Original historical MIMIC event time.

Used for:

```text
traceability
offline correctness
source provenance
```

## `event_time`

Logical event time used by the runtime stream.

For Reference data it originates from the source timeline.

During replay it may be shifted to a synthetic/current logical timeline while preserving relative ordering and time gaps.

Flink event-time/window logic must use the agreed runtime `event_time`.

## `producer_time`

Actual wall-clock timestamp when the Replay Producer sends the event.

Use this for real-time latency evaluation.

Example:

```text
source_event_time = historical MIMIC time
event_time        = replay logical time
producer_time     = actual current send time
```

Do not calculate end-to-end runtime latency using only the original historical MIMIC timestamp.

---

# 9. Lab availability semantics

`chart_time` and `available_time` must remain different concepts.

Example:

```text
sample/chart time      = 10:20
result available time  = 10:32
```

The pipeline must not treat the result as known at 10:20.

Canonical rule already applied:

```text
valid storetime >= charttime
→ available_time = storetime
```

If source availability is missing/malformed/earlier than chart time:

```text
available_time = NULL
```

with the appropriate validation flag.

Never replace unknown availability with `chart_time`.

Downstream clinical correlation must respect availability.

---

# 10. Generated Engineering Test Dataset

The clean Reference Dataset is not sufficient by itself to prove runtime validation.

A second dataset will deliberately inject engineering edge cases.

## Scenario mode

Planned deterministic scenarios:

```text
NORMAL
INVALID_SCHEMA
INVALID_VALUE
DUPLICATE
LATE_EVENT
OUT_OF_ORDER
HYPOTENSION_5M
HYPOTENSION_SHORT
ARTIFACT
STALE
MISSING_LAB
LATE_LAB
```

Purpose:

```text
validation
dedup
watermark
late/out-of-order behavior
alert correctness
quality handling
clinical-availability behavior
```

## Load mode

Virtual workloads:

```text
10 beds
20 beds
50 beds
100 beds
```

May be created by:

```text
clone
time shift
reference-seeded virtual patients
```

Generated records must be clearly labeled:

```text
is_virtual = true
source_patient_key = ...
scenario_type = ...
```

Do not present generated workload as clinically validated synthetic patient data.

---

# 11. Replay Producer input mapping

The Replay Producer is the simulated live source.

## Numeric vitals

Input:

```text
data/reference/telemetry_event.parquet
```

Output topic:

```text
icu.v1.vitals.numeric
```

## Clinical events

Input:

```text
data/reference/clinical_event.parquet
```

Output topic:

```text
icu.v1.clinical.events
```

## Waveform

Inputs:

```text
data/reference/waveform_manifest.parquet
+
raw WFDB .hea/.dat
```

Output topic:

```text
icu.v1.vitals.waveform
```

The producer should emit short chunks rather than one Kafka message per waveform sample.

A practical initial chunk duration is approximately:

```text
1 second
```

This can be adjusted later based on benchmark results.

---

# 12. Replay behavior

The producer should support deterministic replay.

Required concepts:

```text
ordered event replay
wall-clock pacing
configurable replay speed
stable event identity
producer_time stamping
clean shutdown/flush
```

Expected speed modes can include:

```text
x1
x10
x60
```

but implementation may start with x1 + one accelerated mode.

The producer must not send the entire Parquet dataset as one message.

Conceptually:

```text
row/event
    ↓
serialize canonical event
    ↓
Kafka producer API
    ↓
topic message
```

Kafka does not read the Parquet file itself.

---

# 13. Runtime Kafka flow

The runtime uses **one Kafka broker**. Kafka appears both before and after Flink because Flink consumes raw topics and publishes processed results back to output topics on that same broker.

```text
Replay Producer
      ↓
┌──────────────────────────────────────┐
│ ONE KAFKA BROKER — RAW INPUT TOPICS  │
└──────────────────┬───────────────────┘
                   ↓
                 Flink
                   ↓
┌──────────────────────────────────────┐
│ SAME KAFKA BROKER — OUTPUT TOPICS    │
│                                      │
│ icu.v1.primary.vitals.processed      │
│ icu.v1.primary.clinical.processed    │
│ icu.v1.primary.vitals.aggregates     │
│ icu.v1.primary.alerts                │
│ icu.v1.deadletter                    │
└──────────────┬───────────────┬───────┘
               │               │
               ▼               ▼
       Storage Consumer   Alert Receiver
               │
               ▼
          TimescaleDB
               ↓
            Power BI
```

Flink is responsible for:

```text
validation
dedup
event-time/watermark
late/out-of-order
quality
5m/15m windows
early-warning rules
```

The processed/alert topics decouple the stream processor from persistence and notification consumers. In the primary design, Flink publishes its outputs back to Kafka rather than writing directly to TimescaleDB.

---

# 14. Cleaned-event persistence flow

Processed numeric/clinical events are written to TimescaleDB through an independent storage writer/consumer.

```text
Flink
  ↓
Kafka processed topic
  ↓
Storage Consumer
  ↓
TimescaleDB
```

Alerts use the same pattern:

```text
Flink
  ↓
Kafka alert topic
  ├──→ Alert Receiver
  └──→ Storage Consumer
           ↓
      TimescaleDB fact_alert_event
```

The Kafka topics in both paths belong to the same single broker used by the project.

This separation means a temporary database slowdown primarily appears as:

```text
consumer lag / persistence lag
```

rather than requiring the alert rule to query the database first.

Reliability implementation must still consider:

```text
Kafka retention
consumer offsets
retry
idempotency
duplicate handling
Flink checkpoints
```

---

# 15. Alert event contract

Flink is the only component that creates a business alert.

A Kafka alert message should be self-contained.

Minimum fields:

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

run_id
dataset_type
```

The Alert Receiver can therefore show:

```text
BED-002
HIGH
SUSTAINED_HYPOTENSION
MAP = 62 mmHg
```

without querying TimescaleDB.

The same `alert_id` must be persisted in TimescaleDB for consistency with Power BI history.

---

# 16. Vital visualization model

Numeric signals in scope:

```text
HR
MAP
SBP
DBP
SpO2
RR
```

## ICU Overview

Keep compact:

```text
HR
MAP
SpO2
RR
alert/risk status
quality/freshness
last update
```

Target Power BI refresh:

```text
~30 seconds
```

This is near-real-time visualization, not the alert transport.

## Patient Detail

Show:

```text
current value
5m aggregate
15m aggregate
historical trend
alert marker
```

for the six numeric vitals where useful.

Main aggregate statistics:

```text
count
AVG
MIN
MAX
variance
```

Not every statistic must be shown as a card, but all can be retained for BR-2 correctness/evaluation.

---

# 17. Flink BR-2 vs Timescale aggregates

Avoid confusing two responsibilities.

## Flink

Computes streaming windows for:

```text
BR-2 correctness
real-time trend state
alert logic
```

Example:

```text
MAP < 65 sustained over required window
```

must be evaluated in Flink, not by Power BI.

## TimescaleDB

Can also maintain:

```text
vital_5m
vital_15m
```

continuous aggregates for efficient historical/BI queries.

These do not replace Flink's streaming-window responsibility.

---

# 18. Runtime database model

Detailed schema is in:

```text
DATABASE_DESIGN_PROPOSAL.md
```

Current simplified structure:

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

DERIVED
├── vital_5m
└── vital_15m

BI VIEWS
├── vw_icu_overview
├── vw_alert_quality
└── vw_shift_summary

WAVEFORM METADATA
└── waveform_segment
```

Raw waveform samples remain outside TimescaleDB.

---

# 19. Power BI data use

Power BI is not expected to consume Kafka directly for the main dashboard.

Primary path:

```text
TimescaleDB
    ↓
SQL views / facts / aggregates
    ↓
Power BI
```

Recommended pages:

```text
1. ICU Overview
2. Patient Detail
3. Alerts & Quality
```

A separate lightweight Alert Receiver subscribes to the Kafka alert topic for immediate warnings.

This avoids forcing Power BI to behave like a bedside event-notification system.

---

# 20. Data quality and clinical limitations

Current Reference Dataset limitations must remain visible:

```text
PaO2 unavailable in selected stays
no verified vasopressor events
full SOFA cannot be assumed
```

Missing SOFA components:

```text
N/A
```

not:

```text
0
```

Lactate is useful clinical context but is not a SOFA component.

The project should describe the warning logic as:

```text
rule-based physiological/hemodynamic screening
```

not a validated sepsis diagnosis/prediction model.

---

# 21. Current coding checkpoint

Completed:

```text
TICKET-001
Raw inspection
PASS

TICKET-002
Clinical ↔ Waveform mapping
PASS

TICKET-003
Canonical Reference Dataset
PASS
```

Current runtime stack status:

```text
Python 3.12.2               ready
canonical Reference data    ready
Docker Desktop              installed/configured on D:
Kafka broker                not yet considered verified
Flink                       not implemented yet
TimescaleDB runtime schema  not implemented yet
Power BI                    not connected yet
```

---

# 22. Next ticket boundary

Next:

```text
TICKET-004 — Replay Producer + Single Kafka Broker Verification
```

Goal:

```text
canonical data
      ↓
Replay Producer
      ↓
one Kafka broker
      ↓
console/test consumer
      ↓
verify ordering / schema / pacing / producer_time
```

TICKET-004 should **not** yet implement:

```text
Flink processing
TimescaleDB persistence
Power BI
Alert Receiver
Cassandra benchmark
```

Those belong to later tickets.

---

# 23. Planned implementation sequence

Current recommended order:

```text
001  Raw inspection                         DONE
002  Clinical ↔ Waveform mapping            DONE
003  Canonical Reference Dataset            DONE

004  Replay Producer + Kafka verification   NEXT

005  Flink streaming foundation
006  Runtime validation / dedup / event-time / DLQ
007  5m/15m windows + early-warning rule
008  Processed/alert Kafka output topics + consumers
009  TimescaleDB schema + storage writer
010  Power BI views/dashboard connection
011  Lightweight real-time Alert Receiver
012  Engineering scenarios + correctness evaluation
013  Load/performance evaluation
014  Kafka Streams processor benchmark
015  Cassandra storage benchmark
```

Ticket numbering after 004 can still be adjusted if implementation findings require it, but the dependency order should remain approximately the same.

---

# 24. Things not required for project scope

Do not add unless a later ticket explicitly requires them:

```text
multi-broker Kafka cluster
multi-node Cassandra cluster
Kubernetes
cloud deployment
full custom React dashboard
full FastAPI/WebSocket backend for the main BI dashboard
sample-by-sample waveform SQL storage
production authentication
production HA/DR
complex CI/CD
full SOFA without verified source components
```

Current priority:

```text
Replay correctly
→ Process correctly
→ Alert early
→ Persist reliably
→ Visualize clearly
→ Benchmark objectively
```
