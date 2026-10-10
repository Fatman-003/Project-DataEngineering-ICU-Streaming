# TICKET-006 — Runtime validation, deduplication, event-time and DLQ

**Status: IMPLEMENTED — MANUAL VERIFICATION PENDING.** TICKET-001 through TICKET-005 are manually verified PASS. Automated and live TICKET-006 verification passed; the user must complete manual verification before recording final PASS. TICKET-007 starts only after that manual PASS.

## Scope and runtime architecture

```text
Existing host Replay Producer / labeled engineering probes
                         ↓ localhost:9092
                    ONE Kafka broker
                         ↓ icu-kafka:9094
      numeric RAW / clinical RAW / waveform RAW
                         ↓
     Java metadata envelope: topic, partition, offset,
                exact key bytes and value bytes
                         ↓
       strict UTF-8 / JSON-object parsing
                         ↓
          schema / value / timestamp checks
                         ↓
             Flink managed-state dedup
                         ↓
     event_time timestamp assigner / bounded watermark
                         ↓
           effective-watermark classification
                ↙                       ↘
   clean internal stream             rejection side outputs
   CLEAN_EVENT /                     versioned DLQ envelope
   OUT_OF_ORDER_EVENT                       ↓
   bounded waveform log preview          KafkaSink
                                            ↓
                                 icu.v1.deadletter
                                 on the SAME broker
```

The three RAW topics remain `icu.v1.vitals.numeric`, `icu.v1.clinical.events` and `icu.v1.vitals.waveform`. The only added output topic is `icu.v1.deadletter`. Clean records remain internal and are logged for verification. There is no processed, aggregate or alert topic in this ticket.

The existing TICKET-005 `raw_consumer.py` inspection job remains available. It does not perform the TICKET-006 validation or publish a DLQ. The one-slot local stack runs one of these jobs at a time.

## Files created or modified

Modified for TICKET-006:

- `docs/01_PROJECT_SCOPE_SIMPLE.md`
- `docs/02_DATA_PLAN_BEFORE_CODING.md`
- `docker/flink/Dockerfile`
- `docker/flink/README.md`
- `scripts/replay/kafka_topics.py`
- `scripts/replay/consume.py`

Created for TICKET-006:

- `jobs/flink/runtime_config.py`
- `jobs/flink/runtime_pipeline.py`
- `jobs/flink/validation.py`
- `jobs/flink/dedup.py`
- `jobs/flink/event_time.py`
- `jobs/flink/dlq.py`
- `docker/flink/java/org/icu/streaming/KafkaMetadataDeserializer.java`
- `docker/flink/java/org/icu/streaming/DeadLetterSerializationSchema.java`
- `tests/flink/test_validation.py`
- `tests/flink/test_runtime.py`
- `scripts/runtime/verify_runtime.py`
- `docker/flink/RUNTIME_VALIDATION.md`

Generated evidence is Git-ignored under `data/working/runtime/`:

- `baseline.json`: pre-ticket raw-file metadata and protected-output hashes.
- `reference_contract_check.json`: validation of all 905 numeric and 232 clinical Reference rows after replay timestamp stamping, without rewriting their files.
- `probe_report.json`: live cases, published source metadata, exact input audit and verification assertions.
- `clean_events.jsonl`: accepted probe audit lines.
- `dlq_events.jsonl`: actual consumed Kafka DLQ envelopes and Kafka delivery metadata.
- `probe_report.failed_print.json`: preserved initial failed integration attempt before correcting PyFlink's logging compatibility issue.
- `tests_report.json`: results of all four automated suites, totaling 115 passing tests.
- `reference_replay_report.json`: actual Reference-producer smoke results and comparisons against Kafka inputs.
- `reference_clean_events.jsonl`: the 63 accepted Reference replay audit records from the fresh runtime job.
- `verification_report.json`: final combined verification, final job/checkpoint state and protected-data integrity results.
- `verification_summary.md`: final human-readable verification results.

The Dockerfile builds the two small Java classes against the exact Flink and shaded Kafka connector already used by the image. Its temporary JDK exists only in the build stage. The final runtime receives the compiled JAR through the image build; no host Java installation or manually copied runtime JAR is required. These classes supply Kafka metadata and dynamic DLQ keys that the Python value-only source foundation did not expose.

## Exact progress-document changes

The two authoritative documents were updated before implementation: TICKET-001–005 PASS, TICKET-006 IN PROGRESS, Flink READY, and the TICKET-007 boundary after TICKET-006 manual review. Their checkpoint/status/boundary sections and update dates changed; unrelated architecture sections were preserved. The data-plan implementation sequence was updated to the same ordering.

After successful automated and live verification, both documents now contain these statuses:

```text
TICKET-001–005       PASS
TICKET-006          IMPLEMENTED — MANUAL VERIFICATION PENDING
TICKET-007          NEXT AFTER TICKET-006 MANUAL PASS

Kafka broker        READY
Replay Producer     READY
Flink runtime       READY
Runtime validation  IMPLEMENTED
Deduplication       IMPLEMENTED
Event-time/watermark IMPLEMENTED
DLQ Kafka path      IMPLEMENTED

5m/15m windows      NOT STARTED
Early-warning rules NOT STARTED
TimescaleDB         NOT STARTED
Power BI            NOT STARTED
Alert Receiver      NOT STARTED
```

Codex does not mark TICKET-006 final PASS on the user's behalf.

## Validation rules

Validation preserves accepted event fields, timestamp strings, nulls and provenance. It uses the existing TICKET-004 contracts, including string IDs and canonical `SPO2` spelling.

| Stage | Required behavior / rejection |
|---|---|
| Parse | Strict UTF-8, strict JSON constants and an object root. Malformed/truncated JSON, arrays, invalid UTF-8 and Kafka tombstones become `PARSE_ERROR`. |
| Common schema | Nonempty string `stay_id`; nonempty string `event_id` for numeric and clinical; parseable `event_time` and `producer_time`. Supplied identity fields must follow the string contract. Unpaired Unicode surrogates in validated identity/text fields become `SCHEMA_ERROR` before Flink string-key serialization. |
| Numeric schema | `signal_name` is one of `HR`, `MAP`, `SBP`, `DBP`, `SPO2`, `RR`; `value` is present. Missing fields, malformed timestamps and unknown signal names become `SCHEMA_ERROR`. |
| Numeric values | Values must be numeric, finite and not booleans. `HR`, `MAP`, `SBP`, `DBP`, `RR` must be >= 0; `SPO2` must be in [0, 100]. Invalid values become `VALUE_ERROR`. `MAP = 62` is accepted. No extra clinical upper thresholds are invented. |
| Clinical | `event_type` is `LAB`, `GCS` or `URINE`; `chart_time`, `available_time`, `value` and `value_text` keys are present. `available_time = null` remains null and is valid. A non-null availability timestamp before chart time becomes `VALUE_ERROR`. Non-null numeric values must be finite; `value_text` is a string or null. |
| Waveform | Nonempty record/signal IDs, finite numeric `sampling_rate_hz > 0`, a nonempty samples array and parseable start/end with end > start. Individual null samples are accepted; non-null samples must be finite numbers. No waveform artifact interpretation is implemented. |
| Optional numeric fields | JSON float overflow to infinity in an optional/nested field is rejected as `VALUE_ERROR` rather than failing later serialization. |

These are engineering validity bounds, not clinically validated diagnostic thresholds. Missing measurements are never replaced with zero; unknown clinical availability is never replaced with chart time. `source_event_time`, quality flags and other original fields remain provenance/context rather than substitute event-time values.

Non-JSON literal constants such as `NaN` and `Infinity` fail parsing. A valid JSON exponent that overflows Python's finite floating-point range fails value validation. This distinction is deliberate.

Each stage sends a rejected record only to its rejection side output. Rejected parse/schema/value records never reach dedup or watermark assignment. Duplicates never reach timestamp assignment. Late records never reach the clean internal stream.

## Deduplication and state

Deduplication uses real Flink keyed `ValueState`, descriptor `seen-event`, with the following deterministic keys:

- Numeric: `vitals:id:<event_id>`.
- Clinical: `clinical:id:<event_id>`.
- Waveform with an existing event ID: `waveform:id:<event_id>`. TICKET-004 currently emits such stable waveform IDs.
- Waveform without an event ID: `waveform:chunk:<sha256>`, hashing the compact JSON array `[stay_id, waveform_record_id, signal_name, start_time, end_time]`.

The stream prefix isolates identities between contracts. The first schema/value-valid occurrence writes state; a later occurrence with that identity becomes `DUPLICATE_EVENT` and goes to DLQ. Since dedup precedes lateness classification, an initial event later rejected as late still establishes its dedup identity; its next occurrence can be rejected as duplicate.

Default TTL is 24 hours of **processing time**. Flink's TTL updates on create/write, never returns expired values, and enables incremental cleanup. Merely observing duplicates does not refresh the TTL. State can expire and accept the same ID afterward: this is not permanent global deduplication. The per-stay `max-clean-event-time` state uses the same TTL configuration.

Checkpoints run every 10 seconds and include managed dedup state. The existing shared named volume `icu-flink-checkpoints` remains the filesystem checkpoint location. Live evidence must show completed post-probe checkpoints and growth of the dedup operators' checkpointed state, not just a checkpoint counter from the sources.

## Event-time, watermark and ordering policy

The actual Flink timestamp assigner reads only the payload's `event_time`. It does not replace it with `source_event_time`, `producer_time` or wall-clock time. Naive historical MIMIC timestamps are interpreted as UTC solely to provide a consistent epoch/comparison convention; their original strings remain unchanged. This does not claim that MIMIC recorded them in UTC. Explicit offsets normalize for comparisons.

The default bounded out-of-orderness tolerance is 30 seconds, with periodic watermark emission every 200 ms. Flink's bounded strategy emits `maximum_observed_timestamp - 30000 ms - 1 ms`. The classifier reads its effective Flink watermark through `ctx.timer_service().current_watermark()`.

| Condition | Result |
|---|---|
| `event_time_ms <= effective_watermark_ms` | `LATE_EVENT`; DLQ only. |
| Above the effective watermark, but older than the previous accepted maximum for that stay | `OUT_OF_ORDER_EVENT`; retained in the clean internal stream. |
| Above the effective watermark and not older than that maximum | `CLEAN_EVENT`; retained internally. |

Watermarks are independent between the numeric, clinical and waveform pipelines. Within each pipeline the watermark covers its input across stays; it is not an independent watermark for every patient. A newer stay/timeline can make an older stay's event late. During startup the watermark has not advanced yet; an idle source does not advance a bounded watermark just because wall-clock time passes. TICKET-007 may later define window-specific allowed lateness.

## Configuration and exact Kafka sink

Important settings are centralized in `jobs/flink/runtime_config.py`:

| Setting | Default / behavior |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Flink `icu-kafka:9094`; host replay retains `localhost:9092`. |
| `DLQ_TOPIC` | `icu.v1.deadletter`; this ticket rejects a different output-topic setting. |
| `MAX_OUT_OF_ORDERNESS_SECONDS` | 30; CLI override `--max-out-of-orderness-seconds`. |
| `DEDUP_STATE_TTL_HOURS` | 24; CLI override `--dedup-state-ttl-hours`. |
| `CHECKPOINT_INTERVAL_MS` | 10000; environment override inside the job container. |
| `DLQ_MAX_MESSAGE_BYTES` | 8388608 (8 MiB); shared by topic bootstrap and sink request sizing. |

The existing `kafka_topics.py --include-dlq` adds the DLQ through the project's existing bootstrap mechanism. It verifies one partition and replication factor one and sets this topic's `max.message.bytes` to 8388608. Without the flag the earlier three-RAW-topic behavior remains available. No other output topic is created.

The job uses Flink DataStream `KafkaSink` with:

```text
bootstrap.servers    = icu-kafka:9094 by default
topic                = icu.v1.deadletter
delivery guarantee   = AT_LEAST_ONCE
acks                 = all
request.timeout.ms   = 10000
delivery.timeout.ms  = 30000
max.request.size     = 8388608
record serializer    = org.icu.streaming.DeadLetterSerializationSchema
```

The larger DLQ request/topic limit accounts for lossless Base64 plus JSON text escaping of ordinary RAW inputs; a 450,000-byte malformed probe is included. It does not make arbitrary message sizes supported. Checkpoints flush pending sink records. There is no transactional exactly-once configuration or end-to-end exactly-once claim. Recovery/replay can repeat DLQ output; its deterministic `dlq_id` supports recognizing the same original rejection.

Python bundle size is set to 1 for prompt local validation and visible watermark behavior. This is a prototype correctness setting, not a throughput benchmark result.

The exact connector remains `org.apache.flink:flink-sql-connector-kafka:5.0.0-2.2`, SHA-256 `5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616`, on Flink/PyFlink 2.3.0. TICKET-005 established source-only compatibility. The existing upstream limitation described in [README.md](README.md#connector-compatibility-limit) still applies; local source/sink success must not be represented as official upstream Flink 2.3 support.

**TICKET-006 KafkaSink compatibility result: locally verified PASS.** The actual Flink sink published all 12 expected rejection envelopes to the existing broker, and an actual Kafka consumer verified them. Source and sink work in the tested Flink/PyFlink 2.3.0 environment with this exact connector binary. This is not an official upstream support claim.

## Exact DLQ envelope

All fields below are present. Version is `schema_version = "1.0.0"`.

| Field | Type / meaning |
|---|---|
| `schema_version` | String; DLQ contract version. |
| `dlq_id` | String, `dlq-` plus SHA-256 of compact JSON `[topic, partition, offset, error_type, raw_sha256, raw_payload_is_null]`. |
| `error_type` | `PARSE_ERROR`, `SCHEMA_ERROR`, `VALUE_ERROR`, `DUPLICATE_EVENT` or `LATE_EVENT`. |
| `error_reason` | String describing the failed check; late reasons include event-time and effective watermark in milliseconds. |
| `failed_at` | UTC wall-clock ISO timestamp with offset. |
| `original_topic` | String; original RAW topic. |
| `original_partition` | Integer; original Kafka partition. |
| `original_offset` | Integer; original Kafka offset. |
| `original_key` | Strict UTF-8 decoded original key, or null if the key is absent or not valid UTF-8. |
| `original_key_base64` | Exact original key bytes as Base64; null only for an absent key. |
| `stream_type` | `vitals`, `clinical` or `waveform`. |
| `event_id` | Recoverable nonempty, valid-Unicode string or null. |
| `stay_id` | Recoverable nonempty, valid-Unicode string or null. |
| `run_id` | Recoverable nonempty, valid-Unicode string or null. |
| `dataset_type` | Recoverable nonempty, valid-Unicode string or null. |
| `replay_session_id` | Recoverable nonempty, valid-Unicode string or null. |
| `event_time` | Recoverable nonempty, valid-Unicode original timestamp string or null; it can still be malformed if this caused rejection. |
| `signal_name` | Recoverable nonempty, valid-Unicode string or null. |
| `event_type` | Recoverable nonempty, valid-Unicode string or null. |
| `raw_payload` | Exact strict UTF-8 text, or null for invalid UTF-8 or a Kafka tombstone. |
| `raw_payload_base64` | Exact original value bytes as Base64, or null for a Kafka tombstone. |
| `raw_payload_is_null` | Boolean; distinguishes a Kafka tombstone from an empty byte value. |
| `raw_sha256` | Hex SHA-256 of original value bytes; tombstones use the empty-byte digest, with the explicit null flag preserving the distinction. |

Malformed JSON has no parsed object from which to recover identity fields, even if a partial payload happens to contain their spelling. Its raw bytes and Kafka metadata remain available. There is no required `original_event` field because exact text/Base64 preserve the original payload. Parsed data is not reserialized as a substitute for the original bytes.

DLQ Kafka key precedence is: recoverable `stay_id` encoded as UTF-8; otherwise the exact original Kafka key bytes, including a valid empty key; otherwise `dlq_id` encoded as UTF-8. The Java serializer implements this precedence. The extended verification consumer checks it.

Logs distinguish `CLEAN_EVENT`, `OUT_OF_ORDER_EVENT` and `DLQ_EVENT`. They include source topic/partition/offset and recoverable identities, signal/event type and event-time. Clean waveform logs include a five-sample preview rather than complete sample arrays. Rejection logs include reasons and hashes without dumping original payloads. Full original bytes remain in Kafka DLQ and the Git-ignored evidence.

## Automated and live verification

Automated verification: **115 tests PASS**: 84 Flink contract/runtime tests, 19 replay tests, 5 mapping tests and 7 Reference tests. Existing TICKET-005 tests remain included in the Flink suite.

Coverage includes strict parsing, schema/value bounds, null availability preservation, waveform structure/null samples, deterministic identities, duplicate behavior for all three streams and the waveform fallback, logical timestamp extraction, watermark-boundary ordering, lossless DLQ metadata/hash/key behavior, Unicode-surrogate rejection and larger DLQ sizing.

**Live controlled-probe verification: PASS**, job `7bfc6ca55b88e1eb46b7b9625ca3b586`, 17 published probes, 5 accepted clean records and 12 actual Kafka DLQ records. All accepted/rejected routing, source metadata, original payload bytes, Kafka keys and hashes matched the published inputs. Rejected probes did not enter the clean stream. The job remained RUNNING with no reported runtime exceptions.

| Observed case | Result |
|---|---|
| Valid MAP 62 | Clean; no DLQ. |
| Clinical explicit null availability | Clean; null preserved. |
| Valid waveform including null sample | Clean; structure and sample preview preserved. |
| Older event within the 30-second tolerance | `OUT_OF_ORDER_EVENT`; still clean. |
| Waveform without event ID | Deterministic composite identity; first chunk clean. |
| Malformed JSON, invalid UTF-8, 450,000-byte malformed payload, tombstone | Four `PARSE_ERROR` DLQ records. |
| Missing stay ID, unpaired-surrogate event ID | Two `SCHEMA_ERROR` DLQ records. |
| MAP -500 | One `VALUE_ERROR` DLQ record. |
| Numeric, clinical, waveform event ID and waveform composite duplicate | Four `DUPLICATE_EVENT` DLQ records. |
| Event older than effective watermark | One `LATE_EVENT` DLQ record. |

The numeric classifier's actual REST input watermark was `6020338395999`, equal to anchor event-time `6020338426000 - 30001` milliseconds. Checkpoints completed increased from 5 to 7 during verification. Failed checkpoint count stayed at 1: the startup failure occurred before all tasks were running, and no new checkpoint failure occurred during the probes. Dedup checkpoint state sizes grew in all three streams:

| Managed dedup stream | Before | After |
|---|---:|---:|
| Numeric | 3148 bytes | 4745 bytes |
| Clinical | 3148 bytes | 4595 bytes |
| Waveform | 3148 bytes | 4695 bytes |

An initial integration run exposed PyFlink's intercepted `print` incompatibility with the `flush` keyword. Logging was corrected before the successful run above; the failed attempt is retained separately for audit.

**Actual Reference-producer smoke verification: PASS.** A fresh runtime submission accepted 6 numeric, 3 clinical and 54 waveform messages from the existing Reference Replay Producer: 63 clean records and 0 DLQ records. IDs, all three timestamp fields, clinical null availability and waveform sample previews matched actual messages consumed from the RAW topics. This check used the existing canonical dataset and on-demand waveform replay; it did not generate or rewrite a Reference dataset.

The final job left for review is `d9002314132948bc7b2927ce54f33751`, RUNNING. At the final captured check it had 12 completed checkpoints, one startup checkpoint failure before all tasks were ready, and zero reported runtime exceptions. The fresh submission separates the historical Reference timeline from the year-2160 probe watermark.

**Protected-data verification: PASS.** All 15,038 raw paths, sizes and nanosecond modification times matched the pre-ticket baseline. All 20 protected prior output SHA-256 hashes, including the canonical Reference outputs, remained unchanged. The final evidence is recorded in `data/working/runtime/verification_report.json` and `verification_summary.md`.

The probe harness is a small ticket-specific verifier, not the later full Engineering Scenario Generator. It publishes labeled engineering records with unique session IDs, `dataset_type = ENGINEERING_PROBE`, `test_mode = TICKET006` and virtual identity metadata. It never changes canonical Reference files. Malformed probes remain labeled where their payload format permits.

It tests valid MAP 62, malformed JSON, missing stay ID, negative MAP, numeric/clinical/waveform duplicates, clinical null availability, valid waveform with a null sample, tolerated out-of-order, late input, invalid UTF-8/binary key, larger malformed payload, tombstone/fallback key, unpaired-surrogate identity, and waveform composite-ID acceptance/duplication. It gates the ordering probe on the classifier's real REST watermark metric rather than assuming an arbitrary sleep is sufficient. It verifies a completed checkpoint triggered after observed outputs and increased checkpointed state at each managed dedup operator.

## Manual verification commands (PowerShell)

Run from the project root with Docker Desktop running. Use the existing broker. During the controlled probes, run no concurrent replay and no second Flink job.

```powershell
Set-Location 'D:\Data Engineering\ICU_Project\icu-streaming-project'
$icuPython = '.\.venv\Scripts\python.exe'

# Preserve/start the same Kafka container and its internal listener.
& $icuPython -B docker/flink/prepare_kafka.py
& $icuPython -B scripts/replay/kafka_topics.py --include-dlq

# Required once for this image upgrade; skip on ordinary later startups.
docker compose -f docker/flink/compose.yaml build jobmanager
docker compose -f docker/flink/compose.yaml up -d --no-build
docker compose -f docker/flink/compose.yaml ps
Invoke-RestMethod http://localhost:8081/overview
```

Confirm `taskmanagers = 1`. The one slot must be free before submitting a new copy. Cancel an existing project inspection/runtime job if it is still running:

```powershell
$icuExistingJobs = @((Invoke-RestMethod http://localhost:8081/jobs/overview).jobs |
    Where-Object { $_.state -eq 'RUNNING' -and $_.name -match 'TICKET-005|TICKET-006' })
foreach ($icuExistingJob in $icuExistingJobs) {
    docker compose -f docker/flink/compose.yaml exec -T jobmanager /opt/flink/bin/flink cancel $icuExistingJob.jid
}

docker compose -f docker/flink/compose.yaml exec -T jobmanager /opt/flink/bin/flink run -d -py /opt/flink/jobs/runtime_pipeline.py --stream all --offset latest
docker compose -f docker/flink/compose.yaml exec -T jobmanager /opt/flink/bin/flink list -r

$icuRuntimeJobs = @((Invoke-RestMethod http://localhost:8081/jobs/overview).jobs |
    Where-Object { $_.state -eq 'RUNNING' -and $_.name -like '*TICKET-006*' })
if ($icuRuntimeJobs.Count -ne 1) { throw 'Expected one RUNNING TICKET-006 job' }
$icuJobId = $icuRuntimeJobs[0].jid
```

Wait for the first completed checkpoint before publishing probes. This bounded check waits up to 60 seconds:

```powershell
$icuReadyDeadline = (Get-Date).AddSeconds(60)
do {
    $icuCheckpoints = Invoke-RestMethod "http://localhost:8081/jobs/$icuJobId/checkpoints"
    if ($icuCheckpoints.counts.completed -gt 0) { break }
    Start-Sleep -Seconds 2
} while ((Get-Date) -lt $icuReadyDeadline)
if ($icuCheckpoints.counts.completed -eq 0) { throw 'No completed checkpoint; inspect job/logs first' }

& $icuPython -B scripts/runtime/verify_runtime.py --help
& $icuPython -B scripts/runtime/verify_runtime.py --job-id $icuJobId --timeout-seconds 120 --tolerance-seconds 30 --output data/working/runtime/manual
if ($LASTEXITCODE -ne 0) { throw 'Runtime probe verification failed; read the manual probe report' }
```

The harness starts consuming DLQ at its current end, so its report identifies this run rather than older retained test records. Inspect the actual Kafka output with the existing extended consumer:

```powershell
$icuManualReport = Get-Content -LiteralPath data/working/runtime/manual/probe_report.json -Raw | ConvertFrom-Json
& $icuPython -B scripts/replay/consume.py --topic icu.v1.deadletter --offset $icuManualReport.dlq_start_offset --max-messages $icuManualReport.dlq_count --timeout 30

Invoke-RestMethod "http://localhost:8081/jobs/$icuJobId"
Invoke-RestMethod "http://localhost:8081/jobs/$icuJobId/checkpoints"
Invoke-RestMethod "http://localhost:8081/jobs/$icuJobId/exceptions?maxExceptions=5"
docker compose -f docker/flink/compose.yaml logs --tail 300 taskmanager

& $icuPython -B -m unittest discover -s tests/flink -v
& $icuPython -B -m unittest discover -s tests/replay -v
& $icuPython -B scripts/mapping/test_mapping.py
& $icuPython -B scripts/reference/test_reference.py
```

The probe timeline is in year 2160 so it can be controlled independently of the historical Reference data. Before demonstrating historical Reference replay afterward, cancel this probe job and submit a fresh `--offset latest` runtime job, wait for its first completed checkpoint, and then run the existing replay commands. Otherwise the advanced watermark may correctly reject the older Reference timeline as late.

```powershell
docker compose -f docker/flink/compose.yaml exec -T jobmanager /opt/flink/bin/flink cancel $icuJobId
docker compose -f docker/flink/compose.yaml exec -T jobmanager /opt/flink/bin/flink run -d -py /opt/flink/jobs/runtime_pipeline.py --stream all --offset latest

# After the new job is RUNNING with a completed checkpoint:
& $icuPython -B scripts/replay/replay.py --stream vitals --speed 10 --stay-id 37323533 --max-events 6
& $icuPython -B scripts/replay/replay.py --stream clinical --speed 10 --stay-id 37323533 --max-events 3
& $icuPython -B scripts/replay/replay.py --stream waveform --speed 10 --waveform-seconds 10
```

`--offset latest` selects the end when sources initialize, so submitting before publishing matters. `--offset earliest` intentionally reprocesses retained RAW history, including earlier probes; it is not a checkpoint restore or a clean Reference-only input.

## Restart, recovery and limits

For ordinary startup, run the existing prepare script, topic bootstrap and `compose up -d --no-build`, then explicitly submit the desired job. To stop Flink, cancel its selected job and run `docker compose -f docker/flink/compose.yaml stop`. Kafka remains running. A new ordinary submission starts fresh managed state and chooses the requested starting offsets. The group name does not restore Flink dedup state.

Normal cancellation retains the TICKET-005 default checkpoint cleanup behavior: completed checkpoints are not externalized recovery artifacts after cancellation. The named volume surviving a container stop is not by itself sufficient to restore a cancelled job. Restoring managed state requires an explicitly retained compatible checkpoint/savepoint and a restore submission; this ticket does not claim that path was exercised. No HA or automatic session-job resubmission after JobManager restart is configured.

The existing Kafka log directory remains in that broker container's writable layer; restart preserves it, but removing/recreating the Kafka container does not. Do not delete that broker or its data. Durability migration is outside this ticket.

This ticket is limited to local correctness with parallelism 1. It does not prove high load, permanent deduplication, arbitrary payload sizes, official upstream connector compatibility, end-to-end exactly-once delivery, or clinically validated thresholds. Probe records remain in Kafka and are visibly labeled engineering data. Separate job submissions isolate probe/reference event-time histories but do not delete retained inputs.

Raw MIMIC and canonical Reference files were not modified. Verification confirmed unchanged path/size/mtime_ns metadata for all 15,038 raw files and unchanged SHA-256 hashes for all 20 protected prior output files, including the canonical Reference data. The raw check is a metadata comparison, not a full byte hash of the waveform corpus. This implementation and its probe harness write only project code/documentation, working evidence and the authorized Kafka messages/topic configuration.

## Short manual PASS / FAIL checklist

| Check | Result |
|---|---|
| Same Kafka broker; host `localhost:9092` and Flink `icu-kafka:9094` both work | ___ PASS / FAIL |
| One Flink job RUNNING, one TaskManager, checkpoints continue completing | ___ PASS / FAIL |
| Valid MAP 62, clinical null availability and valid waveform enter clean stream unchanged | ___ PASS / FAIL |
| Parse/schema/value failures reach actual Kafka DLQ; negative MAP never enters clean stream | ___ PASS / FAIL |
| Numeric, clinical and waveform duplicates reach DLQ and remain excluded from clean stream | ___ PASS / FAIL |
| Within-tolerance out-of-order stays clean; beyond-effective-watermark event reaches DLQ | ___ PASS / FAIL |
| DLQ metadata, original bytes, hash and key precedence match the actual Kafka input | ___ PASS / FAIL |
| Bad records, invalid UTF-8, surrogate ID and larger payload do not crash the job | ___ PASS / FAIL |
| Automated suites pass; managed dedup state is included in post-probe checkpoint evidence | ___ PASS / FAIL |
| Raw/canonical integrity evidence passes; no TICKET-007 work introduced | ___ PASS / FAIL |

Stop after TICKET-006. No 5m/15m windows, sustained-hypotension rule, other business alerts, processed/aggregate/alert topics, TimescaleDB/storage writer, Power BI, Alert Receiver, Cassandra, Kafka Streams, load benchmark or full scenario generator has been implemented.
