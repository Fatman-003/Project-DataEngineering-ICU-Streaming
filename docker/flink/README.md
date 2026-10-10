# TICKET-005 — Flink streaming foundation

TICKET-005 is complete and reviewed PASS. This document records that foundation.
For the current TICKET-006 runtime job, DLQ contract, live verification and manual
commands, see [Runtime validation guide](RUNTIME_VALIDATION.md). The original
`raw_consumer.py` remains available as a separate diagnostic job. Submit one job
at a time on the shared single slot; historical job IDs below may be canceled.

Foundation scope: host Python replay -> the existing ONE Kafka broker -> PyFlink DataStream KafkaSource -> string JSON -> inspection -> print. No output topic, database sink, validation rules, deduplication, watermark strategy, windows, alerts, scenario generator, or TICKET-006 work was included in TICKET-005.

## Files created

Project sources:
- docker/flink/Dockerfile
- docker/flink/compose.yaml
- docker/flink/prepare_kafka.py
- docker/flink/README.md
- jobs/flink/contracts.py
- jobs/flink/raw_consumer.py
- tests/flink/test_contracts.py

Generated, Git-ignored evidence:
- data/working/flink/kafka_server_properties.before.txt
- data/working/flink/inspected_events.jsonl
- data/working/flink/verification_report.json
- data/working/flink/verification_summary.md

No pre-existing repository file was modified. No raw or canonical data was rewritten. The host .venv was not changed.

## Docker architecture and versions

- Base image: flink:2.3.0, pinned additionally to digest sha256:df6b153c93d3e60f4b96b323979bf0305861949a600fcee287214609d325037e.
- PyFlink: apache-flink==2.3.0 installed inside /opt/pyflink, in the image only.
- Container Python: 3.12.3 in the tested build. Host replay keeps Python 3.12.2.
- Custom image: icu-pyflink:2.3.0-kafka5.0.0-2.2.
- One JobManager, one TaskManager, one slot, parallelism 1. Three source/inspection pipelines share that slot.
- UI: http://localhost:8081, published on host loopback only.
- Jobs bind mount: jobs/flink -> /opt/flink/jobs, read-only. Raw MIMIC and Reference data are not mounted into Flink.
- Shared external Docker bridge: icu-streaming.
- Named checkpoint volume: icu-flink-checkpoints, mounted at /opt/flink/checkpoints in both containers.
- No latest tags, second broker, host Flink install, host Java dependency, or manually copied runtime JAR.

### Connector compatibility limit

Exact Maven artifact: org.apache.flink:flink-sql-connector-kafka:5.0.0-2.2.

SHA-256: 5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616.

The Dockerfile downloads and checksums the shaded JAR into Flink's lib directory at build time. Despite its SQL artifact name, it also contains the DataStream KafkaSource and Kafka client dependencies used here.

As of verification on 2026-10-10, Apache's [Flink 2.3 Kafka documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kafka/) explicitly says there is no 2.3 connector artifact yet. Apache's [downloads compatibility list](https://flink.apache.org/downloads/#apache-flink-kafka-connector-500) lists connector 5.0.0 for Flink 2.1/2.2. The 5.0.0-2.2 artifact is therefore a documented, locally verified compatibility choice for this 2.3.0 source/inspection foundation, not an official upstream 2.3 support claim. Actual consumption of all three topics, checkpoints, cancellation and resubmission passed. Retest/replace the artifact when upstream publishes a 2.3 release; this ticket does not establish sink or business-job compatibility.

Python's direct Flink dependency, base-image digest, connector version and connector checksum are pinned. Transitive Python/Ubuntu package resolution is not a byte-for-byte dependency lock.

The custom Python image follows the [official Flink Docker deployment approach](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/resource-providers/standalone/docker/).

## How Flink reaches the existing broker

The original icu-kafka container used:
- listeners=PLAINTEXT://:9092,CONTROLLER://:9093
- advertised.listeners=PLAINTEXT://localhost:9092

A container connecting via a Docker gateway would still receive localhost:9092 in Kafka metadata, which points at itself. The setup adds INTERNAL://:9094, advertised as INTERNAL://icu-kafka:9094. The original host listener remains PLAINTEXT://localhost:9092.

Result:
- Host replay -> localhost:9092 -> same broker, host-advertised endpoint.
- Flink -> icu-kafka:9094 -> same broker, Docker-advertised endpoint.

prepare_kafka.py inspects the existing apache/kafka:4.2.2 container, creates/attaches the shared network if needed, saves the original server properties under working data, and changes only the three listener-related properties. Unexpected broker identity or listener layouts are rejected. It writes the resulting server.properties to the existing /mnt/shared/config volume, which Kafka's startup wrapper reads. It restarts the SAME container once if configuration changed. No container recreation, topic deletion, raw/canonical changes, or data-directory migration occurs. Its second execution reported restarted=false.

Container ID before/after: 273446ff3e7a3827bc9f7949912279ee31c16fcfaf69ea14bbadc257b8900c27.

The original broker stores logs under /tmp/kraft-combined-logs in its container writable layer. That survives restart, but not container deletion. Do not remove/recreate this Kafka container or delete its volumes. Migration to a project-owned durable Kafka deployment is outside this foundation. The setup can start the existing stopped Kafka container after reboot; it cannot reconstruct a deleted broker.

## Start and submit (PowerShell)

Run from the project root. Docker Desktop must be running.

```powershell
Set-Location 'D:\Data Engineering\ICU_Project\icu-streaming-project'
$icuPython = '.\.venv\Scripts\python.exe'

# Idempotent networking; starts the existing broker if stopped.
& $icuPython -B docker/flink/prepare_kafka.py

# First use only, or after Dockerfile/dependency changes.
docker compose -f docker/flink/compose.yaml build jobmanager

# Normal startup after that: no rebuild.
docker compose -f docker/flink/compose.yaml up -d --no-build
docker compose -f docker/flink/compose.yaml ps
Invoke-RestMethod http://localhost:8081/overview

docker compose -f docker/flink/compose.yaml exec -T jobmanager /opt/flink/bin/flink run -d -py /opt/flink/jobs/raw_consumer.py --stream all --offset earliest
docker compose -f docker/flink/compose.yaml exec -T jobmanager /opt/flink/bin/flink list -r
```

Wait for overview.taskmanagers=1, then verify the submitted job is RUNNING. The submission prints its JobID. Open http://localhost:8081 to inspect the job and TaskManager.

Do not submit a second copy while the first is running: the single slot is sized for one foundation job. Use --stream vitals, clinical, or waveform if inspecting only one topic.

Default --offset earliest reinspects retained messages on each new submission, even with an existing consumer group. --offset latest starts at the end when the source initializes; wait for partition assignment before replaying, or use earliest to avoid the startup race. There is no detailed event-time policy or watermark assignment: WatermarkStrategy.no_watermarks() is explicit. Timestamps remain JSON strings exactly as sent.

Configuration:
- Flink's KAFKA_BOOTSTRAP_SERVERS is set centrally in compose to icu-kafka:9094.
- --bootstrap-servers overrides that value for a job.
- --group-id defaults to icu-flink-raw-inspect.
- The host Replay Producer keeps its existing .env/environment configuration and localhost:9092 default.

## Run the existing host replay (another terminal)

```powershell
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream vitals --speed 10 --stay-id 37323533 --max-events 6
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream clinical --speed 10 --stay-id 37323533 --max-events 3
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream waveform --speed 10 --waveform-seconds 10
```

Expect six numeric events, three clinical GCS events and 54 waveform chunks for these exact current Reference inputs. Match replay_session_id between producer summaries and Flink output.

Additional availability/LAB/URINE checks executed:

```powershell
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream clinical --speed 60 --stay-id 37323533 --start-time 2143-09-20T16:00:00 --max-events 3
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream clinical --speed 60 --stay-id 37323533 --start-time 2143-09-20T01:36:00 --max-events 2
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream clinical --speed 60 --stay-id 39864867 --start-time 2148-08-16T13:33:00 --max-events 1
```

## Inspect logs and checkpoints

```powershell
docker compose -f docker/flink/compose.yaml logs --tail 300 taskmanager
docker compose -f docker/flink/compose.yaml logs -f taskmanager

$icuJobId = (Invoke-RestMethod http://localhost:8081/jobs/overview).jobs |
    Where-Object { $_.state -eq 'RUNNING' -and $_.name -like 'ICU RAW JSON inspection*' } |
    Select-Object -First 1 -ExpandProperty jid
Invoke-RestMethod "http://localhost:8081/jobs/$icuJobId/checkpoints"
```

RAW_EVENT lines show stream/topic, stay_id/event_id, measurement fields and unchanged timestamps. Clinical available_time=null remains null, with the original availability_status and validation_flags. Waveform lines show record, signal, interval, sampling frequency, sample count, missing count and a five-sample preview. Full samples are decoded as JSON, but are not dumped into logs. raw_sha256 identifies the exact Kafka payload inspected.

PARSE_ERROR lines contain an explicit error, raw payload digest and bounded preview. They are inspection output, not valid events or a DLQ. No business-validity claim is made for RAW_EVENT. Valid JSON objects with invalid clinical values are intentionally not rejected in this ticket.

One malformed JSON probe was deliberately appended to the numeric raw topic during verification, with key 37323533. It remains in Kafka and produces PARSE_ERROR when earliest history is reread. Real replay events immediately afterward were still decoded, and the job had no exceptions. This was a single parsing check, not a generated scenario dataset. Kafka connection failures appear in the source's Java client/JobManager/TaskManager logs, with the endpoint and exception.

Checkpointing is enabled every 10 seconds. File-system checkpoint storage is file:///opt/flink/checkpoints on the shared named Docker volume. Real completed checkpoints were verified through REST and on disk. Normal cancellation uses Flink's default cleanup; checkpoints are not retained as externalized recovery artifacts on cancellation. The volume itself survives stop/restart. This is a local checkpoint foundation, not HA, automatic job recovery after JobManager restart, or an end-to-end exactly-once claim. The print sink is not transactional and logs can repeat.

## Cancel and stop

```powershell
$icuJobId = (Invoke-RestMethod http://localhost:8081/jobs/overview).jobs |
    Where-Object { $_.state -eq 'RUNNING' -and $_.name -like 'ICU RAW JSON inspection*' } |
    Select-Object -First 1 -ExpandProperty jid
docker compose -f docker/flink/compose.yaml exec -T jobmanager /opt/flink/bin/flink cancel $icuJobId
docker compose -f docker/flink/compose.yaml stop
```

These stop only Flink. Kafka stays running. To restart, rerun prepare_kafka.py and compose up -d --no-build, then submit the job again. Session-cluster job recovery across JobManager restarts is not configured. Do not use compose down -v if you want to keep the checkpoint volume.

Current reviewed job after the executed cancellation/restart test: 6ab672ea5d728776dede78e5ed9f5626, RUNNING. Both Flink containers and the original Kafka broker were left running for inspection.

## Automated and live results

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests/flink -v
.\.venv\Scripts\python.exe -B -m unittest discover -s tests/replay -v
.\.venv\Scripts\python.exe -B scripts/mapping/test_mapping.py
.\.venv\Scripts\python.exe -B scripts/reference/test_reference.py
```

41 tests passed: 10 foundation, 19 replay, 5 mapping, 7 reference.

Live foundation verification passed:
- Existing broker was restarted in place, and all three raw topics remained present.
- One JobManager, one registered TaskManager; Flink version 2.3.0; UI REST reachable.
- PyFlink version 2.3.0 inside the custom image.
- Fresh host replay -> Kafka -> Flink: 6 numeric, 9 clinical, 54 waveform events.
- All matching log fields, source/logical/producer timestamp strings, null availability and waveform preview/counts matched actual Kafka messages.
- Three fresh clinical null availability events stayed null.
- One malformed JSON probe was logged as PARSE_ERROR; subsequent events still flowed.
- First job: 21 checkpoints completed, zero failed at the captured live verification point.
- First job cancelled cleanly; Flink containers stopped/restarted without rebuilding.
- New job submitted successfully, RUNNING with completed checkpoints and no exceptions.
- 28 pre-existing source/output files retained their SHA-256 hashes; all 15,038 raw file paths/sizes/mtime values matched the baseline. This raw check is metadata comparison, not full waveform byte hashing.
- No processed topic, alert, DLQ routing, database/BI component, or TICKET-006 logic was added.

## Manual PASS / FAIL checklist

| Check | Result |
|---|---|
| Existing Kafka broker identity retained; host replay still works | ___ |
| Flink network endpoint is icu-kafka:9094; host endpoint localhost:9092 | ___ |
| Image rebuild installs PyFlink 2.3.0 and checks pinned connector hash | ___ |
| One JobManager and one registered TaskManager, parallelism 1 | ___ |
| UI http://localhost:8081 reachable; job RUNNING | ___ |
| All three RAW topics produce decoded verification lines | ___ |
| Numeric / GCS / LAB / URINE / waveform fields inspect correctly | ___ |
| stay_id and event identifiers are visible | ___ |
| source_event_time / event_time / producer_time are unchanged | ___ |
| Clinical null available_time and validation_flags remain intact | ___ |
| Malformed JSON is a visible PARSE_ERROR; job continues | ___ |
| 10-second checkpoints complete in the mounted local volume | ___ |
| Cancel -> stop -> restart -> resubmit succeeds without rebuilding | ___ |
| All 41 automated tests pass | ___ |
| Prior source/output hashes and raw metadata remain unchanged | ___ |
| Connector's upstream 2.3 support limit is understood | ___ |
| No TICKET-006+ logic introduced | ___ |

Stop after TICKET-005 for review.
