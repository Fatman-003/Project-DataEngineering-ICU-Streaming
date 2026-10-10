# TICKET-004 replay and RAW topic verification

Canonical Parquet / referenced WFDB -> Python replay -> Kafka RAW topics -> verification consumer.

This ticket uses the existing broker and .venv. It implements no downstream processor, storage sink, alerts, UI, or infrastructure installation. Raw MIMIC and data/reference are opened for reading only. TICKET-001/002/003 scripts and outputs are unchanged.

## Files

- config.py: shared literal .env / environment settings and RAW topic names.
- kafka_topics.py: idempotent topic bootstrap and partition/replication checks.
- serializers.py: JSON, stay_id key, runtime UTC producer timestamp.
- replay.py: one producer implementation, deterministic ordering, monotonic pacing, delivery callbacks, flush and run summary.
- waveforms.py: lazy frame-aligned chunk metadata and bounded WFDB decoding.
- consume.py: bounded manual inspection with partition/offset/key and event fields.
- README.md: these instructions and manual checklist.
- ../../tests/replay/test_replay.py: broker-free tests.
- ../../data/working/replay/verification_report.json and verification_summary.md: executed verification evidence (generated, ignored by Git).

No existing project file was modified. The updated database proposal was supplied by the user and read as context.

## Configuration

Run commands from the project root in PowerShell:

```powershell
Set-Location 'D:\Data Engineering\ICU_Project\icu-streaming-project'
$env:KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
```

Environment variables override literal project .env values. Kafka defaults centrally to localhost:9092 if unset. REFERENCE_DATA_ROOT defaults to data/reference. MIMIC_WAVEFORM_ROOT comes from the existing .env and is required for waveform decoding. Relative paths resolve against the project root, even when invoked elsewhere.

Do not add .env to Git. There are no new package installations or broker startup commands.

## Exact bounded smoke-test commands

Bootstrap twice: the second call reports EXISTS, with one partition and one replica per topic.

```powershell
.\.venv\Scripts\python.exe -B scripts/replay/kafka_topics.py
.\.venv\Scripts\python.exe -B scripts/replay/kafka_topics.py

.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream vitals --speed 10 --stay-id 37323533 --max-events 6
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream clinical --speed 10 --stay-id 37323533 --max-events 3
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream waveform --speed 10 --waveform-seconds 10

.\.venv\Scripts\python.exe -B scripts/replay/consume.py --topic icu.v1.vitals.numeric --max-messages 6
.\.venv\Scripts\python.exe -B scripts/replay/consume.py --topic icu.v1.clinical.events --max-messages 3
.\.venv\Scripts\python.exe -B scripts/replay/consume.py --topic icu.v1.vitals.waveform --max-messages 4
```

The numeric prefix contains HR, RR, MAP, DBP, SPO2 and SBP. Its 120-second historical gap takes approximately 12 seconds at x10. The initial clinical prefix contains three GCS observations at one timestamp. The waveform window includes six actual signal names (II, ABP, Pleth, Resp, V, aVR), with channels changing across segments.

Check other speeds with short waveform windows and the numeric gap at x60:

```powershell
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream waveform --speed 1 --waveform-seconds 2
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream waveform --speed 60 --waveform-seconds 2
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream vitals --speed 60 --stay-id 37323533 --max-events 6
```

Check unknown clinical availability, LAB, and URINE:

```powershell
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream clinical --speed 60 --stay-id 37323533 --start-time 2143-09-20T16:00:00 --max-events 3
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream clinical --speed 60 --stay-id 37323533 --start-time 2143-09-20T01:36:00 --max-events 2
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream clinical --speed 60 --stay-id 39864867 --start-time 2148-08-16T13:33:00 --max-events 1
```

Check waveform data from the other two stays:

```powershell
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream waveform --speed 60 --stay-id 35727289 --waveform-seconds 10
.\.venv\Scripts\python.exe -B scripts/replay/replay.py --stream waveform --speed 60 --stay-id 39864867 --waveform-seconds 10
```

Consumers default to earliest retained offset, never commit offsets, and stop after the requested count or a 15-second total timeout. Old runs stay in Kafka. Match replay_session_id with the producer summary to identify a run; use --offset N to start at a known partition-0 offset. --offset latest starts at the high watermark when the consumer starts, so start that consumer before a new replay. Increase --timeout for long runs. A timeout, malformed JSON, mismatched key, or Kafka error gives a nonzero exit.

## Replay semantics and assumptions

- Numeric payloads retain every canonical field. Only producer_time is replaced with the actual UTC send timestamp. Canonical run_id and event_id stay unchanged; replay_session_id distinguishes executions.
- Historical timestamps remain timezone-naive ISO strings, as in MIMIC. They are not assigned UTC or shifted. producer_time is an aware UTC ISO timestamp with +00:00.
- Numeric ordering/pacing uses canonical event_time. Events sort by (event_time, event_id). The producer uses absolute monotonic deadlines: historical elapsed seconds / speed, avoiding accumulated serialization drift. Slow decoding/network can make a replay late; there is no guarantee of sub-millisecond pacing.
- Clinical source_event_time is chart_time. With verified availability, event_time/pacing uses available_time, preventing early lab release.
- Unknown clinical availability remains JSON null, together with its canonical availability_status and validation_flags (plural, the actual canonical field). These rows are transported as raw observations at chart_time with replay_time_basis=chart_time_unknown_availability. This pacing timestamp is not evidence of availability. Downstream correlation must withhold their clinical use until availability is verified.
- No source time, available_time, measurement, unit, source identifier, or canonical validation flag is repaired. No source duplicates are removed.
- Without --stay-id, numeric/clinical runs preserve the complete global historical timeline, including gaps of several years between stays. Use --stay-id for practical runs. Separate streams are independent invocations; a synchronized multi-stream clock is outside this ticket.
- --start-time is an inclusive timezone-naive historical lower bound on the stream's pacing timestamp. For waveform it defines the requested window start, constrained to the earliest available manifest time.
- --waveform-seconds defaults to 10 and limits one global historical window after stay filtering, rather than replaying 10 seconds separately from every manifest segment. Without a stay filter the current first window covers P001 only. Increase the limit explicitly for longer runs.
- Waveform chunks use approximately 1 second of frames, with shorter segment/window tails. Integer boundaries at 62.4725 frames/s alternate 62/63 frames. The last partial frame is omitted at the duration bound, at most one frame (about 16 ms).
- WFDB smooth_frames=False retains expanded native samples: ECG 249.89 Hz, ABP/Pleth 124.945 Hz, Resp 62.4725 Hz. Decoding requests only the chunk's frame interval and selected channel. A compressed file may contain other channels, but there is no full-record sample materialization or waveform Parquet output.
- Missing waveform samples become JSON null with missing_sample_count and MISSING_SAMPLES quality; no interpolation or filtering. These are distinct from missing files. Missing/out-of-root input files or a header/manifest mismatch fail the run.
- Waveform IDs deterministically identify the record/segment/channel/frame interval. Message metadata preserves manifest provenance and adds chunk frame/sample bounds and start/end times. Sample intervals are half-open.
- Producer uses idempotence and delivery acknowledgements within a run. Re-running intentionally appends the same canonical event IDs; cross-run deduplication is a later processor responsibility. Messages use the UTF-8 stay_id as their Kafka key.
- Normal replay is paced; there is no burst mode. --max-events bounds a smoke test. Ctrl+C flushes queued events and exits 130. Delivery errors, decode failures, pending messages after flush, or bootstrap errors exit nonzero and appear in the JSON summary. Queue-full retries are bounded to 30 seconds, delivery timeout to 10 seconds, final flush to 15 seconds.

## Automated checks

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests/replay -v
.\.venv\Scripts\python.exe -B scripts/mapping/test_mapping.py
.\.venv\Scripts\python.exe -B scripts/reference/test_reference.py
```

Executed: 19 replay tests, 5 mapping tests, 7 reference tests; all passed. Live verification read back 12 numeric, 9 clinical and 169 waveform messages from the raw topics. All 905 numeric / 232 clinical input rows were also ordered and serialized offline; all 23 canonical unknown availability values stayed null. Live coverage is deliberately bounded, not a full-duration replay.

The live readback checked keyed JSON, all canonical numeric/clinical fields, runtime timestamps, ordering within each session, waveform sample counts/duration bounds, and four waveform chunks against an independent raw decode. Three live clinical messages retained null availability. The 14 prior outputs retained their SHA-256 hashes; all 15,038 raw file paths/sizes/mtime values matched the pre-ticket snapshot. This raw check is metadata comparison, not a full content hash of every waveform file.

Observed numeric pacing: 120 historical seconds took 12.002 seconds at x10 and 2.020 seconds at x60. The x1 waveform next-chunk send gap was 0.948 seconds for a 0.992-second historical gap; chunk decode overhead explains the difference. This is functional verification, not a performance benchmark.

## Manual checklist (fill PASS / FAIL)

| Check | Result |
|---|---|
| Bootstrap both times succeeds; topics have 1 partition / replica | ___ |
| Numeric prefix publishes and consumer reads HR/RR/MAP/DBP/SPO2/SBP | ___ |
| Clinical replay supports GCS, LAB and URINE | ___ |
| Waveform short window publishes chunks and consumer reads them | ___ |
| Every key equals stay_id; numeric/clinical payloads are JSON | ___ |
| producer_time is current UTC; historical source times stay unchanged | ___ |
| Available clinical events are paced by available_time | ___ |
| Unknown available_time stays null and validation_flags stay present | ___ |
| x1 / x10 / x60 pace according to historical gaps | ___ |
| Repeated run event ordering/IDs are deterministic | ___ |
| Waveform chunks contain native-rate samples, with explicit missing counts | ___ |
| --waveform-seconds bounds the requested historical window | ___ |
| Summary reports published/failed/pending and sample/chunk totals | ___ |
| Broker-free delivery failure / backpressure / pending-flush tests pass | ___ |
| Ctrl+C ends a long replay with a flushed INTERRUPTED summary | ___ |
| Prior ticket tests pass; raw/canonical files remain unchanged | ___ |
| No downstream implementation added; stop at TICKET-004 | ___ |

Stop here for review before TICKET-005.
