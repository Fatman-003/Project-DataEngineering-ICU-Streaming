"""Real MIMIC identities plus deterministic generated telemetry; no source measurements."""
import argparse
import math
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "jobs/flink"))
from contracts import SIGNALS, TOPICS, stay_key
from dlq import encode
from registry import load_patients
from validation import parse_timestamp

PATIENTS = load_patients()
DEFAULT_START = datetime(2026, 10, 16, 20, tzinfo=timezone.utc)
SCENARIO_SECONDS = 360
DEFAULT_DURATION_MINUTES = 6
DEFAULT_INTERVAL_SECONDS = 1
EDGE_SECONDS = {"ooo": 45, "duplicate": 120, "invalid": 210, "late": 300}
# FastVerify inputs, not alternate alert logic. P02 proves the inclusive gap
# boundary; P04 would falsely alert without resetting at the 91-second gap.
MAP_CHECK_SAMPLES = {"P02": (13, 103, 193, 283, 313, 343),
                     "P04": (0, 60, 151, 211, 271, 331)}
MAP_RECOVERY_OOO_SUFFIX = "-map-recovery-ooo"
MAP_RECOVERY_STALE_SUFFIX = "-map-recovery-stale"


def chart_event(patient, signal, second, start, suffix=""):
    itemid, unit, _, _ = SIGNALS[signal]
    phase = second % SCENARIO_SECONDS
    patient_number = int(patient["demo_id"][1:])
    # Slow bounded changes are reproducible and readable at bedside-demo cadence.
    drift = int(second // 18)
    values = {"HR": 78 + patient_number + drift % 5, "MAP": 74 + drift % 4,
              "SBP": 116 + drift % 5, "DBP": 70 + drift % 3,
              "SpO2": 97 + drift % 2, "RR": 16 + drift % 4}
    if signal == "MAP" and ((patient_number == 2 and 12 <= phase < SCENARIO_SECONDS)
                            or (patient_number == 3 and (phase < 120 or phase >= 180))):
        values[signal] = 61 + drift % 3
    event_time = (start + timedelta(seconds=second)).isoformat().replace("+00:00", "Z")
    return {"schema_version": "2.0", "event_id": f"evt-{patient['stay_id']}-{event_time}-{signal}{suffix}",
            **{field: patient[field] for field in ("subject_id", "hadm_id", "stay_id", "bed_id")},
            "itemid": itemid, "signal_name": signal, "event_time": event_time,
            "valuenum": values[signal], "valueuom": unit}


def crossed(second, previous_second, period, offset):
    """Emit a scheduled item once on the first sample crossing its clock boundary."""
    return math.floor((second - offset) / period) > math.floor((previous_second - offset) / period)


def events_at(second, start=DEFAULT_START, *, previous_second=None, tick=None,
              map_episode_checks=False):
    previous_second = second - 1 if previous_second is None else previous_second
    tick = int(second) if tick is None else tick
    for patient in PATIENTS:
        # Four readable updates per clock second; each patient's six signals rotate.
        for signal in (tuple(SIGNALS)[tick % len(SIGNALS)],):
            if map_episode_checks and signal == "MAP" and patient["demo_id"] in MAP_CHECK_SAMPLES:
                continue  # Emit this patient's explicit MAP boundary scenario below.
            event = chart_event(patient, signal, second, start)
            if map_episode_checks and patient["demo_id"] == "P03" and signal == "MAP":
                event["valuenum"] = 71 if 120 <= second < 162 else 62
                # The OOO recovery at 120s plus normal MAP through 157s confirms
                # recovery; the later low segment is independently too short.
            yield TOPICS["chartevents"], event
            if patient["demo_id"] == "P04" and crossed(second, previous_second, SCENARIO_SECONDS, EDGE_SECONDS["duplicate"]):
                yield TOPICS["chartevents"], dict(event)
        if crossed(second, previous_second, 240, 0):
            event = chart_event(patient, "MAP", second, start)
            event.update(event_id=event["event_id"].replace("evt-", "lab-"),
                         mapped_stay_id=event.pop("stay_id"), itemid=50813,
                         lab_name="Lactate", valuenum=3.1 if patient["demo_id"] == "P02" else 1.2,
                         valueuom="mmol/L")
            del event["signal_name"]
            yield TOPICS["labevents"], event
    if map_episode_checks:
        for patient in PATIENTS:
            if second in MAP_CHECK_SAMPLES.get(patient["demo_id"], ()):
                event = chart_event(patient, "MAP", second, start)
                event["valuenum"] = 62
                yield TOPICS["chartevents"], event
        if second == 130:
            recovery = chart_event(PATIENTS[2], "MAP", 120, start, MAP_RECOVERY_OOO_SUFFIX)
            recovery["valuenum"] = 71
            yield TOPICS["chartevents"], recovery
        if second == 340:
            recovery = chart_event(PATIENTS[1], "MAP", 200, start, MAP_RECOVERY_STALE_SUFFIX)
            recovery["valuenum"] = 71
            yield TOPICS["chartevents"], recovery
    patient = PATIENTS[3]
    if crossed(second, previous_second, SCENARIO_SECONDS, EDGE_SECONDS["invalid"]):
        event = chart_event(patient, "SpO2", second, start, "-invalid")
        event["valuenum"] = -5
        yield TOPICS["chartevents"], event
    if crossed(second, previous_second, SCENARIO_SECONDS, EDGE_SECONDS["ooo"]):
        yield TOPICS["chartevents"], chart_event(patient, "HR", second - 10, start, "-ooo")
    if crossed(second, previous_second, SCENARIO_SECONDS, EDGE_SECONDS["late"]):
        yield TOPICS["chartevents"], chart_event(patient, "HR", second - 170, start, "-late")


def demo_events(duration_minutes=DEFAULT_DURATION_MINUTES, start=DEFAULT_START, *, map_episode_checks=False):
    for second in range(math.ceil(duration_minutes * 60)):
        yield from events_at(second, start, map_episode_checks=map_episode_checks)


def next_demo_start(bootstrap="localhost:9092"):
    """Continue the shared logical clock across accelerated producer invocations."""
    from confluent_kafka import Consumer, TopicPartition
    from contracts import parse_event
    clock = datetime.now(timezone.utc).replace(microsecond=0)
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "icu-demo-clock-reader",
                         "enable.auto.commit": False})
    try:
        for topic in TOPICS.values():
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
            if low == high:
                continue
            consumer.assign([TopicPartition(topic, 0, max(low, high - 128))])
            deadline = time.monotonic() + 10
            while True:
                if time.monotonic() >= deadline:
                    raise RuntimeError("Could not read the current demo clock")
                record = consumer.poll(0.2)
                if record is None:
                    continue
                if record.error():
                    raise RuntimeError(record.error())
                event = parse_event(record.value().decode())
                clock = max(clock, parse_timestamp(event["event_time"]) + timedelta(seconds=1))
                if record.offset() >= high - 1:
                    break
    finally:
        consumer.close()
    return clock


def run_demo(speed=1, duration_minutes=DEFAULT_DURATION_MINUTES, loop=False, raw=False, start=None,
             bootstrap="localhost:9092", quiet=False, interval_seconds=DEFAULT_INTERVAL_SECONDS,
             skip_missed_ticks=True, map_episode_checks=False):
    from confluent_kafka import Producer
    if not math.isfinite(speed) or speed <= 0 or not math.isfinite(duration_minutes) or duration_minutes <= 0:
        raise ValueError("speed and duration-minutes must be finite and positive")
    if not math.isfinite(interval_seconds) or interval_seconds <= 0:
        raise ValueError("interval-seconds must be finite and positive")
    start = start or next_demo_start(bootstrap)
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                         "acks": "all", "linger.ms": 0, "message.timeout.ms": 15000})
    failures = []

    def delivered(error, message):
        if error:
            failures.append(str(error))

    tick, total = 0, duration_minutes * 60
    began = time.monotonic()
    previous_second = -interval_seconds
    published, interrupted = 0, False
    try:
        while loop or tick * interval_seconds < total:
            second = tick * interval_seconds
            scheduled = began + second / speed
            now = time.monotonic()
            # Never catch up by emitting a stale backlog as an instantaneous burst.
            if skip_missed_ticks and now - scheduled > interval_seconds / speed:
                tick = math.ceil((now - began) * speed / interval_seconds)
                if not loop and tick * interval_seconds >= total:
                    break
                second = tick * interval_seconds
                scheduled = began + second / speed
            time.sleep(max(0, scheduled - time.monotonic()))
            duplicate_ids = set()
            for topic, event in events_at(second, start, previous_second=previous_second, tick=tick,
                                          map_episode_checks=map_episode_checks):
                while True:
                    try:
                        producer.produce(topic, key=stay_key(event).encode(), value=encode(event).encode(),
                                         on_delivery=delivered)
                        break
                    except BufferError:
                        producer.poll(0.1)
                        if failures:
                            raise RuntimeError("Kafka delivery failed: " + failures[0])
                published += 1
                if not quiet:
                    patient = next(p for p in PATIENTS if str(p["stay_id"]) == stay_key(event))
                    mark = next((label.upper() for label in ("ooo", "invalid", "late")
                                 if event["event_id"].endswith("-" + label)), "")
                    if event["event_id"] in duplicate_ids:
                        mark = "DUPLICATE"
                    duplicate_ids.add(event["event_id"])
                    print(encode(event) if raw else
                          f"{event['event_time'][11:19]}  {event['bed_id']}  {patient['demo_id']}  "
                          f"{event.get('signal_name', event.get('lab_name')):<8} {event['valuenum']:g} {event['valueuom']}"
                          + (f" [{mark}]" if mark else ""), flush=True)
            producer.poll(0)
            if failures:
                raise RuntimeError("Kafka delivery failed: " + failures[0])
            previous_second = second
            tick += 1
        time.sleep(max(0, began + total / speed - time.monotonic()))
    except KeyboardInterrupt:
        interrupted = True
    finally:
        remaining = producer.flush(20)
    if remaining or failures:
        raise RuntimeError(f"Undelivered events: {remaining}; errors: {failures[:3]}")
    if not quiet:
        print(f"[DONE] Simulator {'interrupted' if interrupted else 'finished'}: "
              f"{published} records delivered in {time.monotonic() - began:.1f}s.", flush=True)
    if interrupted:
        raise KeyboardInterrupt
    return start


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--speed", type=float, default=1)
    parser.add_argument("--duration-minutes", type=float, default=DEFAULT_DURATION_MINUTES)
    parser.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--high-frequency", type=int, choices=(100, 200), help="Opt-in synthetic samples/s/signal")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--raw", action="store_true")
    parser.add_argument("--start-time", help="Explicit ISO UTC clock for repeatable runs")
    args = parser.parse_args(argv)
    if args.high_frequency is not None:
        if args.speed != 1 or args.interval_seconds != 1 or args.loop or args.raw:
            parser.error("HighFrequency uses fixed realtime ticks without Speed, IntervalSeconds, Loop, or Raw overrides")
        from high_frequency import profile
        try:
            profile(args.high_frequency, args.duration_minutes)
        except ValueError as error:
            parser.error(str(error))
    return args


def main():
    args = arguments()
    try:
        start = parse_timestamp(args.start_time) if args.start_time else None
        if args.high_frequency is not None:
            from high_frequency import run_high_frequency
            run_high_frequency(args.high_frequency, args.duration_minutes, start=start)
        else:
            run_demo(args.speed, args.duration_minutes, args.loop, args.raw, start,
                     interval_seconds=args.interval_seconds)
    except KeyboardInterrupt:
        return 130
    except (ValueError, RuntimeError) as error:
        print(f"[ERROR] Simulator: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
