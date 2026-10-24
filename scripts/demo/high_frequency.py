"""Deterministic, bounded HF100/HF200 Kafka input; independent of stream processors."""
import json
import math
import time

from run_icu_demo import (DEFAULT_DURATION_MINUTES, DEFAULT_START, PATIENTS,
                          chart_event, events_at, next_demo_start)
from contracts import SIGNALS, TOPICS, stay_key
from dlq import encode


KINDS = ("numeric", "extra", "lab")


class HighFrequencyError(RuntimeError):
    def __init__(self, message, result):
        super().__init__(message)
        self.result = result


def profile(frequency, duration_minutes=DEFAULT_DURATION_MINUTES):
    if type(frequency) is not int or frequency not in (100, 200):
        raise ValueError("high-frequency must be 100 or 200")
    if not math.isfinite(duration_minutes) or duration_minutes <= 0:
        raise ValueError("duration-minutes must be finite and positive")
    # Round CLI floating-point minutes once; subsequent tick arithmetic is integer.
    duration_us = round(duration_minutes * 60 * 1_000_000)
    if duration_us <= 0:
        raise ValueError("duration-minutes must represent at least one microsecond")
    interval_us = 1_000_000 // frequency
    ticks = (duration_us + interval_us - 1) // interval_us
    events_per_tick = len(PATIENTS) * len(SIGNALS)
    return {"profile": f"HF{frequency}", "patients": len(PATIENTS),
            "signals_per_patient": len(SIGNALS), "frequency_hz": frequency,
            "tick_interval_ms": interval_us / 1000, "events_per_tick": events_per_tick,
            "target_numeric_events_per_second": events_per_tick * frequency,
            "requested_seconds": duration_us / 1_000_000, "ticks": ticks,
            "expected_numeric_events": ticks * events_per_tick}


def tick_events(tick, frequency, start=DEFAULT_START):
    """Yield 24 scalar events, then only the existing sparse labs/quality probes."""
    second = tick * (1_000_000 // frequency) / 1_000_000
    for patient in PATIENTS:
        for signal in SIGNALS:
            yield "numeric", TOPICS["chartevents"], chart_event(patient, signal, second, start)
    # Reuse the normal scenario's sparse scheduler, discarding its scalar tick.
    # The repeated ID is the deliberate P04 duplicate probe.
    seen = set()
    for topic, event in events_at(second, start, previous_second=(tick - 1) / frequency, tick=tick):
        if topic == TOPICS["labevents"]:
            yield "lab", topic, event
        elif event["event_id"] in seen or event["event_id"].endswith(("-invalid", "-ooo", "-late")):
            yield "extra", topic, event
        seen.add(event["event_id"])


def high_frequency_events(frequency, duration_minutes=DEFAULT_DURATION_MINUTES, start=DEFAULT_START):
    """Lazy input iterator for replay/tests, with no Kafka or Flink dependency."""
    settings = profile(frequency, duration_minutes)
    for tick in range(settings["ticks"]):
        for _, topic, event in tick_events(tick, frequency, start):
            yield topic, event


def run_high_frequency(frequency, duration_minutes=DEFAULT_DURATION_MINUTES, start=None,
                       bootstrap="localhost:9092", quiet=False):
    from confluent_kafka import Producer

    settings = profile(frequency, duration_minutes)
    if not quiet:
        print("HIGH-FREQUENCY BENCHMARK PROFILE\n" + json.dumps(settings, indent=2), flush=True)
    start = start or next_demo_start(bootstrap)
    counts = {kind: {"attempted": 0, "queued": 0, "delivered": 0, "failures": 0} for kind in KINDS}
    errors = []
    callback_failures = 0

    def note_error(error):
        if len(errors) < 3:
            errors.append(str(error))

    def delivery_callback(kind):
        def delivered(error, message):
            nonlocal callback_failures
            if error:
                counts[kind]["failures"] += 1
                callback_failures += 1
                note_error(error)
            else:
                counts[kind]["delivered"] += 1
        return delivered

    callbacks = {kind: delivery_callback(kind) for kind in KINDS}
    producer = None
    remaining = retries = completed_ticks = scheduled_ticks = lagged_ticks = 0
    max_lag = total_lag = 0.0
    interrupted, fatal_error = False, None
    # perf_counter is monotonic and uses QPC on Windows Python 3.12, avoiding
    # GetTickCount64's coarse resolution at 5/10ms sampling intervals.
    began = time.perf_counter()
    try:
        producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                             "acks": "all", "linger.ms": 5, "batch.size": 65536,
                             "queue.buffering.max.messages": 100000,
                             "queue.buffering.max.kbytes": 65536, "message.timeout.ms": 15000})
        for tick in range(settings["ticks"]):
            target = began + tick / frequency
            # One deadline per tick, never one sleep per observation. When late,
            # keep every logical tick and report the resulting wall-clock lag.
            wait = target - time.perf_counter()
            if wait > 0:
                time.sleep(wait)
            lag = max(0.0, time.perf_counter() - target)
            max_lag, total_lag = max(max_lag, lag), total_lag + lag
            scheduled_ticks += 1
            lagged_ticks += lag >= 1 / frequency
            for kind, topic, event in tick_events(tick, frequency, start):
                counts[kind]["attempted"] += 1
                enqueue_deadline = time.perf_counter() + 15
                while True:
                    try:
                        producer.produce(topic, key=stay_key(event).encode(), value=encode(event).encode(),
                                         on_delivery=callbacks[kind])
                        counts[kind]["queued"] += 1
                        break
                    except BufferError:
                        retries += 1
                        producer.poll(0.01)
                        if callback_failures:
                            raise RuntimeError("Kafka delivery failed: " + errors[0])
                        if time.perf_counter() >= enqueue_deadline:
                            counts[kind]["failures"] += 1
                            raise RuntimeError("Kafka producer queue stayed full for 15 seconds")
                    except Exception as error:
                        counts[kind]["failures"] += 1
                        raise RuntimeError(f"Kafka enqueue failed: {error}") from error
                if callback_failures:
                    raise RuntimeError("Kafka delivery failed: " + errors[0])
            completed_ticks += 1
            producer.poll(0)
            if callback_failures:
                raise RuntimeError("Kafka delivery failed: " + errors[0])
        wait = began + settings["requested_seconds"] - time.perf_counter()
        if wait > 0:
            time.sleep(wait)
    except KeyboardInterrupt:
        interrupted = True
    except Exception as error:
        fatal_error = str(error)
        note_error(error)
    finally:
        if producer is not None:
            try:
                remaining = producer.flush(20)  # One final drain, including failure/interruption.
            except Exception as error:
                fatal_error = f"Kafka final flush failed: {error}"
                note_error(error)

    elapsed = max(time.perf_counter() - began, 1e-9)
    totals = {field: sum(counts[kind][field] for kind in KINDS) for field in counts["numeric"]}
    undelivered = max(remaining, totals["queued"] - totals["delivered"] - callback_failures)
    numeric_rate = counts["numeric"]["delivered"] / elapsed
    failed = bool(fatal_error or totals["failures"] or undelivered)
    result = dict(settings, event_start=start.isoformat(), actual_seconds=elapsed,
                  outcome="failed" if failed else "interrupted" if interrupted else "completed",
                  counts=counts, **totals, undelivered=undelivered, errors=errors,
                  actual_numeric_events_per_second=numeric_rate,
                  actual_total_events_per_second=totals["delivered"] / elapsed,
                  rate_achievement_percent=100 * numeric_rate / settings["target_numeric_events_per_second"],
                  completed_ticks=completed_ticks, queue_full_retries=retries,
                  max_scheduling_lag_ms=max_lag * 1000,
                  mean_scheduling_lag_ms=total_lag * 1000 / max(1, scheduled_ticks),
                  ticks_late_by_one_interval=lagged_ticks)
    if not quiet:
        print("HIGH-FREQUENCY RESULT\n" + json.dumps(result, indent=2), flush=True)
    if failed:
        raise HighFrequencyError(fatal_error or f"Kafka failures={totals['failures']}, undelivered={undelivered}", result)
    if interrupted:
        error = KeyboardInterrupt()
        error.result = result
        raise error
    return result
