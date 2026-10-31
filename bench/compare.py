"""Correctness of each engine against the offline reference (plan §9.3).

    python bench/compare.py --run bench/results/<name>

Reads raw.parquet / outputs.parquet (bench/collect.py), prints a table and writes
correctness.json. Tail items (not yet closable, §9.2) and arrival-dependent
events are reported separately and never count as errors.
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from reference import FIELDS, build_reference   # noqa: E402

AVG_VAR = [prefix + field for field in FIELDS.values() for prefix in ("avg_", "var_")]
ALERT_KEY = ("stay_id", "alert_time", "alert_id", "trigger_value")


def ratio(part, whole):
    return None if not whole else part / whole


def unique_by_id(messages, id_field):
    seen, unique = Counter(), {}
    for message in messages:
        seen[message[id_field]] += 1
        unique.setdefault(message[id_field], message)
    return unique, sum(count - 1 for count in seen.values())


def keyed(unique, clock):
    return {(m["stay_id"], m[clock]): m for m in unique.values()}


def compare_items(engine, closed, tail, clock, value_fields, exact):
    """Precision/recall on keys; engine keys that are reference tail are excluded from precision."""
    found = keyed(engine, clock)
    considered = {k: v for k, v in found.items() if k not in tail}
    matched = considered.keys() & closed.keys()
    result = {"reference": len(closed), "engine": len(considered), "engine_tail": len(found) - len(considered),
              "matched": len(matched), "precision": ratio(len(matched), len(considered)),
              "recall": ratio(len(matched), len(closed)),
              "message_id_match": ratio(sum(found[k]["message_id"] == closed[k]["message_id"] for k in matched),
                                        len(matched))}
    if exact:
        result["fields_exact"] = ratio(sum(all(found[k][f] == closed[k][f] for f in value_fields) for k in matched),
                                       len(matched))
    else:
        errors = [abs(found[k][f] - closed[k][f]) if found[k][f] is not None and closed[k][f] is not None
                  else (0.0 if found[k][f] is None and closed[k][f] is None else float("inf"))
                  for k in matched for f in value_fields]
        result["max_abs_error"] = max(errors, default=0.0)
    result["missing"] = sorted(map(list, closed.keys() - considered.keys()))[:10]
    result["unexpected"] = sorted(map(list, considered.keys() - closed.keys()))[:10]
    return result


def spark_dropped(run_dir):
    metrics = Path(run_dir) / "spark_metrics"
    totals = {}
    for name in ("wide", "alert"):
        path = metrics / f"{name}.jsonl"
        if path.exists():
            rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
            totals[name] = sum(row.get("numRowsDroppedByWatermark", 0) for row in rows if row.get("event") == "progress")
    return totals


def evaluate(run_dir):
    import pandas as pd
    run_dir = Path(run_dir)
    raw = pd.read_parquet(run_dir / "raw.parquet").to_dict("records")
    outputs = pd.read_parquet(run_dir / "outputs.parquet").to_dict("records")
    reference = build_reference(raw)
    report = {"reference": {"telemetry": len(reference["telemetry"]), "telemetry_tail": len(reference["telemetry_tail"]),
                            "windows": len(reference["windows"]), "windows_tail": len(reference["windows_tail"]),
                            "alerts": len(reference["alerts"]), "alert_buckets_tail": reference["alert_buckets_tail"],
                            "dlq": reference["dlq"], "engine_dependent_events": len(reference["engine_dependent"]),
                            "max_event_time": reference["max_event_ts"]},
              "engines": {}}
    ref_alerts = {tuple(a[f] for f in ALERT_KEY): a for a in reference["alerts"]}
    for engine in sorted({row["engine"] for row in outputs}):
        by_kind = {kind: [json.loads(r["payload"]) for r in outputs if r["engine"] == engine and r["kind"] == kind]
                   for kind in ("telemetry", "aggregates", "alerts", "deadletter")}
        telemetry, telemetry_dup = unique_by_id(by_kind["telemetry"], "message_id")
        windows, windows_dup = unique_by_id(by_kind["aggregates"], "message_id")
        alerts, alerts_dup = unique_by_id(by_kind["alerts"], "message_id")
        dlq, dlq_dup = unique_by_id(by_kind["deadletter"], "dlq_id")
        engine_alerts = {tuple(a[f] for f in ALERT_KEY): a for a in alerts.values()}
        matched = engine_alerts.keys() & ref_alerts.keys()
        result = {
            "telemetry": compare_items(telemetry, reference["telemetry"], reference["telemetry_tail"],
                                       "sample_time", list(FIELDS.values()), exact=True),
            "windows": compare_items(windows, reference["windows"], reference["windows_tail"],
                                     "window_end", AVG_VAR, exact=False),
            "alerts": {"reference": len(ref_alerts), "engine": len(engine_alerts), "exact_match": len(matched),
                       "trigger_message_match": sum(engine_alerts[k]["trigger_message"] == ref_alerts[k]["trigger_message"]
                                                    for k in matched),
                       "missing": [list(k) for k in ref_alerts.keys() - engine_alerts.keys()],
                       "unexpected": [list(k) for k in engine_alerts.keys() - ref_alerts.keys()]},
            "dlq": dict(Counter(m["error_message"].split(":", 1)[0] for m in dlq.values()
                                if m["source_topic"] == "icu.v2.chartevents")),
            "dlq_labevents": sum(m["source_topic"] == "icu.v2.labevents" for m in dlq.values()),
            "duplicates": {"telemetry": telemetry_dup, "aggregates": windows_dup, "alerts": alerts_dup,
                           "deadletter": dlq_dup},
        }
        if engine == "spark":
            result["dropped_by_watermark"] = spark_dropped(run_dir)
        report["engines"][engine] = result
    (run_dir / "correctness.json").write_text(json.dumps(report, indent=2, default=str))
    return report


def fmt(value):
    return "-" if value is None else f"{value:.4f}" if isinstance(value, float) else str(value)


def print_report(report):
    ref = report["reference"]
    print(f"Reference: telemetry={ref['telemetry']} (+{ref['telemetry_tail']} tail), windows={ref['windows']} "
          f"(+{ref['windows_tail']} tail), alerts={ref['alerts']} (+{ref['alert_buckets_tail']} pending buckets), "
          f"dlq={ref['dlq']}, arrival-dependent={ref['engine_dependent_events']}")
    for engine, result in report["engines"].items():
        print(f"\n[{engine}]")
        for kind in ("telemetry", "windows"):
            r = result[kind]
            extra = (f"fields_exact={fmt(r['fields_exact'])}" if "fields_exact" in r
                     else f"max_abs_error={r['max_abs_error']:.2e}")
            print(f"  {kind:<9} ref={r['reference']} engine={r['engine']} (+{r['engine_tail']} tail) "
                  f"precision={fmt(r['precision'])} recall={fmt(r['recall'])} "
                  f"message_id={fmt(r['message_id_match'])} {extra}")
        a = result["alerts"]
        print(f"  alerts    ref={a['reference']} engine={a['engine']} exact={a['exact_match']} "
              f"trigger_message={a['trigger_message_match']} missing={a['missing']} unexpected={a['unexpected']}")
        print(f"  dlq       {result['dlq']} labevents={result['dlq_labevents']}")
        print(f"  duplicates {result['duplicates']}")
        if "dropped_by_watermark" in result:
            print(f"  dropped by watermark {result['dropped_by_watermark']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", required=True)
    print_report(evaluate(parser.parse_args().run))


if __name__ == "__main__":
    main()
