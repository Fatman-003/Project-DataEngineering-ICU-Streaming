"""Lazy, frame-aligned WFDB chunks; sample arrays are decoded only on demand."""
import hashlib
import heapq
import math
from datetime import timedelta
from pathlib import Path


def chunk_frames(first, last, fs):
    """Approximately one second; cover every requested frame exactly once."""
    if not math.isfinite(fs) or fs < 1 or first < 0 or last < first:
        raise ValueError("Invalid frame interval/frequency")
    left = first
    number = 1
    while left < last:
        right = min(last, first + max(1, math.floor(number * fs)))
        yield left, right
        left = right
        number += 1


def _row_chunks(row, window_start, window_end):
    fs = row["frame_frequency_hz"]
    raw_start = row["raw_segment_start_time"]
    first = max(row["source_frame_start"],
                math.ceil((window_start - raw_start).total_seconds() * fs - 1e-6))
    last = min(row["source_frame_end"],
               math.floor((window_end - raw_start).total_seconds() * fs + 1e-6))
    if first >= last:
        return
    for left, right in chunk_frames(first, last, fs):
        identity = [row["stay_id"], row["waveform_record_id"],
                    row["waveform_segment_id"], row["channel_index"], left, right]
        event_id = "wf-" + hashlib.sha256(repr(identity).encode()).hexdigest()
        start = raw_start + timedelta(seconds=left / fs)
        end = raw_start + timedelta(seconds=right / fs)
        yield dict(row, event_id=event_id, event_time=start, source_event_time=start,
                   start_time=start, end_time=end, chunk_frame_start=left,
                   chunk_frame_end=right, replay_time_basis="waveform_chunk_start")


def descriptors(rows, seconds=10, start_time=None):
    if not rows:
        return iter(())
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("waveform-seconds must be positive and finite")
    start = max(min(row["source_start_time"] for row in rows), start_time or min(
        row["source_start_time"] for row in rows))
    end = start + timedelta(seconds=seconds)
    # One pending descriptor per manifest row; never materialize all sample chunks.
    streams = [_row_chunks(row, start, end) for row in rows]
    return heapq.merge(*streams, key=lambda event: (event["event_time"], event["event_id"]))


def decode_chunk(event, waveform_root):
    import wfdb
    if waveform_root is None:
        raise ValueError("MIMIC_WAVEFORM_ROOT is required for waveform replay")
    header = Path(event["source_header_path"]).resolve()
    data = Path(event["source_data_reference"]).resolve()
    for path in (header, data):
        if not path.is_relative_to(waveform_root) or not path.is_file():
            raise ValueError(f"Missing/out-of-root waveform input: {path}")
    record_name = str(header.with_suffix(""))
    description = wfdb.rdheader(record_name)
    channel = event["channel_index"]
    spf = (description.samps_per_frame or [1] * description.n_sig)[channel]
    actual_data = (header.parent / description.file_name[channel]).resolve()
    if (description.sig_name[channel] != event["signal_name"]
            or not math.isclose(description.fs, event["frame_frequency_hz"])
            or spf != event["samples_per_frame"]
            or not math.isclose(description.fs * spf, event["sampling_rate_hz"])
            or actual_data != data):
        raise ValueError(f"Manifest/header mismatch: {header}, channel {channel}")
    decoded = wfdb.rdrecord(
        record_name, sampfrom=event["chunk_frame_start"], sampto=event["chunk_frame_end"],
        channels=[channel], physical=True, smooth_frames=False)
    samples = decoded.e_p_signal[0].tolist()
    expected = (event["chunk_frame_end"] - event["chunk_frame_start"]) * spf
    if len(samples) != expected:
        raise ValueError(f"Waveform length mismatch: {len(samples)} != {expected}")
    result = dict(event)
    result["samples"] = [value if math.isfinite(value) else None for value in samples]
    result["sample_count"] = len(samples)
    result["missing_sample_count"] = sum(value is None for value in result["samples"])
    result["quality_flag"] = "MISSING_SAMPLES" if result["missing_sample_count"] else "SOURCE"
    result["chunk_sample_start"] = event["chunk_frame_start"] * spf
    result["chunk_sample_end"] = event["chunk_frame_end"] * spf
    return result
