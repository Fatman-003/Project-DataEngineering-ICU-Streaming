"""Optional retained WFDB source-tool checks; excluded from the core runtime requirements."""
import math
import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, Mock
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts/source_tools'))
import waveforms
T = datetime(2143, 9, 19, 21, 9)

def manifest(signal='II', fs=62.4725, spf=4, first=0, last=320):
    return dict(stay_id='37323533', waveform_record_id='83411188', waveform_segment_id='83411188_0001', channel_index=0, raw_segment_start_time=T, source_start_time=T + timedelta(seconds=first / fs), source_end_time=T + timedelta(seconds=last / fs), source_frame_start=first, source_frame_end=last, signal_name=signal, frame_frequency_hz=fs, samples_per_frame=spf, sampling_rate_hz=fs * spf)

@unittest.skipUnless(importlib.util.find_spec('wfdb'), 'Optional WFDB source tools')
class WaveformSourceTests(unittest.TestCase):

    def test_frame_chunks_cover_once_at_fractional_rate(self):
        frames = list(waveforms.chunk_frames(19, 1000, 62.4725))
        self.assertEqual(frames[0][0], 19)
        self.assertEqual(frames[-1][1], 1000)
        self.assertEqual(sum((b - a for a, b in frames)), 981)
        self.assertTrue(all((frames[i][1] == frames[i + 1][0] for i in range(len(frames) - 1))))
        self.assertTrue(all((b - a in (62, 63) for a, b in frames[:-1])))

    def test_waveform_window_limit_and_stable_order(self):
        rows = [manifest(), dict(manifest('ABP', spf=2), channel_index=1)]
        events = list(waveforms.descriptors(rows, 2))
        repeated = list(waveforms.descriptors(list(reversed(rows)), 2))
        self.assertEqual(events, repeated)
        self.assertEqual(len(events), 4)
        self.assertTrue(all((event['end_time'] <= T + timedelta(seconds=2) for event in events)))
        for channel in (0, 1):
            selected = [e for e in events if e['channel_index'] == channel]
            self.assertEqual(sum((e['chunk_frame_end'] - e['chunk_frame_start'] for e in selected)), 124)

    def test_window_is_global_not_per_record(self):
        later = manifest()
        later['raw_segment_start_time'] = T + timedelta(days=1)
        later['source_start_time'] = T + timedelta(days=1)
        later['source_end_time'] = T + timedelta(days=1, seconds=5)
        events = list(waveforms.descriptors([manifest(), later], 10))
        self.assertTrue(all((e['start_time'] < T + timedelta(seconds=10) for e in events)))

    def test_waveform_clipped_source_frames(self):
        events = list(waveforms.descriptors([manifest(first=30, last=200)], 2))
        self.assertEqual(events[0]['chunk_frame_start'], 30)
        self.assertLessEqual(events[-1]['chunk_frame_end'], 154)

    def test_bounded_decoder_retains_expanded_multirate_samples(self):
        import wfdb
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            header, data = (root / 'segment.hea', root / 'segment.dat')
            header.touch()
            data.touch()
            event = next(waveforms.descriptors([manifest()], 1))
            event.update(source_header_path=str(header), source_data_reference=str(data))
            description = SimpleNamespace(fs=62.4725, n_sig=1, samps_per_frame=[4], file_name=['segment.dat'], sig_name=['II'])
            samples = [0.25] * 247 + [math.nan]
            decoded = SimpleNamespace(e_p_signal=[Mock(tolist=lambda: samples)])
            with patch.object(wfdb, 'rdheader', return_value=description), patch.object(wfdb, 'rdrecord', return_value=decoded) as read:
                result = waveforms.decode_chunk(event, root)
                self.assertEqual(result['sample_count'], 248)
                self.assertEqual(result['missing_sample_count'], 1)
                self.assertIsNone(result['samples'][-1])
                self.assertFalse(read.call_args.kwargs['smooth_frames'])
                self.assertEqual(read.call_args.kwargs['sampto'], 62)
            with self.assertRaises(ValueError):
                waveforms.decode_chunk(event, root / 'other')
