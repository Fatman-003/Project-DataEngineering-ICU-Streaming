import type { TelemetryPoint } from '../features/realtime/realtimeReducer'
import type { PatientDetail } from './patient'

function csvValue(value: string | number | null | undefined): string {
  if (value === null || value === undefined) return ''
  const text = String(value)
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text
}

function fileStamp(value: Date): string {
  return value.toISOString().replace(/[:.]/g, '-').slice(0, 19)
}

export function exportTelemetryHistory(
  patient: PatientDetail,
  points: TelemetryPoint[],
  rangeLabel: string,
): void {
  const header = [
    'mrn',
    'patient',
    'stay_id',
    'range',
    'sample_time',
    'heart_rate_bpm',
    'map_mmhg',
    'sbp_mmhg',
    'spo2_pct',
  ]

  const rows = points.map((point) => [
    patient.mrn,
    patient.name,
    patient.stayId,
    rangeLabel,
    point.sampleTime,
    point.heartRate,
    point.map,
    point.sbp,
    point.spo2,
  ])

  const csv = [header, ...rows]
    .map((row) => row.map((value) => csvValue(value)).join(','))
    .join('\r\n')

  const blob = new Blob(['\uFEFF', csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')

  link.href = url
  link.download = `icu-history-${patient.stayId}-${fileStamp(new Date())}.csv`
  link.style.display = 'none'
  document.body.append(link)
  link.click()
  link.remove()
  window.setTimeout(() => URL.revokeObjectURL(url), 1_000)
}
