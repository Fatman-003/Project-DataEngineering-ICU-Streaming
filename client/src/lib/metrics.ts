import type { MetricKey } from '../features/realtime/types'

export interface MetricConfig {
  key: MetricKey
  label: string
  short: string
  unit: string
  digits: number
  color: string
  domain: [number, number]
  normal: [number, number]
}

export const METRICS: MetricConfig[] = [
  {
    key: 'heartRate',
    label: 'Heart rate',
    short: 'HR',
    unit: 'bpm',
    digits: 0,
    color: '#059669',
    domain: [30, 180],
    normal: [60, 100],
  },
  {
    key: 'map',
    label: 'Mean arterial pressure',
    short: 'MAP',
    unit: 'mmHg',
    digits: 0,
    color: '#dc2626',
    domain: [40, 140],
    normal: [65, 100],
  },
  {
    key: 'sbp',
    label: 'Systolic pressure',
    short: 'SBP',
    unit: 'mmHg',
    digits: 0,
    color: '#4f46e5',
    domain: [60, 200],
    normal: [90, 140],
  },
  {
    key: 'spo2',
    label: 'Oxygen saturation',
    short: 'SpO2',
    unit: '%',
    digits: 0,
    color: '#0284c7',
    domain: [70, 100],
    normal: [94, 100],
  },
]

export type VitalTone = 'normal' | 'warning' | 'critical'

export function vitalTone(key: MetricKey, value: number | null): VitalTone | null {
  if (value === null || Number.isNaN(value)) return null

  switch (key) {
    case 'heartRate':
      if (value < 50 || value > 130) return 'critical'
      if (value < 60 || value > 100) return 'warning'
      return 'normal'
    case 'map':
      if (value < 55) return 'critical'
      if (value < 65 || value > 110) return 'warning'
      return 'normal'
    case 'sbp':
      if (value < 80 || value > 180) return 'critical'
      if (value < 90 || value > 160) return 'warning'
      return 'normal'
    case 'spo2':
      if (value < 88) return 'critical'
      if (value < 94) return 'warning'
      return 'normal'
  }
}

export function vitalValue(point: Record<MetricKey, number | null> | null, key: MetricKey): number | null {
  return point ? point[key] : null
}


