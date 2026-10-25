export interface TrendRangeOption {
  label: string
  minutes: number
}

export const DEFAULT_TREND_RANGE_MINUTES = 5

export const TREND_RANGES: TrendRangeOption[] = [
  { label: '1h', minutes: 60 },
  { label: '2h', minutes: 120 },
  { label: '3h', minutes: 180 },
  { label: '6h', minutes: 360 },
  { label: '12h', minutes: 720 },
]

// Clinical render matrix: bucket size per range so every window renders
// ~300-360 points regardless of raw sample rate.
export const TREND_RANGE_BUCKET_SECONDS: Record<number, number> = {
  60: 10,
  120: 20,
  180: 30,
  360: 60,
  720: 120,
}

export function bucketSecondsForRange(minutes: number): number {
  return (
    TREND_RANGE_BUCKET_SECONDS[minutes] ??
    Math.max(1, Math.ceil((minutes * 60) / 360))
  )
}

export function formatRangeLabel(minutes: number): string {
  if (minutes < 60) return `${minutes} min`
  const hours = minutes / 60
  return `${Number.isInteger(hours) ? hours : hours.toFixed(1)}h`
}
