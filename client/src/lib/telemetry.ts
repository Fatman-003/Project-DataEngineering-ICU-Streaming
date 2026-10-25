import type { TelemetryPoint } from '../features/realtime/realtimeReducer'

export function filterTelemetryByRange(
  points: TelemetryPoint[],
  minutes: number,
): TelemetryPoint[] {
  if (points.length === 0) return points

  let latest = Number.NEGATIVE_INFINITY
  for (const point of points) {
    const time = Date.parse(point.sampleTime)
    if (Number.isFinite(time) && time > latest) latest = time
  }

  if (!Number.isFinite(latest)) return points

  const cutoff = latest - minutes * 60_000
  return points.filter((point) => {
    const time = Date.parse(point.sampleTime)
    return Number.isFinite(time) && time >= cutoff
  })
}
