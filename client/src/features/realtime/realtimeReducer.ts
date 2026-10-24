import { appendCapped } from '../../lib/collections'
import type {
  AlertPayload,
  AlertSeverity,
  AlertStatus,
  AlertStatusChangePayload,
  AlertType,
  RealtimeEvent,
  TelemetryPayload,
  Vitals5mPayload,
} from './types'

export const TELEMETRY_RETENTION_MS = 12 * 60 * 60 * 1000
export const TELEMETRY_CAP = 6000
export const VITALS_CAP = 288

export type TelemetryPoint = TelemetryPayload

export interface AlertRecord {
  alertId: string
  stayId: number
  alertTime: string
  alertType: AlertType | null
  severity: AlertSeverity | null
  triggerValue: number | null
  triggerMessage: string | null
  status: AlertStatus
  updatedAt: string
}

export interface RealtimeState {
  telemetry: TelemetryPoint[]
  latestTelemetry: TelemetryPoint | null
  vitals5m: Vitals5mPayload[]
  alerts: AlertRecord[]
}

export type LiveTelemetryMode =
  | { mode: 'raw' }
  | { mode: 'bucket'; bucketSeconds: number }

export type RealtimeAction =
  | { type: 'event'; event: RealtimeEvent }
  | { type: 'batch'; events: RealtimeEvent[]; live: LiveTelemetryMode }
  | {
      type: 'hydrate'
      telemetry: TelemetryPoint[]
      vitals5m: Vitals5mPayload[]
      alerts: AlertRecord[]
      bucketSeconds?: number
    }
  | { type: 'reset' }

export function createInitialState(): RealtimeState {
  return {
    telemetry: [],
    latestTelemetry: null,
    vitals5m: [],
    alerts: [],
  }
}

export function realtimeReducer(state: RealtimeState, action: RealtimeAction): RealtimeState {
  if (action.type === 'reset') return createInitialState()

  if (action.type === 'batch') {
    let next = state
    for (const event of action.events) {
      next = applyEvent(next, event, action.live)
    }
    return next
  }

  if (action.type === 'hydrate') {
    // Raw samples accumulated for the previous range are rolled up into the
    // incoming bucket size so the window stays seamless between the stored
    // history (server) and the live tail (WebSocket).
    const carried =
      action.bucketSeconds && action.bucketSeconds > 0 && state.telemetry.length > 0
        ? rollUpToBuckets(state.telemetry, action.bucketSeconds)
        : state.telemetry

    const telemetry = [...carried]
    for (const point of action.telemetry) {
      const index = telemetry.findIndex((item) => item.sampleTime === point.sampleTime)
      if (index === -1) telemetry.push(point)
      else telemetry[index] = point
    }

    const vitals5m = [...state.vitals5m]
    for (const window of action.vitals5m) {
      const index = vitals5m.findIndex((item) => item.windowEnd === window.windowEnd)
      if (index === -1) vitals5m.push(window)
      else vitals5m[index] = window
    }

    const alerts = [...state.alerts]
    for (const alert of action.alerts) {
      const index = alerts.findIndex((item) => item.alertId === alert.alertId)
      if (index === -1) alerts.push(alert)
      else alerts[index] = alert
    }

    const sortedTelemetry = sortTelemetry(telemetry)
    return {
      telemetry: sortedTelemetry,
      latestTelemetry: sortedTelemetry.length > 0 ? sortedTelemetry[sortedTelemetry.length - 1] : null,
      vitals5m: sortVitals(vitals5m),
      alerts: sortAlerts(alerts),
    }
  }

  return applyEvent(state, action.event, { mode: 'raw' })
}

function applyEvent(
  state: RealtimeState,
  event: RealtimeEvent,
  live: LiveTelemetryMode,
): RealtimeState {
  switch (event.type) {
    case 'TELEMETRY':
      return applyTelemetry(state, event, live)
    case 'VITALS_5M':
      return applyVitals5m(state, event)
    case 'ALERT_CREATED':
      return applyAlertCreated(state, event)
    case 'ALERT_STATUS_CHANGED':
      return applyAlertStatus(state, event)
    default:
      return state
  }
}

function bucketKeyFor(sampleTime: string, bucketSeconds: number): string | null {
  const ms = Date.parse(sampleTime)
  if (!Number.isFinite(ms)) return null
  const bucketMs = Math.floor(ms / (bucketSeconds * 1000)) * bucketSeconds * 1000
  return new Date(bucketMs).toISOString().replace('.000Z', 'Z')
}

function rollUpToBuckets(points: TelemetryPoint[], bucketSeconds: number): TelemetryPoint[] {
  const rolled = new Map<string, TelemetryPoint>()
  for (const point of points) {
    const key = bucketKeyFor(point.sampleTime, bucketSeconds)
    if (key === null) continue
    rolled.set(key, { ...point, sampleTime: key })
  }
  return [...rolled.values()]
}

function applyTelemetry(
  state: RealtimeState,
  event: RealtimeEvent,
  live: LiveTelemetryMode,
): RealtimeState {
  if (!isTelemetry(event.data)) return state

  const point: TelemetryPoint = { ...event.data, stayId: event.stayId }

  if (live.mode === 'bucket') {
    // Longer ranges render server-style buckets; live events roll up into the
    // current bucket (latest value wins) instead of piling up raw samples.
    const bucketTime = bucketKeyFor(point.sampleTime, live.bucketSeconds)
    if (bucketTime !== null) {
      const bucketPoint: TelemetryPoint = {
        stayId: point.stayId,
        sampleTime: bucketTime,
        heartRate: point.heartRate,
        map: point.map,
        sbp: point.sbp,
        spo2: point.spo2,
      }
      const telemetry = trimTelemetryHistory(upsertTelemetryPoint(state.telemetry, bucketPoint))
      const latestTelemetry = telemetry.length > 0 ? telemetry[telemetry.length - 1] : bucketPoint
      return { ...state, telemetry, latestTelemetry }
    }
  }

  const telemetry = trimTelemetryHistory(upsertTelemetryPoint(state.telemetry, point))
  const latestTelemetry = telemetry.length > 0 ? telemetry[telemetry.length - 1] : point

  return { ...state, telemetry, latestTelemetry }
}

function upsertTelemetryPoint(
  points: TelemetryPoint[],
  point: TelemetryPoint,
): TelemetryPoint[] {
  // Live events are almost always new samples; only the recent tail can hold
  // an overlapping sample_time (reconnect/hydrate), so scanning it keeps the
  // cost O(1)-ish instead of O(n) on a large history.
  const scanStart = Math.max(0, points.length - 128)
  for (let index = points.length - 1; index >= scanStart; index -= 1) {
    if (points[index].sampleTime === point.sampleTime) {
      const next = [...points]
      next[index] = point
      return next
    }
  }
  return [...points, point]
}

function trimTelemetryHistory(points: TelemetryPoint[]): TelemetryPoint[] {
  if (points.length === 0) return points

  let start = 0
  const last = Date.parse(points[points.length - 1].sampleTime)
  if (Number.isFinite(last)) {
    const cutoff = last - TELEMETRY_RETENTION_MS
    while (start < points.length && Date.parse(points[start].sampleTime) < cutoff) {
      start += 1
    }
  }

  const overflow = points.length - start - TELEMETRY_CAP
  if (overflow > 0) start += overflow

  return start > 0 ? points.slice(start) : points
}

function applyVitals5m(state: RealtimeState, event: RealtimeEvent): RealtimeState {
  if (!isVitals5m(event.data)) return state

  const data = event.data
  const existing = state.vitals5m.filter((window) => window.windowEnd !== data.windowEnd)
  return { ...state, vitals5m: appendCapped(existing, data, VITALS_CAP) }
}

function applyAlertCreated(state: RealtimeState, event: RealtimeEvent): RealtimeState {
  if (!isAlert(event.data)) return state

  const record: AlertRecord = {
    alertId: event.data.alertId,
    stayId: event.stayId,
    alertTime: event.data.alertTime,
    alertType: event.data.alertType,
    severity: event.data.severity,
    triggerValue: event.data.triggerValue,
    triggerMessage: event.data.triggerMessage,
    status: 'ACTIVE',
    updatedAt: event.sentAt,
  }

  return { ...state, alerts: sortAlerts(upsertAlert(state.alerts, record)) }
}

function applyAlertStatus(state: RealtimeState, event: RealtimeEvent): RealtimeState {
  if (!isAlertStatusChange(event.data)) return state

  const { alertId, alertTime, status } = event.data
  const existing = state.alerts.find((alert) => alert.alertId === alertId)

  if (!existing) {
    const placeholder: AlertRecord = {
      alertId,
      stayId: event.stayId,
      alertTime,
      alertType: null,
      severity: null,
      triggerValue: null,
      triggerMessage: null,
      status,
      updatedAt: event.sentAt,
    }
    return { ...state, alerts: sortAlerts(upsertAlert(state.alerts, placeholder)) }
  }

  const updated = state.alerts.map((alert) =>
    alert.alertId === alertId ? { ...alert, status, updatedAt: event.sentAt } : alert,
  )
  return { ...state, alerts: sortAlerts(updated) }
}

function upsertAlert(alerts: AlertRecord[], record: AlertRecord): AlertRecord[] {
  const index = alerts.findIndex((alert) => alert.alertId === record.alertId)
  if (index === -1) return [...alerts, record]
  const next = [...alerts]
  next[index] = record
  return next
}

function sortTelemetry(points: TelemetryPoint[]): TelemetryPoint[] {
  return [...points].sort(
    (a, b) => Date.parse(a.sampleTime) - Date.parse(b.sampleTime),
  )
}

function sortVitals(windows: Vitals5mPayload[]): Vitals5mPayload[] {
  return [...windows].sort(
    (a, b) => Date.parse(a.windowEnd) - Date.parse(b.windowEnd),
  )
}

function sortAlerts(alerts: AlertRecord[]): AlertRecord[] {
  const rank: Record<AlertStatus, number> = { ACTIVE: 0, ACKNOWLEDGED: 1, FALSE_ALARM: 2 }
  return [...alerts].sort(
    (a, b) => rank[a.status] - rank[b.status] || Date.parse(b.alertTime) - Date.parse(a.alertTime),
  )
}

function isTelemetry(data: unknown): data is TelemetryPayload {
  return (
    typeof data === 'object' &&
    data !== null &&
    typeof (data as TelemetryPayload).sampleTime === 'string'
  )
}

function isVitals5m(data: unknown): data is Vitals5mPayload {
  return (
    typeof data === 'object' &&
    data !== null &&
    typeof (data as Vitals5mPayload).windowEnd === 'string'
  )
}

function isAlert(data: unknown): data is AlertPayload {
  return (
    typeof data === 'object' &&
    data !== null &&
    typeof (data as AlertPayload).alertId === 'string' &&
    typeof (data as AlertPayload).alertTime === 'string'
  )
}

function isAlertStatusChange(data: unknown): data is AlertStatusChangePayload {
  return (
    typeof data === 'object' &&
    data !== null &&
    typeof (data as AlertStatusChangePayload).alertId === 'string' &&
    typeof (data as AlertStatusChangePayload).status === 'string'
  )
}
