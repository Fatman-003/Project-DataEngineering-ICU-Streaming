export type RealtimeEventType =
  | 'TELEMETRY'
  | 'VITALS_5M'
  | 'ALERT_CREATED'
  | 'ALERT_STATUS_CHANGED'

export interface RealtimeEvent<T = unknown> {
  type: RealtimeEventType
  stayId: number
  eventTime: string
  sentAt: string
  seq: number
  data: T
}

export type MetricKey = 'heartRate' | 'map' | 'sbp' | 'spo2'

export interface TelemetryPayload {
  stayId: number
  sampleTime: string
  heartRate: number | null
  map: number | null
  sbp: number | null
  spo2: number | null
  minHeartRate?: number | null
  maxHeartRate?: number | null
  minMap?: number | null
  maxMap?: number | null
  minSbp?: number | null
  maxSbp?: number | null
  minSpo2?: number | null
  maxSpo2?: number | null
}

export interface Vitals5mPayload {
  windowEnd: string
  stayId: number
  avgHeartRate: number | null
  avgMap: number | null
  avgSbp: number | null
  avgSpo2: number | null
}

export type AlertType = 'HYPOTENSION_SUSTAINED' | 'QSOFA_HIGH_RISK' | 'SENSOR_DISCONNECTED'

export type AlertSeverity = 'WARNING' | 'CRITICAL'

export type AlertStatus = 'ACTIVE' | 'ACKNOWLEDGED' | 'FALSE_ALARM'

export interface AlertPayload {
  alertId: string
  stayId: number
  alertTime: string
  alertType: AlertType
  severity: AlertSeverity
  triggerValue: number | null
  triggerMessage: string | null
}

export interface AlertStatusChangePayload {
  alertId: string
  stayId: number
  alertTime: string
  status: AlertStatus
}
