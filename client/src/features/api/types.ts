export interface ApiResponse<T> {
  code: number
  data: T
  message: string
}

export interface PageDto<T> {
  content: T[]
  page: number
  size: number
  totalElements: number
  totalPages: number
  first: boolean
  last: boolean
}

export interface StaySummaryDto {
  stayId: number
  subjectId: number
  hadmId: number
  gender: string | null
  age: number | null
  bedId: string | null
  firstCareunit: string | null
  lastCareunit: string | null
  admissionTime: string | null
  dischargeTime: string | null
  hospitalAdmissionTime: string | null
  hospitalDischargeTime: string | null
  lengthOfStay: number | null
  admissionType: string | null
  admissionLocation: string | null
}

export interface PatientTelemetryDto {
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

export interface TelemetryHistoryResponse {
  stayId: number
  from: string | null
  to: string | null
  rangeMinutes: number | null
  bucketSeconds: number
  aggregated: boolean
  points: PatientTelemetryDto[]
}

export type LabFlag = 'normal' | 'high' | 'low' | 'critical' | 'unknown'

export interface LabResultDto {
  labId: string
  stayId: number
  collectedAt: string
  itemId: number | null
  name: string | null
  category: string | null
  value: number | null
  rawValue: string | null
  unit: string | null
  referenceLow: number | null
  referenceHigh: number | null
  flag: LabFlag
}

export interface LabHistoryResponse {
  stayId: number
  results: PageDto<LabResultDto>
}

export type AlertType =
  | 'HYPOTENSION_SUSTAINED'
  | 'QSOFA_HIGH_RISK'
  | 'SENSOR_DISCONNECTED'

export type AlertSeverity = 'WARNING' | 'CRITICAL'

export type AlertStatus = 'ACTIVE' | 'ACKNOWLEDGED' | 'FALSE_ALARM'

export interface AlertSnapshotDto {
  alertId: string
  stayId: number
  alertTime: string
  alertType: AlertType
  severity: AlertSeverity
  triggerValue: number | null
  triggerMessage: string | null
  status: AlertStatus
}

export interface AlertHistoryResponse {
  stayId: number
  alerts: PageDto<AlertSnapshotDto>
}

export interface VitalsWindow5mDto {
  windowEnd: string
  stayId: number
  avgHeartRate: number | null
  avgMap: number | null
  avgSbp: number | null
  avgSpo2: number | null
}

export interface Vitals5mHistoryResponse {
  stayId: number
  windows: VitalsWindow5mDto[]
}
