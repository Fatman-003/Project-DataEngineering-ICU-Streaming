import { createContext, useCallback, useContext, useEffect, useMemo, useReducer, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import {
  fetchAlertHistory,
  fetchLabHistory,
  fetchStaySummaries,
  fetchTelemetryHistory,
  fetchVitals5mHistory,
} from '../api/client'
import type { AlertSnapshotDto, LabResultDto } from '../api/types'
import { toPatientDetail } from '../../lib/patient'
import type { PatientDetail } from '../../lib/patient'
import { createInitialState, realtimeReducer } from './realtimeReducer'
import type { LiveTelemetryMode, RealtimeState } from './realtimeReducer'
import type { RealtimeEvent } from './types'
import type { AlertRecord } from './realtimeReducer'
import { useStompConnection } from './useStompConnection'
import type { ConnectionStatus } from './useStompConnection'
import { DEFAULT_TREND_RANGE_MINUTES, bucketSecondsForRange } from '../../lib/timeRange'

const defaultWsUrl = `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws`
// Empty-string build args count as unset; otherwise the Docker image would
// connect with brokerURL: "" and STOMP would refuse to start.
const WS_URL = import.meta.env.VITE_WS_URL || defaultWsUrl

export type LoadingStatus = 'loading' | 'ready' | 'error'

interface RealtimeContextValue {
  state: RealtimeState
  status: ConnectionStatus
  patient: PatientDetail | null
  patients: PatientDetail[]
  labs: LabResultDto[]
  patientsStatus: LoadingStatus
  patientsError: string | null
  snapshotStatus: LoadingStatus
  snapshotError: string | null
  trendRangeMinutes: number | null
  selectTrendRange: (minutes: number | null) => void
  selectPatient: (stayId: number) => void
}

interface RealtimeProviderProps {
  children: ReactNode
}

const RealtimeContext = createContext<RealtimeContextValue | null>(null)

function toAlertRecord(alert: AlertSnapshotDto): AlertRecord {
  return {
    alertId: alert.alertId,
    stayId: alert.stayId,
    alertTime: alert.alertTime,
    alertType: alert.alertType,
    severity: alert.severity,
    triggerValue: alert.triggerValue,
    triggerMessage: alert.triggerMessage,
    status: alert.status,
    updatedAt: alert.alertTime,
  }
}

function sortLabs(labs: LabResultDto[]): LabResultDto[] {
  return [...labs].sort(
    (a, b) => Date.parse(b.collectedAt) - Date.parse(a.collectedAt),
  )
}

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback
}

export function RealtimeProvider({ children }: RealtimeProviderProps) {
  const [state, dispatch] = useReducer(realtimeReducer, undefined, createInitialState)
  const [patients, setPatients] = useState<PatientDetail[]>([])
  const [labs, setLabs] = useState<LabResultDto[]>([])
  const [stayId, setStayId] = useState<number | null>(null)
  const [patientsStatus, setPatientsStatus] = useState<LoadingStatus>('loading')
  const [patientsError, setPatientsError] = useState<string | null>(null)
  const [snapshotStatus, setSnapshotStatus] = useState<LoadingStatus>('loading')
  const [snapshotError, setSnapshotError] = useState<string | null>(null)
  const [trendRangeMinutes, setTrendRangeMinutes] = useState<number | null>(null)
  const trendRangeRef = useRef<number | null>(null)

  useEffect(() => {
    trendRangeRef.current = trendRangeMinutes
  }, [trendRangeMinutes])

  // High-rate streams emit hundreds of events per second. Buffering them and
  // dispatching one batch per interval keeps React re-renders bounded (~5/s)
  // instead of one full dashboard render per WebSocket message.
  const pendingEventsRef = useRef<RealtimeEvent[]>([])

  const emit = useCallback((event: RealtimeEvent) => {
    pendingEventsRef.current.push(event)
  }, [])

  useEffect(() => {
    const interval = window.setInterval(() => {
      if (pendingEventsRef.current.length === 0) return
      const events = pendingEventsRef.current
      pendingEventsRef.current = []
      const rangeMinutes = trendRangeRef.current ?? DEFAULT_TREND_RANGE_MINUTES
      const live: LiveTelemetryMode =
        rangeMinutes > DEFAULT_TREND_RANGE_MINUTES
          ? { mode: 'bucket', bucketSeconds: bucketSecondsForRange(rangeMinutes) }
          : { mode: 'raw' }
      dispatch({ type: 'batch', events, live })
    }, 200)
    return () => window.clearInterval(interval)
  }, [])

  const status = useStompConnection({ url: WS_URL, stayId, onEvent: emit })

  useEffect(() => {
    let cancelled = false

    async function loadPatients() {
      try {
        const page = await fetchStaySummaries(0, 50)
        if (cancelled) return

        const summaries = page.content.map(toPatientDetail)
        setPatients(summaries)
        setPatientsStatus('ready')
        setPatientsError(null)
        setStayId((current) => current ?? summaries[0]?.stayId ?? null)
      } catch (error) {
        if (cancelled) return
        setPatientsStatus('error')
        setPatientsError(errorMessage(error, 'Unable to load patients'))
      }
    }

    void loadPatients()

    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (stayId === null) return

    let cancelled = false

    async function loadSnapshot() {
      setSnapshotStatus('loading')
      setSnapshotError(null)

      try {
        const [labsPage, alertsPage, vitals] = await Promise.all([
          fetchLabHistory(stayId as number),
          fetchAlertHistory(stayId as number),
          fetchVitals5mHistory(stayId as number),
        ])

        if (cancelled) return

        dispatch({
          type: 'hydrate',
          telemetry: [],
          vitals5m: vitals.windows,
          alerts: alertsPage.alerts.content.map(toAlertRecord),
        })
        setLabs(sortLabs(labsPage.results.content))
        setSnapshotStatus('ready')
      } catch (error) {
        if (cancelled) return
        dispatch({ type: 'reset' })
        setLabs([])
        setSnapshotStatus('error')
        setSnapshotError(errorMessage(error, 'Unable to load patient data'))
      }
    }

    void loadSnapshot()

    return () => {
      cancelled = true
    }
  }, [stayId])

  // Telemetry history is refetched for the selected trend range so switching
  // between 5m/1h/6h pulls the matching server-side buckets immediately
  // instead of stretching the same 5-minute snapshot. Longer ranges also
  // refresh periodically (shift-handover view), since they no longer tick
  // with every raw sample.
  useEffect(() => {
    if (stayId === null || status !== 'connected') return

    let cancelled = false
    const rangeMinutes = trendRangeMinutes ?? DEFAULT_TREND_RANGE_MINUTES
    const bucketSeconds = rangeMinutes > DEFAULT_TREND_RANGE_MINUTES
      ? bucketSecondsForRange(rangeMinutes)
      : undefined

    async function loadTelemetryHistory() {
      try {
        const telemetry = await fetchTelemetryHistory(
          stayId as number,
          rangeMinutes,
          600,
          bucketSeconds,
        )
        if (cancelled) return
        dispatch({
          type: 'hydrate',
          telemetry: telemetry.points,
          vitals5m: [],
          alerts: [],
          bucketSeconds,
        })
      } catch {
        // Keep whatever is on screen; live events continue to flow in.
      }
    }

    void loadTelemetryHistory()

    if (rangeMinutes <= DEFAULT_TREND_RANGE_MINUTES) {
      return () => {
        cancelled = true
      }
    }

    const interval = window.setInterval(() => {
      void loadTelemetryHistory()
    }, 60_000)
    return () => {
      cancelled = true
      window.clearInterval(interval)
    }
  }, [stayId, trendRangeMinutes, status])

  const selectTrendRange = useCallback((minutes: number | null) => {
    // Points already accumulated stay in state: the hydrate for the new range
    // rolls them up into the target bucket size, so the window shows the
    // stored history, the live tail, and everything in between.
    setTrendRangeMinutes(minutes)
  }, [])

  const selectPatient = useCallback((nextStayId: number) => {
    setStayId(nextStayId)
    dispatch({ type: 'reset' })
    setLabs([])
  }, [])

  const patient = useMemo(
    () => patients.find((item) => item.stayId === stayId) ?? null,
    [patients, stayId],
  )

  const value = useMemo<RealtimeContextValue>(
    () => ({
      state,
      status,
      patient,
      patients,
      labs,
      patientsStatus,
      patientsError,
      snapshotStatus,
      snapshotError,
      trendRangeMinutes,
      selectTrendRange,
      selectPatient,
    }),
    [
      state,
      status,
      patient,
      patients,
      labs,
      patientsStatus,
      patientsError,
      snapshotStatus,
      snapshotError,
      trendRangeMinutes,
      selectTrendRange,
      selectPatient,
    ],
  )

  return <RealtimeContext.Provider value={value}>{children}</RealtimeContext.Provider>
}

export function useRealtime(): RealtimeContextValue {
  const value = useContext(RealtimeContext)
  if (!value) throw new Error('useRealtime must be used inside RealtimeProvider')
  return value
}
