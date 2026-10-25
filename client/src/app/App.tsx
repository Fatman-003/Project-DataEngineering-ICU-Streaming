import { useCallback, useMemo } from 'react'
import { AppHeader } from '../components/layout/AppHeader'
import { PatientList } from '../components/patient/PatientList'
import { PatientCard } from '../components/patient/PatientCard'
import { VitalsGrid } from '../components/vitals/VitalsGrid'
import { TrendChart } from '../components/charts/TrendChart'
import { TrendToolbar } from '../components/charts/TrendToolbar'
import { AlertsPanel } from '../components/alerts/AlertsPanel'
import { Vitals5mPanel } from '../components/vitals/Vitals5mPanel'
import { LabsPanel } from '../components/labs/LabsPanel'
import { RealtimeProvider, useRealtime } from '../features/realtime/RealtimeContext'
import { exportTelemetryHistory } from '../lib/exportHistory'
import { METRICS } from '../lib/metrics'
import { filterTelemetryByRange } from '../lib/telemetry'
import { DEFAULT_TREND_RANGE_MINUTES, formatRangeLabel } from '../lib/timeRange'
import styles from './App.module.css'

function Dashboard() {
  const {
    state,
    patient,
    labs,
    patientsStatus,
    patientsError,
    trendRangeMinutes,
    selectTrendRange,
  } = useRealtime()

  const activeRange = trendRangeMinutes ?? DEFAULT_TREND_RANGE_MINUTES
  const rangeLabel = formatRangeLabel(activeRange)
  const trendPoints = useMemo(
    () => filterTelemetryByRange(state.telemetry, activeRange),
    [state.telemetry, activeRange],
  )

  const handleExport = useCallback(() => {
    if (!patient) return
    exportTelemetryHistory(patient, trendPoints, rangeLabel)
  }, [patient, trendPoints, rangeLabel])

  if (!patient) {
    return (
      <div className={styles.app}>
        <main className={styles.layout}>
          <div className="card">
            <div className="empty">
              {patientsStatus === 'error'
                ? patientsError ?? 'Unable to load patients.'
                : 'Loading patients...'}
            </div>
          </div>
        </main>
      </div>
    )
  }

  return (
    <div className={styles.app}>
      <AppHeader />

      <main className={styles.layout}>
        <aside className={styles.sidebar}>
          <PatientList />
          <PatientCard patient={patient} />
        </aside>

        <section className={styles.center}>
          <VitalsGrid points={state.telemetry} latest={state.latestTelemetry} />
          <TrendToolbar
            rangeMinutes={trendRangeMinutes}
            pointCount={trendPoints.length}
            onRangeChange={selectTrendRange}
            onExport={handleExport}
            exportDisabled={trendPoints.length === 0}
          />
          <div className={styles.charts}>
            {METRICS.map((metric) => (
              <TrendChart
                key={metric.key}
                metric={metric}
                points={trendPoints}
                rangeMinutes={activeRange}
              />
            ))}
          </div>
          <LabsPanel labs={labs} />
        </section>

        <aside className={styles.right}>
          <AlertsPanel alerts={state.alerts} />
          <Vitals5mPanel windows={state.vitals5m} latest={state.latestTelemetry} />
        </aside>
      </main>
    </div>
  )
}

export function App() {
  return (
    <RealtimeProvider>
      <Dashboard />
    </RealtimeProvider>
  )
}
