import { Timer } from 'lucide-react'
import type { TelemetryPoint } from '../../features/realtime/realtimeReducer'
import type { MetricKey, Vitals5mPayload } from '../../features/realtime/types'
import { formatDelta, formatTime, formatValue } from '../../lib/format'
import { Badge } from '../ui/Badge'
import styles from './Vitals5mPanel.module.css'

interface Stat {
  key: keyof Pick<Vitals5mPayload, 'avgHeartRate' | 'avgMap' | 'avgSbp' | 'avgSpo2'>
  varKey: keyof Pick<Vitals5mPayload, 'varHeartRate' | 'varMap' | 'varSbp' | 'varSpo2'>
  metricKey: MetricKey
  label: string
  unit: string
}

const STATS: Stat[] = [
  { key: 'avgHeartRate', varKey: 'varHeartRate', metricKey: 'heartRate', label: 'Avg HR', unit: 'bpm' },
  { key: 'avgMap', varKey: 'varMap', metricKey: 'map', label: 'Avg MAP', unit: 'mmHg' },
  { key: 'avgSbp', varKey: 'varSbp', metricKey: 'sbp', label: 'Avg SBP', unit: 'mmHg' },
  { key: 'avgSpo2', varKey: 'varSpo2', metricKey: 'spo2', label: 'Avg SpO2', unit: '%' },
]

// Flink stores the population variance per 5-minute window; the console shows
// the standard deviation (σ) because it shares the unit with the average.
function sigmaOf(variance: number | null | undefined): number | null {
  if (variance === null || variance === undefined || Number.isNaN(variance) || variance < 0) {
    return null
  }
  return Math.sqrt(variance)
}

interface Vitals5mPanelProps {
  windows: Vitals5mPayload[]
  latest: TelemetryPoint | null
}

export function Vitals5mPanel({ windows, latest }: Vitals5mPanelProps) {
  const current = windows.length > 0 ? windows[windows.length - 1] : null
  const recent = [...windows].slice(-5).reverse()

  return (
    <section className="card">
      <div className="card__header">
        <h2 className="card__title">
          <Timer size={14} /> 5-minute averages
        </h2>
        {current ? (
          <Badge tone="info">{formatTime(current.windowEnd, true)}</Badge>
        ) : (
          <Badge tone="neutral">waiting</Badge>
        )}
      </div>

      <div className="card__body">
        <div className={styles.grid}>
          {STATS.map((stat) => {
            const average = current ? current[stat.key] : null
            const sigma = current ? sigmaOf(current[stat.varKey]) : null
            const instant = latest ? latest[stat.metricKey] : null
            const delta = average !== null && instant !== null ? average - instant : null

            return (
              <div key={stat.key} className={styles.stat}>
                <span className={styles.statLabel}>{stat.label}</span>
                <span className={`mono ${styles.statValue}`}>
                  {formatValue(average, 1)}
                  <small>{stat.unit}</small>
                </span>
                <span className={styles.delta}>
                  σ <span className="mono" title="5-minute variability (standard deviation)">{formatValue(sigma, 1)}</span>
                  {' · '}vs now <span className="mono">{formatDelta(delta, 1)}</span>
                </span>
              </div>
            )
          })}
        </div>

        {!current && (
          <p className={styles.note}>
            Averages appear after the first 5-minute window closes.
          </p>
        )}

        {recent.length > 0 && (
          <div className={styles.table}>
            <div className={`${styles.row} ${styles.head}`}>
              <span>Window end</span>
              <span>HR ±σ</span>
              <span>MAP ±σ</span>
              <span>SBP ±σ</span>
              <span>SpO2 ±σ</span>
            </div>
            {recent.map((window) => (
              <div key={window.windowEnd} className={styles.row}>
                <span className="mono">{formatTime(window.windowEnd, true)}</span>
                {STATS.map((stat) => {
                  const sigma = sigmaOf(window[stat.varKey])
                  return (
                    <span
                      key={stat.key}
                      className="mono"
                      title="5-minute mean ± standard deviation"
                    >
                      {formatValue(window[stat.key], 1)}
                      {sigma !== null ? ` ±${sigma.toFixed(1)}` : ''}
                    </span>
                  )
                })}
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  )
}
