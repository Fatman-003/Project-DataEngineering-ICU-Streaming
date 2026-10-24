import { HeartPulse } from 'lucide-react'
import type { TelemetryPoint } from '../../features/realtime/realtimeReducer'
import { formatTime, formatValue } from '../../lib/format'
import { METRICS, vitalTone } from '../../lib/metrics'
import type { VitalTone } from '../../lib/metrics'
import { Badge } from '../ui/Badge'
import { Sparkline } from '../ui/Sparkline'
import styles from './VitalsGrid.module.css'

interface VitalsGridProps {
  points: TelemetryPoint[]
  latest: TelemetryPoint | null
}

function tileClass(tone: VitalTone | null): string {
  if (tone === 'warning') return `${styles.tile} ${styles.tileWarning}`
  if (tone === 'critical') return `${styles.tile} ${styles.tileCritical}`
  return styles.tile
}

export function VitalsGrid({ points, latest }: VitalsGridProps) {
  return (
    <section className="card">
      <div className="card__header">
        <h2 className="card__title">
          <HeartPulse size={14} /> Live vitals
        </h2>
        <div className={styles.headerMeta}>
          <span className="mono muted">
            {latest ? formatTime(latest.sampleTime, true) : 'waiting for telemetry'}
          </span>
        </div>
      </div>
      <div className={styles.grid}>
        {METRICS.map((metric) => {
          const value = latest ? latest[metric.key] : null
          const tone = vitalTone(metric.key, value)
          const values = points
            .slice(-48)
            .map((point) => point[metric.key])
            .filter((sample): sample is number => sample !== null)

          return (
            <article key={metric.key} className={tileClass(tone)}>
              <div className={styles.tileTop}>
                <span className={styles.tileLabel}>{metric.label}</span>
                <span className={`mono ${styles.range}`}>
                  {metric.normal[0]}–{metric.normal[1]}
                </span>
              </div>

              <div className={styles.valueRow}>
                <span className={`mono ${styles.value}`} style={{ color: metric.color }}>
                  {formatValue(value, metric.digits)}
                </span>
                <span className={styles.unit}>{metric.unit}</span>
              </div>

              <div className={styles.tileFoot}>
                {tone === 'warning' && <Badge tone="warn">warning</Badge>}
                {tone === 'critical' && <Badge tone="crit">critical</Badge>}
                {tone === null && <Badge tone="neutral">no data</Badge>}
                <Sparkline values={values} color={metric.color} />
              </div>
            </article>
          )
        })}
      </div>
    </section>
  )
}
