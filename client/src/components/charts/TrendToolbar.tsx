import { Download, LineChart, X } from 'lucide-react'
import {
  DEFAULT_TREND_RANGE_MINUTES,
  TREND_RANGES,
  formatRangeLabel,
} from '../../lib/timeRange'
import styles from './TrendToolbar.module.css'

interface TrendToolbarProps {
  rangeMinutes: number | null
  pointCount: number
  onRangeChange: (minutes: number | null) => void
  onExport: () => void
  exportDisabled: boolean
}

export function TrendToolbar({
  rangeMinutes,
  pointCount,
  onRangeChange,
  onExport,
  exportDisabled,
}: TrendToolbarProps) {
  const activeMinutes = rangeMinutes ?? DEFAULT_TREND_RANGE_MINUTES

  return (
    <section className={`card ${styles.bar}`}>
      <div className={styles.group}>
        <h2 className="card__title">
          <LineChart size={14} /> Trends
        </h2>

        <div className={styles.options}>
          <button
            type="button"
            className={rangeMinutes === null ? styles.active : undefined}
            onClick={() => onRangeChange(null)}
          >
            5m
          </button>
          {TREND_RANGES.map((range) => (
            <button
              key={range.minutes}
              type="button"
              className={rangeMinutes === range.minutes ? styles.active : undefined}
              onClick={() => onRangeChange(range.minutes)}
            >
              {range.label}
            </button>
          ))}
        </div>
      </div>

      <div className={styles.actions}>
        <span className={`mono muted ${styles.summary}`}>
          {pointCount} pts · last {formatRangeLabel(activeMinutes)}
        </span>

        {rangeMinutes !== null && (
          <button
            type="button"
            className={`btn btn--ghost ${styles.clear}`}
            onClick={() => onRangeChange(null)}
          >
            <X size={13} /> Clear
          </button>
        )}

        <button
          type="button"
          className="btn"
          onClick={onExport}
          disabled={exportDisabled}
        >
          <Download size={13} /> Export history
        </button>
      </div>
    </section>
  )
}
