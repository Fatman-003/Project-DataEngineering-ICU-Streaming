import { FlaskConical } from 'lucide-react'
import type { LabFlag, LabResultDto } from '../../features/api/types'
import { formatDateTime } from '../../lib/format'
import { Badge } from '../ui/Badge'
import type { BadgeTone } from '../ui/Badge'
import styles from './LabsPanel.module.css'

const FLAG_META: Record<LabFlag, { label: string; tone: BadgeTone }> = {
  normal: { label: 'Normal', tone: 'ok' },
  high: { label: 'High', tone: 'warn' },
  low: { label: 'Low', tone: 'info' },
  critical: { label: 'Critical', tone: 'crit' },
  unknown: { label: 'Unknown', tone: 'neutral' },
}

function formatResult(lab: LabResultDto): string {
  if (lab.value !== null) {
    return lab.value.toLocaleString('en-US', { maximumFractionDigits: 2 })
  }
  return lab.rawValue ?? '—'
}

function formatNumber(value: number): string {
  return value.toLocaleString('en-US', { maximumFractionDigits: 2 })
}

function formatReference(low: number | null, high: number | null): string {
  if (low === null && high === null) return '—'
  if (low === null) return `≤ ${formatNumber(high as number)}`
  if (high === null) return `≥ ${formatNumber(low)}`
  return `${formatNumber(low)}–${formatNumber(high)}`
}

interface LabsPanelProps {
  labs: LabResultDto[]
}

export function LabsPanel({ labs }: LabsPanelProps) {
  return (
    <section className="card">
      <div className="card__header">
        <h2 className="card__title">
          <FlaskConical size={14} /> Lab results
        </h2>
        <Badge tone="neutral">{labs.length} recent</Badge>
      </div>

      {labs.length === 0 ? (
        <div className="empty">No lab results for this patient.</div>
      ) : (
        <div className={styles.table}>
          <div className={`${styles.row} ${styles.head}`}>
            <span>Collected</span>
            <span>Test</span>
            <span>Result</span>
            <span className={styles.reference}>Reference</span>
            <span>Flag</span>
          </div>

          {labs.map((lab) => {
            const meta = FLAG_META[lab.flag]

            return (
              <div key={lab.labId} className={styles.row} data-flag={lab.flag}>
                <span className="mono muted">{formatDateTime(lab.collectedAt)}</span>
                <span className={styles.name}>{lab.name ?? 'Unknown test'}</span>
                <span className={`mono ${styles.result}`}>
                  <strong>{formatResult(lab)}</strong>
                  {lab.unit && <small>{lab.unit}</small>}
                </span>
                <span className={`mono muted ${styles.reference}`}>
                  {formatReference(lab.referenceLow, lab.referenceHigh)}
                </span>
                <span>
                  <Badge tone={meta.tone}>{meta.label}</Badge>
                </span>
              </div>
            )
          })}
        </div>
      )}
    </section>
  )
}
