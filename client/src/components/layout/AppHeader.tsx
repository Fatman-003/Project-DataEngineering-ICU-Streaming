import { Activity } from 'lucide-react'
import { useRealtime } from '../../features/realtime/RealtimeContext'
import type { ConnectionStatus } from '../../features/realtime/useStompConnection'
import { formatTime } from '../../lib/format'
import { patientDisplayName, patientIdentity } from '../../lib/patient'
import { useClock } from '../../lib/useClock'
import { Badge } from '../ui/Badge'
import type { BadgeTone } from '../ui/Badge'
import styles from './AppHeader.module.css'

const STATUS_META: Record<ConnectionStatus, { tone: BadgeTone; label: string }> = {
  connecting: { tone: 'warn', label: 'Connecting' },
  connected: { tone: 'ok', label: 'Live' },
  offline: { tone: 'neutral', label: 'Offline' },
}

export function AppHeader() {
  const { patient, status, state } = useRealtime()
  const now = useClock(1000)
  const statusMeta = STATUS_META[status]
  const lastUpdate = state.latestTelemetry

  if (!patient) {
    return (
      <header className={styles.header}>
        <div className={styles.brand}>
          <span className={styles.logo}>
            <Activity size={18} strokeWidth={2.4} />
          </span>
          <div className={styles.brandText}>
            <h1>ICU Monitor</h1>
            <p>Live patient monitoring</p>
          </div>
        </div>
      </header>
    )
  }

  return (
    <header className={styles.header}>
      <div className={styles.brand}>
        <span className={styles.logo}>
          <Activity size={18} strokeWidth={2.4} />
        </span>
        <div className={styles.brandText}>
          <h1>ICU Monitor</h1>
          <p>Live patient monitoring</p>
        </div>
      </div>

      <div className={styles.patient}>
        <span className={styles.bed}>{patient.bed ?? '—'}</span>
        <div className={styles.patientInfo}>
          <strong>{patientDisplayName(patient)}</strong>
          <span>{patientIdentity(patient)}</span>
        </div>
      </div>

      <div className={styles.actions}>
        <div className={styles.updated}>
          <span>Last update</span>
          <strong className="mono">
            {lastUpdate ? formatTime(lastUpdate.sampleTime, true) : '—'}
          </strong>
        </div>
        <span className={`mono ${styles.clock}`}>{formatTime(now, true)}</span>
        <Badge tone={statusMeta.tone} dot>
          {statusMeta.label}
        </Badge>
      </div>
    </header>
  )
}
