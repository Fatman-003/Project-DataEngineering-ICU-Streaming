import { ShieldCheck, TriangleAlert } from 'lucide-react'
import type { AlertRecord } from '../../features/realtime/realtimeReducer'
import type { AlertStatus, AlertType } from '../../features/realtime/types'
import { formatTime, shortId } from '../../lib/format'
import { Badge } from '../ui/Badge'
import type { BadgeTone } from '../ui/Badge'
import styles from './AlertsPanel.module.css'

const TYPE_LABEL: Record<AlertType, string> = {
  HYPOTENSION_SUSTAINED: 'Sustained hypotension',
  QSOFA_HIGH_RISK: 'High qSOFA risk',
  SENSOR_DISCONNECTED: 'Sensor disconnected',
}

const STATUS_LABEL: Record<AlertStatus, string> = {
  ACTIVE: 'Active',
  ACKNOWLEDGED: 'Acknowledged',
  FALSE_ALARM: 'False alarm',
}

const STATUS_TONE: Record<AlertStatus, BadgeTone> = {
  ACTIVE: 'crit',
  ACKNOWLEDGED: 'info',
  FALSE_ALARM: 'neutral',
}

interface AlertsPanelProps {
  alerts: AlertRecord[]
}

export function AlertsPanel({ alerts }: AlertsPanelProps) {
  const active = alerts.filter((alert) => alert.status === 'ACTIVE').length

  return (
    <section className="card">
      <div className="card__header">
        <h2 className="card__title">
          <TriangleAlert size={14} /> Alerts
        </h2>
        <Badge tone={active > 0 ? 'crit' : 'neutral'}>{active} active</Badge>
      </div>

      {alerts.length === 0 ? (
        <div className="empty">
          No alerts for this patient.
          <br />
          New alerts will appear here automatically.
        </div>
      ) : (
        <ul className={styles.list}>
          {alerts.map((alert) => {
            const tone =
              alert.status === 'FALSE_ALARM'
                ? 'muted'
                : alert.severity === 'CRITICAL'
                  ? 'crit'
                  : 'warn'

            return (
              <li key={alert.alertId} className={styles.item} data-tone={tone}>
                <span className={styles.icon}>
                  {alert.status === 'FALSE_ALARM' ? (
                    <ShieldCheck size={15} />
                  ) : (
                    <TriangleAlert size={15} />
                  )}
                </span>

                <div className={styles.content}>
                  <div className={styles.titleRow}>
                    <strong>{alert.alertType ? TYPE_LABEL[alert.alertType] : 'Unclassified alert'}</strong>
                    <Badge tone={STATUS_TONE[alert.status]}>{STATUS_LABEL[alert.status]}</Badge>
                  </div>

                  {alert.triggerMessage && <p className={styles.message}>{alert.triggerMessage}</p>}

                  <div className={styles.meta}>
                    <span className="mono">{formatTime(alert.alertTime, true)}</span>
                    <span className="mono">#{shortId(alert.alertId)}</span>
                    {alert.triggerValue !== null && (
                      <span className="mono">value {alert.triggerValue}</span>
                    )}
                  </div>
                </div>
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}
