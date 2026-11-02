import { useMemo } from 'react'
import { TriangleAlert, Users } from 'lucide-react'
import { useRealtime } from '../../features/realtime/RealtimeContext'
import { patientDisplayName } from '../../lib/patient'
import { Badge } from '../ui/Badge'
import styles from './PatientList.module.css'

interface StayAlertFlag {
  count: number
  critical: boolean
}

export function PatientList() {
  const { patients, patient, wardAlerts, selectPatient } = useRealtime()

  // Central-monitoring rule: every row flags its own active alerts so a doctor
  // scanning the ward list spots trouble without opening each patient page.
  const alertFlags = useMemo(() => {
    const flags = new Map<number, StayAlertFlag>()
    for (const alert of wardAlerts) {
      if (alert.status !== 'ACTIVE') continue
      const flag = flags.get(alert.stayId) ?? { count: 0, critical: false }
      flag.count += 1
      flag.critical = flag.critical || alert.severity === 'CRITICAL'
      flags.set(alert.stayId, flag)
    }
    return flags
  }, [wardAlerts])

  return (
    <section className="card">
      <div className="card__header">
        <h2 className="card__title">
          <Users size={14} /> Patients
        </h2>
        <Badge tone="neutral">{patients.length}</Badge>
      </div>
      <div className={styles.list}>
        {patients.map((item) => {
          const active = item.stayId === patient?.stayId
          const flag = alertFlags.get(item.stayId)

          return (
            <button
              key={item.stayId}
              type="button"
              className={active ? `${styles.item} ${styles.active}` : styles.item}
              data-alert={flag ? (flag.critical ? 'crit' : 'warn') : undefined}
              onClick={() => selectPatient(item.stayId)}
            >
              <span className={styles.bed}>{item.bed ?? '—'}</span>
              <span className={styles.info}>
                <span className={styles.name}>{patientDisplayName(item)}</span>
                <span className={styles.diagnosis}>{item.diagnosis ?? 'No diagnosis available'}</span>
              </span>
              {flag && (
                <span className={styles.alertFlag} title={`${flag.count} active alert(s)`}>
                  <TriangleAlert size={11} />
                  {flag.count > 1 ? <span className="mono">{flag.count}</span> : null}
                </span>
              )}
              <span className={`mono ${styles.id}`}>{item.stayId}</span>
            </button>
          )
        })}
      </div>
    </section>
  )
}
