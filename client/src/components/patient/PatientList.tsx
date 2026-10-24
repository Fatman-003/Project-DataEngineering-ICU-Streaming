import { Users } from 'lucide-react'
import { useRealtime } from '../../features/realtime/RealtimeContext'
import { patientDisplayName } from '../../lib/patient'
import { Badge } from '../ui/Badge'
import styles from './PatientList.module.css'

export function PatientList() {
  const { patients, patient, selectPatient } = useRealtime()

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
          return (
            <button
              key={item.stayId}
              type="button"
              className={active ? `${styles.item} ${styles.active}` : styles.item}
              onClick={() => selectPatient(item.stayId)}
            >
              <span className={styles.bed}>{item.bed ?? '—'}</span>
              <span className={styles.info}>
                <span className={styles.name}>{patientDisplayName(item)}</span>
                <span className={styles.diagnosis}>{item.diagnosis ?? 'No diagnosis available'}</span>
              </span>
              <span className={`mono ${styles.id}`}>{item.stayId}</span>
            </button>
          )
        })}
      </div>
    </section>
  )
}
