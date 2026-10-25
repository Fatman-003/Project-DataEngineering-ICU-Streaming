import {
  Bed,
  Calendar,
  ClipboardList,
  IdCard,
  Ruler,
  Scale,
  ShieldAlert,
  Stethoscope,
  Syringe,
  UserRound,
} from 'lucide-react'
import type { ReactNode } from 'react'
import type { PatientDetail } from '../../lib/patient'
import { formatDateTime } from '../../lib/format'
import styles from './PatientCard.module.css'

interface PatientCardProps {
  patient: PatientDetail
}

interface DetailRow {
  icon: ReactNode
  label: string
  value: string
}

export function PatientCard({ patient }: PatientCardProps) {
  const location = [patient.bed ?? '—', patient.unit ?? 'Unknown unit'].join(' · ')
  const weight = patient.weightKg !== null ? `${patient.weightKg.toFixed(1)} kg` : '—'
  const height = patient.heightCm !== null ? `${patient.heightCm} cm` : '—'
  const allergies = patient.allergies.length > 0 ? patient.allergies.join(', ') : 'None documented'
  const rows: DetailRow[] = [
    { icon: <Bed size={13} />, label: 'Location', value: location },
    {
      icon: <Calendar size={13} />,
      label: 'Admitted',
      value: patient.admissionTime ? formatDateTime(patient.admissionTime) : '—',
    },
    { icon: <Stethoscope size={13} />, label: 'Diagnosis', value: patient.diagnosis ?? '—' },
    { icon: <IdCard size={13} />, label: 'MRN', value: patient.mrn ?? '—' },
    { icon: <UserRound size={13} />, label: 'Attending', value: patient.attending ?? '—' },
    { icon: <ShieldAlert size={13} />, label: 'Code status', value: patient.codeStatus ?? '—' },
    {
      icon: <Scale size={13} />,
      label: 'Weight',
      value: weight,
    },
    { icon: <Ruler size={13} />, label: 'Height', value: height },
    {
      icon: <Syringe size={13} />,
      label: 'Allergies',
      value: allergies,
    },
  ]

  return (
    <section className="card">
      <div className="card__header">
        <h2 className="card__title">
          <ClipboardList size={14} /> Patient detail
        </h2>
      </div>
      <div className="card__body">
        <div className={styles.identity}>
          <div>
            <strong>{patient.name ?? `Stay #${patient.stayId}`}</strong>
            <span>
              {patient.age !== null ? `${patient.age} y` : 'Age unknown'} ·{' '}
              {patient.gender === 'F' ? 'Female' : patient.gender === 'M' ? 'Male' : 'Unknown'}
            </span>
          </div>
          <span className={`mono ${styles.stay}`}>#{patient.stayId}</span>
        </div>

        <dl className={styles.rows}>
          {rows.map((row) => (
            <div key={row.label} className={styles.row}>
              <dt>
                {row.icon}
                {row.label}
              </dt>
              <dd>{row.value}</dd>
            </div>
          ))}
        </dl>

        <div className={styles.flags}>
          {patient.riskFlags.map((flag) => (
            <span key={flag} className={styles.flag}>
              {flag}
            </span>
          ))}
        </div>
      </div>
    </section>
  )
}
