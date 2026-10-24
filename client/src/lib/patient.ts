import type { StaySummaryDto } from '../features/api/types'

export interface PatientDetail {
  stayId: number
  subjectId: number | null
  hadmId: number | null
  name: string | null
  mrn: string | null
  age: number | null
  gender: 'M' | 'F' | null
  bed: string | null
  unit: string | null
  admissionTime: string | null
  diagnosis: string | null
  codeStatus: string | null
  attending: string | null
  weightKg: number | null
  heightCm: number | null
  allergies: string[]
  riskFlags: string[]
}

export function toPatientDetail(summary: StaySummaryDto): PatientDetail {
  return {
    stayId: summary.stayId,
    subjectId: summary.subjectId,
    hadmId: summary.hadmId,
    name: null,
    mrn: null,
    age: summary.age,
    gender: summary.gender === 'M' || summary.gender === 'F' ? summary.gender : null,
    bed: summary.bedId,
    unit: summary.firstCareunit,
    admissionTime: summary.admissionTime,
    diagnosis: null,
    codeStatus: null,
    attending: null,
    weightKg: null,
    heightCm: null,
    allergies: [],
    riskFlags: [],
  }
}

export function patientDisplayName(patient: PatientDetail): string {
  return patient.name ?? `Stay #${patient.stayId}`
}

export function patientIdentity(patient: PatientDetail): string {
  const parts = [
    patient.age !== null ? `${patient.age} y` : null,
    patient.gender === 'F' ? 'Female' : patient.gender === 'M' ? 'Male' : null,
    patient.mrn,
  ].filter((part): part is string => part !== null)

  return parts.length > 0 ? parts.join(' · ') : 'No identity data'
}
