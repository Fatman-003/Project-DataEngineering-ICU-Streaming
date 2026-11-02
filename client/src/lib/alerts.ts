import type { AlertType } from '../features/realtime/types'

export const ALERT_TYPE_LABEL: Record<AlertType, string> = {
  HYPOTENSION_SUSTAINED: 'Sustained hypotension',
  QSOFA_HIGH_RISK: 'High qSOFA risk',
  SENSOR_DISCONNECTED: 'Sensor disconnected',
}

export function alertTypeLabel(type: AlertType | null): string {
  return type ? ALERT_TYPE_LABEL[type] : 'Unclassified alert'
}
