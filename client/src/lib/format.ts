const withSecondsFormatter = new Intl.DateTimeFormat('en-GB', {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
})

const timeFormatter = new Intl.DateTimeFormat('en-GB', {
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
})

const dateTimeFormatter = new Intl.DateTimeFormat('en-GB', {
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
})

function toDate(value: string | number | Date | null | undefined): Date | null {
  if (value === null || value === undefined) return null
  const date = value instanceof Date ? value : new Date(value)
  return Number.isNaN(date.getTime()) ? null : date
}

export function formatTime(value: string | number | Date | null | undefined, withSeconds = false): string {
  const date = toDate(value)
  if (!date) return '—'
  return (withSeconds ? withSecondsFormatter : timeFormatter).format(date)
}

export function formatDateTime(value: string | number | Date | null | undefined): string {
  const date = toDate(value)
  return date ? dateTimeFormatter.format(date) : '—'
}

export function formatValue(value: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—'
  return value.toFixed(digits)
}

export function formatDelta(value: number | null, digits = 0): string {
  if (value === null || Number.isNaN(value)) return '—'
  const rounded = Number(value.toFixed(digits))
  const sign = rounded > 0 ? '+' : ''
  return `${sign}${rounded}`
}

export function shortId(value: string): string {
  return value.length > 8 ? value.slice(0, 8) : value
}
