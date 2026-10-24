import type {
  AlertHistoryResponse,
  ApiResponse,
  LabHistoryResponse,
  StaySummaryDto,
  PageDto,
  TelemetryHistoryResponse,
  Vitals5mHistoryResponse,
} from './types'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? ''

export class ApiError extends Error {
  readonly status: number
  readonly code: number

  constructor(message: string, status: number, code: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { Accept: 'application/json' },
  })

  if (!response.ok) {
    let message = `Request failed with status ${response.status}`
    let code = response.status

    try {
      const payload = (await response.json()) as ApiResponse<unknown>
      message = payload.message ?? message
      code = payload.code ?? code
    } catch {
      // Fall back to the HTTP status when the body is not an API response.
    }

    throw new ApiError(message, response.status, code)
  }

  const payload = (await response.json()) as ApiResponse<T>
  return payload.data
}

function query(params: Record<string, string | number | boolean | null | undefined>): string {
  const search = new URLSearchParams()

  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined && value !== '') {
      search.set(key, String(value))
    }
  }

  const text = search.toString()
  return text.length > 0 ? `?${text}` : ''
}

export async function fetchStaySummaries(page = 0, size = 50): Promise<PageDto<StaySummaryDto>> {
  return get<PageDto<StaySummaryDto>>(`/api/v1/icu-stays${query({ page, size })}`)
}

export async function fetchStaySummary(stayId: number): Promise<StaySummaryDto> {
  return get<StaySummaryDto>(`/api/v1/icu-stays/${stayId}`)
}

export async function fetchTelemetryHistory(
  stayId: number,
  rangeMinutes: number,
  maxPoints = 600,
  bucketSeconds?: number,
): Promise<TelemetryHistoryResponse> {
  return get<TelemetryHistoryResponse>(
    `/api/v1/icu-stays/${stayId}/telemetry${query({ rangeMinutes, maxPoints, bucketSeconds })}`,
  )
}

export async function fetchLabHistory(
  stayId: number,
  limit = 50,
): Promise<LabHistoryResponse> {
  return get<LabHistoryResponse>(`/api/v1/icu-stays/${stayId}/labs${query({ limit })}`)
}

export async function fetchAlertHistory(
  stayId: number,
  limit = 100,
): Promise<AlertHistoryResponse> {
  return get<AlertHistoryResponse>(`/api/v1/icu-stays/${stayId}/alerts${query({ limit })}`)
}

export async function fetchVitals5mHistory(
  stayId: number,
  limit = 5,
): Promise<Vitals5mHistoryResponse> {
  return get<Vitals5mHistoryResponse>(`/api/v1/icu-stays/${stayId}/vitals-5m${query({ limit })}`)
}
