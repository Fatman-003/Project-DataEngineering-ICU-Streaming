export function stayDestinations(stayId: number): string[] {
  return [
    `/topic/stays/${stayId}/telemetry`,
    `/topic/stays/${stayId}/vitals-5m`,
    `/topic/stays/${stayId}/alerts`,
  ]
}
