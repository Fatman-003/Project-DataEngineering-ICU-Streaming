export function appendCapped<T>(items: readonly T[], item: T, cap: number): T[] {
  const next = [...items, item]
  return next.length > cap ? next.slice(next.length - cap) : next
}
