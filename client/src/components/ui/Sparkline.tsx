interface SparklineProps {
  values: number[]
  color: string
  width?: number
  height?: number
}

export function Sparkline({ values, color, width = 120, height = 34 }: SparklineProps) {
  if (values.length < 2) {
    return <div className="sparkline sparkline--empty" style={{ width, height }} />
  }

  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = max - min || 1
  const step = width / (values.length - 1)

  const coordinates = values.map((value, index) => ({
    x: index * step,
    y: height - ((value - min) / span) * (height - 4) - 2,
  }))
  const last = coordinates[coordinates.length - 1]
  const points = coordinates.map(({ x, y }) => `${x.toFixed(2)},${y.toFixed(2)}`).join(' ')

  return (
    <svg
      className="sparkline"
      viewBox={`0 0 ${width} ${height}`}
      width={width}
      height={height}
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx={last.x} cy={last.y} r={2.4} fill={color} />
    </svg>
  )
}
