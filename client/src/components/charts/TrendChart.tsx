import { useMemo } from 'react'
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { TelemetryPoint } from '../../features/realtime/realtimeReducer'
import type { MetricKey } from '../../features/realtime/types'
import { formatTime, formatValue } from '../../lib/format'
import type { MetricConfig } from '../../lib/metrics'
import { formatRangeLabel } from '../../lib/timeRange'
import styles from './TrendChart.module.css'

interface TrendChartProps {
  metric: MetricConfig
  points: TelemetryPoint[]
  rangeMinutes: number
}

interface ChartPoint {
  t: number
  value: number
  bandMin?: number
  bandSpan?: number
}

const BAND_MIN_KEY: Record<MetricKey, keyof TelemetryPoint> = {
  heartRate: 'minHeartRate',
  map: 'minMap',
  sbp: 'minSbp',
  spo2: 'minSpo2',
}

const BAND_MAX_KEY: Record<MetricKey, keyof TelemetryPoint> = {
  heartRate: 'maxHeartRate',
  map: 'maxMap',
  sbp: 'maxSbp',
  spo2: 'maxSpo2',
}

function downsample(samples: ChartPoint[], maxPoints: number): ChartPoint[] {
  if (samples.length <= maxPoints) return samples

  const bucketSize = Math.ceil(samples.length / maxPoints)
  const result: ChartPoint[] = []

  for (let start = 0; start < samples.length; start += bucketSize) {
    const end = Math.min(start + bucketSize, samples.length)
    let sum = 0

    for (let index = start; index < end; index += 1) {
      sum += samples[index].value
    }

    result.push({ t: samples[end - 1].t, value: sum / (end - start) })
  }

  return result
}

export function TrendChart({ metric, points, rangeMinutes }: TrendChartProps) {
  const showBand = rangeMinutes >= 120

  const data = useMemo(() => {
    const minKey = BAND_MIN_KEY[metric.key]
    const maxKey = BAND_MAX_KEY[metric.key]
    const samples: ChartPoint[] = []

    for (const point of points) {
      const value = point[metric.key]
      const t = Date.parse(point.sampleTime)
      if (value === null || Number.isNaN(t)) continue

      const sample: ChartPoint = { t, value }

      if (showBand) {
        const min = point[minKey]
        const max = point[maxKey]
        if (typeof min === 'number' && typeof max === 'number' && max >= min) {
          sample.bandMin = min
          sample.bandSpan = max - min
        }
      }

      samples.push(sample)
    }

    samples.sort((a, b) => a.t - b.t)
    return downsample(samples, rangeMinutes <= 5 ? 600 : 480)
  }, [points, metric.key, rangeMinutes, showBand])

  const latest = data.length > 0 ? data[data.length - 1].value : null

  return (
    <section className="card">
      <div className="card__header">
        <h2 className="card__title" style={{ color: metric.color }}>
          {metric.label}
        </h2>
        <div className={styles.meta}>
          <span className="mono">
            {formatValue(latest, metric.digits)} {metric.unit}
          </span>
          <span className="muted">
            last {formatRangeLabel(rangeMinutes)} · normal {metric.normal[0]}–{metric.normal[1]}
          </span>
        </div>
      </div>

      <div className={styles.chart}>
        {data.length === 0 ? (
          <div className="empty">Waiting for {metric.short} samples…</div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={data} margin={{ top: 10, right: 10, bottom: 0, left: 0 }}>
              <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" vertical={false} />
              <ReferenceArea
                y1={metric.normal[0]}
                y2={metric.normal[1]}
                fill={metric.color}
                fillOpacity={0.06}
              />
              <XAxis
                dataKey="t"
                type="number"
                domain={['dataMin', 'dataMax']}
                tickFormatter={(value) => formatTime(new Date(Number(value)).toISOString(), true)}
                tick={{ fill: '#64748b', fontSize: 10 }}
                axisLine={false}
                tickLine={false}
                minTickGap={46}
              />
              <YAxis
                domain={[metric.domain[0], metric.domain[1]]}
                width={36}
                tick={{ fill: '#64748b', fontSize: 10 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                contentStyle={{
                  background: '#ffffff',
                  border: '1px solid #e2e8f0',
                  borderRadius: 8,
                  fontSize: 12,
                  boxShadow: '0 1px 3px 0 rgb(0 0 0 / 0.05), 0 1px 2px -1px rgb(0 0 0 / 0.05)',
                }}
                labelStyle={{ color: '#64748b' }}
                labelFormatter={(value) => formatTime(new Date(Number(value)).toISOString(), true)}
              />
              {showBand && (
                <>
                  <Area
                    dataKey="bandMin"
                    stackId="band"
                    stroke="none"
                    fill="none"
                    isAnimationActive={false}
                    connectNulls
                  />
                  <Area
                    dataKey="bandSpan"
                    stackId="band"
                    stroke="none"
                    fill={metric.color}
                    fillOpacity={0.14}
                    isAnimationActive={false}
                    connectNulls
                  />
                </>
              )}
              <Line
                type="monotone"
                dataKey="value"
                name={metric.label}
                unit={` ${metric.unit}`}
                stroke={metric.color}
                strokeWidth={rangeMinutes <= 5 ? 1.5 : 2}
                dot={data.length <= 60 ? { r: 2.2, strokeWidth: 0, fill: metric.color } : false}
                activeDot={{ r: 3.5, strokeWidth: 2, stroke: '#ffffff', fill: metric.color }}
                isAnimationActive={false}
                connectNulls
              />
            </ComposedChart>
          </ResponsiveContainer>
        )}
      </div>
    </section>
  )
}
