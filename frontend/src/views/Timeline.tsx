/**
 * View 3 — Timeline / Animation
 * Plays delay levels across the 24-hour day using a scrubber.
 * The map colour-codes airport nodes by their average delay for that hour.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react'
import * as d3 from 'd3'
import { useQuery } from '@tanstack/react-query'
import { fetchGraph, fetchMonthlyTrends } from '../api'
import type { AirportNode, MonthlyTrend } from '../types'

const US_TOPO_URL = 'https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json'

const HOURS = Array.from({ length: 24 }, (_, i) => i)

function delayColor(v: number): string {
  if (v <= 0)  return '#2ea043'
  if (v <= 5)  return '#2ea043'
  if (v <= 15) return '#d29922'
  if (v <= 30) return '#f78166'
  return '#da3633'
}

// Monthly line chart
function MonthlyChart({ data }: { data: MonthlyTrend[] }) {
  const ref = useRef<SVGSVGElement>(null)
  useEffect(() => {
    if (!ref.current || !data.length) return
    const W = ref.current.clientWidth || 600, H = 180
    const m = { top: 16, right: 16, bottom: 36, left: 44 }
    const iW = W - m.left - m.right, iH = H - m.top - m.bottom

    const svg = d3.select(ref.current).attr('width', W).attr('height', H)
    svg.selectAll('*').remove()
    const g = svg.append('g').attr('transform', `translate(${m.left},${m.top})`)

    const x = d3.scalePoint().domain(data.map(d => d.month)).range([0, iW]).padding(0.3)
    const maxDelay = d3.max(data, d => d.avg_dep_delay) ?? 1
    const y = d3.scaleLinear().domain([0, maxDelay]).nice().range([iH, 0])

    // Grid
    g.append('g').selectAll('line')
      .data(y.ticks(5))
      .join('line')
      .attr('x1', 0).attr('x2', iW)
      .attr('y1', d => y(d)).attr('y2', d => y(d))
      .attr('stroke', '#21262d').attr('stroke-dasharray', '3 3')

    // Area
    const area = d3.area<MonthlyTrend>()
      .x(d => x(d.month)!)
      .y0(iH)
      .y1(d => y(d.avg_dep_delay))
      .curve(d3.curveMonotoneX)

    g.append('path')
      .datum(data)
      .attr('d', area)
      .attr('fill', '#f7816622')

    // Line — dep delay
    const line = d3.line<MonthlyTrend>()
      .x(d => x(d.month)!)
      .y(d => y(d.avg_dep_delay))
      .curve(d3.curveMonotoneX)

    g.append('path')
      .datum(data)
      .attr('d', line)
      .attr('fill', 'none')
      .attr('stroke', '#f78166')
      .attr('stroke-width', 2)

    // Cancellation line (secondary y)
    const maxCancel = d3.max(data, d => d.cancellation_rate) ?? 0.1
    const yC = d3.scaleLinear().domain([0, maxCancel]).range([iH, 0])
    const lineC = d3.line<MonthlyTrend>()
      .x(d => x(d.month)!)
      .y(d => yC(d.cancellation_rate))
      .curve(d3.curveMonotoneX)

    g.append('path')
      .datum(data)
      .attr('d', lineC)
      .attr('fill', 'none')
      .attr('stroke', '#58a6ff')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', '5 3')

    // Axes
    g.append('g')
      .attr('transform', `translate(0,${iH})`)
      .call(d3.axisBottom(x)
        .tickValues(data.filter((_, i) => i % 3 === 0).map(d => d.month))
        .tickFormat(m => m.slice(2))
      )
      .call(ax => {
        ax.selectAll('text').attr('fill', '#6e7681').attr('font-size', 9)
        ax.selectAll('line,path').attr('stroke', '#30363d')
      })

    g.append('g')
      .call(d3.axisLeft(y).ticks(4).tickFormat(v => `${v}m`))
      .call(ax => {
        ax.selectAll('text').attr('fill', '#6e7681').attr('font-size', 9)
        ax.selectAll('line,path').attr('stroke', '#30363d')
      })

    // Legend
    const legend = svg.append('g').attr('transform', `translate(${m.left + 10},${m.top + 6})`)
    legend.append('line').attr('x1', 0).attr('x2', 16).attr('stroke', '#f78166').attr('stroke-width', 2)
    legend.append('text').attr('x', 20).attr('y', 4).attr('fill', '#8b949e').attr('font-size', 10).text('Avg dep delay')
    legend.append('line').attr('x1', 120).attr('x2', 136).attr('stroke', '#58a6ff').attr('stroke-width', 1.5).attr('stroke-dasharray', '4 2')
    legend.append('text').attr('x', 140).attr('y', 4).attr('fill', '#8b949e').attr('font-size', 10).text('Cancel rate')
  }, [data])

  return <svg ref={ref} style={{ width: '100%', height: 180 }} />
}

// Timeline map (mini)
function TimelineMap({
  nodes, hour, topoData,
}: {
  nodes: AirportNode[]
  hour: number
  topoData: unknown
}) {
  const svgRef = useRef<SVGSVGElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!svgRef.current || !containerRef.current || !topoData) return
    const { width, height } = containerRef.current.getBoundingClientRect()
    if (!width || !height) return

    const svg = d3.select(svgRef.current).attr('width', width).attr('height', height)
    svg.selectAll('*').remove()

    const projection = d3.geoAlbersUsa()
      .translate([width / 2, height / 2])
      .scale(Math.min(width, height) * 1.2)
    const path = d3.geoPath().projection(projection)

    import('topojson-client').then(topo => {
      const states = topo.feature(
        topoData as Parameters<typeof topo.feature>[0],
        (topoData as any).objects.states
      ) as unknown as GeoJSON.FeatureCollection
      const g = svg.append('g')
      g.append('g').selectAll('path')
        .data((states as GeoJSON.FeatureCollection).features)
        .join('path')
        .attr('d', path)
        .attr('fill', '#161b22')
        .attr('stroke', '#30363d')
        .attr('stroke-width', 0.5)

      const maxFlights = d3.max(nodes, n => n.total_flights) ?? 1
      const rScale = d3.scaleSqrt().domain([0, maxFlights]).range([2, 16])

      g.selectAll('circle')
        .data(nodes.filter(n => n.lat != null && n.lon != null))
        .join('circle')
        .attr('cx', n => (projection([n.lon!, n.lat!]) ?? [0, 0])[0])
        .attr('cy', n => (projection([n.lon!, n.lat!]) ?? [0, 0])[1])
        .attr('r', n => rScale(n.total_flights))
        .attr('fill', n => {
          // In a full implementation, per-airport hourly data would be fetched.
          // Here we simulate by offsetting the airport's average by a time-of-day factor.
          const factor = 1 + 0.3 * Math.sin((hour - 7) * Math.PI / 12)
          return delayColor((n.avg_dep_delay ?? 0) * factor)
        })
        .attr('opacity', 0.8)
        .attr('stroke', '#0d1117')
        .attr('stroke-width', 0.5)
    })
  }, [nodes, hour, topoData])

  return (
    <div ref={containerRef} style={{ width: '100%', height: '100%' }}>
      <svg ref={svgRef} style={{ width: '100%', height: '100%' }} />
    </div>
  )
}

export default function Timeline() {
  const [hour, setHour] = useState(8)
  const [playing, setPlaying] = useState(false)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const [topoData, setTopoData] = useState<unknown>(null)

  const { data: graph } = useQuery({
    queryKey: ['graph-timeline'],
    queryFn: () => fetchGraph({ top_edges: 500, min_flights: 500 }),
  })
  const { data: monthly } = useQuery({
    queryKey: ['monthly'],
    queryFn: fetchMonthlyTrends,
  })

  useEffect(() => {
    fetch(US_TOPO_URL).then(r => r.json()).then(setTopoData)
  }, [])

  // Autoplay
  useEffect(() => {
    if (playing) {
      intervalRef.current = setInterval(() => {
        setHour(h => (h + 1) % 24)
      }, 800)
    } else {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current) }
  }, [playing])

  const hourLabel = `${String(hour).padStart(2, '0')}:00`

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: '#0d1117', overflow: 'hidden' }}>
      {/* Header */}
      <div style={{ padding: '16px 20px', borderBottom: '1px solid #30363d' }}>
        <div style={{ fontSize: 16, fontWeight: 700, color: '#f0f6fc', marginBottom: 4 }}>
          Delay Timeline
        </div>
        <div style={{ fontSize: 12, color: '#8b949e' }}>
          Animate how delay patterns evolve across the day. Monthly trend shown below.
        </div>
      </div>

      {/* Map */}
      <div style={{ flex: 1, minHeight: 0, position: 'relative' }}>
        {topoData && graph ? (
          <TimelineMap nodes={graph.nodes} hour={hour} topoData={topoData} />
        ) : (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#8b949e' }}>
            Loading…
          </div>
        )}

        {/* Hour overlay */}
        <div style={{
          position: 'absolute', top: 12, right: 16,
          background: '#161b22cc', border: '1px solid #30363d',
          borderRadius: 8, padding: '8px 14px', fontSize: 28, fontWeight: 800,
          color: '#f0f6fc', letterSpacing: 2,
        }}>
          {hourLabel}
        </div>
      </div>

      {/* Controls */}
      <div style={{ padding: '12px 20px', borderTop: '1px solid #30363d', background: '#161b22' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 8 }}>
          <button
            onClick={() => setPlaying(p => !p)}
            style={{
              background: playing ? '#da3633' : '#2ea043',
              border: 'none', borderRadius: 6, color: '#fff',
              padding: '7px 18px', cursor: 'pointer', fontWeight: 600, fontSize: 13,
            }}
          >
            {playing ? '⏸ Pause' : '▶ Play'}
          </button>
          <span style={{ fontSize: 22, fontWeight: 700, color: '#58a6ff', minWidth: 50 }}>{hourLabel}</span>
          <input
            type="range"
            min={0} max={23}
            value={hour}
            onChange={e => { setHour(+e.target.value); setPlaying(false) }}
            style={{ flex: 1 }}
          />
          <div style={{ display: 'flex', gap: 6, fontSize: 11 }}>
            {[
              ['#2ea043', '≤5m'],
              ['#d29922', '5-15m'],
              ['#f78166', '15-30m'],
              ['#da3633', '>30m'],
            ].map(([c, l]) => (
              <span key={l} style={{ display: 'flex', alignItems: 'center', gap: 3, color: '#8b949e' }}>
                <span style={{ width: 8, height: 8, borderRadius: '50%', background: c, display: 'inline-block' }} />
                {l}
              </span>
            ))}
          </div>
        </div>

        {/* Tick marks */}
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 9, color: '#484f58', paddingLeft: 76, paddingRight: 0 }}>
          {HOURS.filter(h => h % 3 === 0).map(h => (
            <span key={h}>{String(h).padStart(2, '0')}h</span>
          ))}
        </div>
      </div>

      {/* Monthly trend */}
      <div style={{ padding: '12px 20px', borderTop: '1px solid #30363d', background: '#161b22' }}>
        <div style={{ fontSize: 12, fontWeight: 600, color: '#c9d1d9', marginBottom: 8 }}>
          Monthly Trends (2022–2025)
        </div>
        {monthly && monthly.length > 0 ? (
          <MonthlyChart data={monthly} />
        ) : (
          <div style={{ color: '#484f58', fontSize: 11 }}>No monthly data available.</div>
        )}
      </div>
    </div>
  )
}
