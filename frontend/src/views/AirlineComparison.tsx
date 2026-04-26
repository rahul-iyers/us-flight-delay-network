import React, { useEffect, useRef, useState, useCallback } from 'react'
import * as d3 from 'd3'
import { useQuery } from '@tanstack/react-query'
import { fetchAirlines, fetchTopHubs } from '../api'
import type { AirlineStat, PropagationHub } from '../types'

const PALETTE = [
  '#58a6ff', '#3fb950', '#d29922', '#f78166', '#bc8cff',
  '#79c0ff', '#56d364', '#ffa657', '#ff7b72', '#d2a8ff',
]
const airlineColor = (i: number) => PALETTE[i % PALETTE.length]

// helpers for formatting
function fmtDelay(v: number | null | undefined) {
  return v != null ? v.toFixed(1) + 'm' : '-'
}
function fmtPct(v: number | null | undefined) {
  return v != null ? (v * 100).toFixed(1) + '%' : '-'
}

function DelayBarChart({ data, metric }: { data: AirlineStat[]; metric: keyof AirlineStat }) {
  const wrapRef = useRef<HTMLDivElement>(null)
  const svgRef  = useRef<SVGSVGElement>(null)

  const draw = useCallback(() => {
    if (!svgRef.current || !wrapRef.current || !data.length) return

    const W = wrapRef.current.clientWidth

    const H = 280
    const m = { top: 10, right: 16, bottom: 90, left: 52 }
    const iW = W - m.left - m.right
    const iH = H - m.top - m.bottom

    // filter to real values
    const valid = data.filter(d => d[metric] != null && isFinite(d[metric] as number))

    const svg = d3.select(svgRef.current).attr('width', W).attr('height', H)
    svg.selectAll('*').remove()
    const g = svg.append('g').attr('transform', `translate(${m.left},${m.top})`)

    const sorted = [...valid].sort((a, b) => (b[metric] as number) - (a[metric] as number))

    const x = d3.scaleBand()
      .domain(sorted.map(d => d.airline_code))
      .range([0, iW]).padding(0.25)

    const maxVal = d3.max(sorted, d => d[metric] as number) ?? 1
    const y = d3.scaleLinear().domain([0, maxVal]).nice().range([iH, 0])

    //grid lines
    g.append('g').selectAll('line')
      .data(y.ticks(5))
      .join('line')
      .attr('x1', 0).attr('x2', iW)
      .attr('y1', d => y(d)).attr('y2', d => y(d))
      .attr('stroke', '#21262d')

    //bars
    g.selectAll('rect')
      .data(sorted)
      .join('rect')
      .attr('x', d => x(d.airline_code)!)
      .attr('y', d => y(Math.max(0, d[metric] as number)))
      .attr('width', x.bandwidth())
      .attr('height', d => Math.max(0, iH - y(Math.max(0, d[metric] as number))))
      .attr('fill', (_, i) => airlineColor(i))

    //label values
    g.selectAll('.val')
      .data(sorted)
      .join('text')
      .attr('class', 'val')
      .attr('x', d => x(d.airline_code)! + x.bandwidth() / 2)
      .attr('y', d => y(Math.max(0, d[metric] as number)) - 4)
      .attr('text-anchor', 'middle')
      .attr('font-size', 9)
      .attr('fill', '#8b949e')
      .text(d => {
        const v = d[metric] as number
        return metric.includes('rate') ? `${(v * 100).toFixed(1)}%` : v.toFixed(1)
      })

    // x
    g.append('g')
      .attr('transform', `translate(0,${iH})`)
      .call(d3.axisBottom(x))
      .call(ax => {
        ax.selectAll('text')
          .attr('fill', '#6e7681').attr('font-size', 10)
          .attr('transform', 'rotate(-35)').attr('text-anchor', 'end')
        ax.selectAll('line,path').attr('stroke', '#30363d')
      })

    // y
    g.append('g')
      .call(d3.axisLeft(y).ticks(5).tickFormat(v =>
        metric.includes('rate') ? `${(+v * 100).toFixed(0)}%` : `${v}m`
      ))
      .call(ax => {
        ax.selectAll('text').attr('fill', '#6e7681').attr('font-size', 9)
        ax.selectAll('line,path').attr('stroke', '#30363d')
      })
  }, [data, metric])

  useEffect(() => { draw() }, [draw])

  useEffect(() => {
    const obs = new ResizeObserver(draw)
    if (wrapRef.current) obs.observe(wrapRef.current)
    return () => obs.disconnect()
  }, [draw])

  return (
    <div ref={wrapRef} style={{ width: '100%' }}>
      <svg ref={svgRef} style={{ display: 'block', width: '100%', height: 280 }} />
    </div>
  )
}

function ScatterPlot({ data }: { data: AirlineStat[] }) {
  const wrapRef = useRef<HTMLDivElement>(null)
  const svgRef  = useRef<SVGSVGElement>(null)

  const draw = useCallback(() => {
    if (!svgRef.current || !wrapRef.current || !data.length) return

    const W = wrapRef.current.clientWidth
    const H = 260
    const m = { top: 16, right: 16, bottom: 36, left: 52 }
    const iW = W - m.left - m.right
    const iH = H - m.top - m.bottom

    const valid = data.filter(d => d.on_time_rate != null && d.total_flights > 0)

    const svg = d3.select(svgRef.current).attr('width', W).attr('height', H)
    svg.selectAll('*').remove()
    const g = svg.append('g').attr('transform', `translate(${m.left},${m.top})`)

    const xExtent = d3.extent(valid, d => d.total_flights) as [number, number]
    const x = d3.scaleLog().domain(xExtent).nice().range([0, iW])

    const yMax = d3.max(valid, d => d.on_time_rate) ?? 1
    const y = d3.scaleLinear().domain([0, yMax]).nice().range([iH, 0])

    //grid
    g.append('g').selectAll('line')
      .data(y.ticks(5))
      .join('line')
      .attr('x1', 0).attr('x2', iW)
      .attr('y1', d => y(d)).attr('y2', d => y(d))
      .attr('stroke', '#21262d')

    //points
    g.selectAll('circle')
      .data(valid)
      .join('circle')
      .attr('cx', d => x(d.total_flights))
      .attr('cy', d => y(d.on_time_rate))
      .attr('r', 7)
      .attr('fill', (_, i) => airlineColor(i))
      .attr('opacity', 0.85)

    //labels
    g.selectAll('text.lbl')
      .data(valid)
      .join('text')
      .attr('class', 'lbl')
      .attr('x', d => x(d.total_flights) + 9)
      .attr('y', d => y(d.on_time_rate) + 4)
      .attr('font-size', 9)
      .attr('fill', '#8b949e')
      .text(d => d.airline_code)

    //axes
    g.append('g')
      .attr('transform', `translate(0,${iH})`)
      .call(d3.axisBottom(x).tickFormat(v => d3.format('.2s')(+v)))
      .call(ax => {
        ax.selectAll('text').attr('fill', '#6e7681').attr('font-size', 9)
        ax.selectAll('line,path').attr('stroke', '#30363d')
      })

    g.append('g')
      .call(d3.axisLeft(y).ticks(5).tickFormat(v => `${(+v * 100).toFixed(0)}%`))
      .call(ax => {
        ax.selectAll('text').attr('fill', '#6e7681').attr('font-size', 9)
        ax.selectAll('line,path').attr('stroke', '#30363d')
      })

    svg.append('text')
      .attr('x', m.left + iW / 2).attr('y', H - 2)
      .attr('text-anchor', 'middle').attr('font-size', 10).attr('fill', '#6e7681')
      .text('Total Flights (log scale)')

    svg.append('text')
      .attr('transform', 'rotate(-90)')
      .attr('x', -(m.top + iH / 2)).attr('y', 14)
      .attr('text-anchor', 'middle').attr('font-size', 10).attr('fill', '#6e7681')
      .text('On-Time Rate')
  }, [data])

  useEffect(() => { draw() }, [draw])

  useEffect(() => {
    const obs = new ResizeObserver(draw)
    if (wrapRef.current) obs.observe(wrapRef.current)
    return () => obs.disconnect()
  }, [draw])

  return (
    <div ref={wrapRef} style={{ width: '100%' }}>
      <svg ref={svgRef} style={{ display: 'block', width: '100%', height: 260 }} />
    </div>
  )
}

function PropHubsTable({ data }: { data: PropagationHub[] }) {
  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
      <thead>
        <tr style={{ borderBottom: '1px solid #30363d', color: '#6e7681', fontSize: 11 }}>
          <th style={{ textAlign: 'left',  padding: '5px 8px' }}>Hub</th>
          <th style={{ textAlign: 'right', padding: '5px 8px' }}>Propagations</th>
          <th style={{ textAlign: 'right', padding: '5px 8px' }}>Dest. affected</th>
          <th style={{ textAlign: 'right', padding: '5px 8px' }}>Avg delay out</th>
        </tr>
      </thead>
      <tbody>
        {data.map((h, i) => (
          <tr key={h.hub_airport} style={{ borderBottom: '1px solid #21262d', background: i % 2 === 0 ? 'transparent' : '#161b2244' }}>
            <td style={{ padding: '5px 8px', color: '#58a6ff', fontWeight: 600 }}>{h.hub_airport}</td>
            <td style={{ padding: '5px 8px', textAlign: 'right', color: '#f78166' }}>{h.total_propagations.toLocaleString()}</td>
            <td style={{ padding: '5px 8px', textAlign: 'right', color: '#c9d1d9' }}>{h.unique_destinations}</td>
            <td style={{ padding: '5px 8px', textAlign: 'right', color: '#d29922' }}>{fmtDelay(h.avg_outbound_delay)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

// main funcs

type Metric = 'avg_dep_delay' | 'cancellation_rate' | 'on_time_rate' | 'dep_delay_rate'

const METRICS: { key: Metric; label: string }[] = [
  { key: 'avg_dep_delay', label: 'Avg Dep Delay' },
  { key: 'on_time_rate', label: 'On-Time Rate' },
  { key: 'cancellation_rate', label: 'Cancellation Rate' },
  { key: 'dep_delay_rate', label: '% Flights Delayed' },
]

const card: React.CSSProperties = {
  background: '#161b22', border: '1px solid #30363d',
  borderRadius: 8, padding: '14px 16px', marginBottom: 14,
}

export default function AirlineComparison() {
  const [metric, setMetric] = useState<Metric>('avg_dep_delay')

  const { data: airlines } = useQuery({
    queryKey: ['airlines'],
    queryFn: () => fetchAirlines(30),
  })

  const { data: hubs } = useQuery({
    queryKey: ['top-hubs'],
    queryFn: () => fetchTopHubs(15),
  })

  if (!airlines) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#8b949e' }}>
        Loading airline data...
      </div>
    )
  }

  return (
    <div style={{ height: '100%', overflow: 'auto', padding: 20, background: '#0d1117' }}>
      <div style={{ fontSize: 18, fontWeight: 700, color: '#f0f6fc', marginBottom: 4 }}>Airline Comparison</div>
      <div style={{ fontSize: 12, color: '#8b949e', marginBottom: 16 }}>
        Comparing {airlines.length} carriers across key performance metrics.
      </div>

      {/* metric selector */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 14, flexWrap: 'wrap' }}>
        {METRICS.map(m => (
          <button
            key={m.key}
            onClick={() => setMetric(m.key)}
            style={{
              padding: '5px 12px', borderRadius: 6, border: '1px solid',
              borderColor: metric === m.key ? '#58a6ff' : '#30363d',
              background: metric === m.key ? '#1f6feb22' : 'transparent',
              color: metric === m.key ? '#58a6ff' : '#8b949e',
              cursor: 'pointer', fontSize: 12,
            }}
          >
            {m.label}
          </button>
        ))}
      </div>

      {/* bar chart */}
      <div style={card}>
        <div style={{ fontSize: 13, fontWeight: 600, color: '#c9d1d9', marginBottom: 10 }}>
          {METRICS.find(m => m.key === metric)?.label} by Airline
        </div>
        <DelayBarChart data={airlines} metric={metric} />
      </div>

      {/* scatter */}
      <div style={card}>
        <div style={{ fontSize: 13, fontWeight: 600, color: '#c9d1d9', marginBottom: 6 }}>
          On-Time Rate vs. Flight Volume
        </div>
        <div style={{ fontSize: 11, color: '#6e7681', marginBottom: 8 }}>
          Larger airlines are not necessarily less punctual.
        </div>
        <ScatterPlot data={airlines} />
      </div>

      {/* summary table */}
      <div style={card}>
        <div style={{ fontSize: 13, fontWeight: 600, color: '#c9d1d9', marginBottom: 10 }}>
          All Airlines - Summary Stats
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #30363d', color: '#6e7681' }}>
                {['Code', 'Name', 'Flights', 'Avg Dep', 'On-Time', 'Cancel', '% Delayed', 'Airports', 'Routes'].map(h => (
                  <th key={h} style={{ padding: '5px 8px', textAlign: h === 'Code' || h === 'Name' ? 'left' : 'right' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[...airlines]
                .sort((a, b) => b.total_flights - a.total_flights)
                .map((a, i) => (
                  <tr key={a.airline_code} style={{ borderBottom: '1px solid #21262d', background: i % 2 === 0 ? 'transparent' : '#161b2244' }}>
                    <td style={{ padding: '5px 8px', color: '#58a6ff', fontWeight: 600 }}>{a.airline_code}</td>
                    <td style={{ padding: '5px 8px', color: '#c9d1d9' }}>{a.airline_name}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{a.total_flights.toLocaleString()}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', color: (a.avg_dep_delay ?? 0) > 15 ? '#f78166' : '#3fb950' }}>{fmtDelay(a.avg_dep_delay)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmtPct(a.on_time_rate)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmtPct(a.cancellation_rate)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmtPct(a.dep_delay_rate)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{a.airports_served}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{a.routes_served}</td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* top propagation hubs */}
      {hubs && hubs.length > 0 && (
        <div style={card}>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#c9d1d9', marginBottom: 10 }}>
            Top Delay Propagation Hubs
          </div>
          <div style={{ fontSize: 11, color: '#6e7681', marginBottom: 10 }}>
            Airports that most often propagate delays to downstream routes.
          </div>
          <PropHubsTable data={hubs} />
        </div>
      )}
    </div>
  )
}
