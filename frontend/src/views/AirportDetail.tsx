/**
 airport detail panel
 */
import React, { useEffect, useRef } from 'react'
import * as d3 from 'd3'
import { useQuery } from '@tanstack/react-query'
import { fetchAirport, fetchAirportHourly, fetchAirportPropagation } from '../api'
import type { HourlyDelay, PropagationSummary } from '../types'

interface Props {
  code: string
  onBack: () => void
}

//colors

function delayColor(avg: number): string {
  if (avg <= 0)  return '#2ea043'
  if (avg <= 5)  return '#2ea043'
  if (avg <= 15) return '#d29922'
  if (avg <= 30) return '#f78166'
  return '#da3633'
}

function pct(v: number | undefined | null, digits = 1): string {
  if (v == null) return '—'
  return (v * 100).toFixed(digits) + '%'
}
function min(v: number | undefined | null): string {
  if (v == null) return '—'
  return v.toFixed(1) + ' min'
}
function fmt(v: number | undefined | null): string {
  if (v == null) return '—'
  return v.toLocaleString()
}

// hourly bar chart

function HourlyChart({ data }: { data: HourlyDelay[] }) {
  const ref = useRef<SVGSVGElement>(null)

  useEffect(() => {
    if (!ref.current || !data.length) return
    const W = ref.current.clientWidth || 420, H = 140
    const margin = { top: 10, right: 10, bottom: 24, left: 36 }
    const innerW = W - margin.left - margin.right
    const innerH = H - margin.top - margin.bottom

    const svg = d3.select(ref.current)
      .attr('width', W).attr('height', H)
    svg.selectAll('*').remove()

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`)

    const x = d3.scaleBand()
      .domain(data.map(d => String(d.hour)))
      .range([0, innerW]).padding(0.15)

    const maxDelay = d3.max(data, d => d.avg_dep_delay) ?? 1
    const y = d3.scaleLinear().domain([0, maxDelay]).nice().range([innerH, 0])

    // bars
    g.selectAll('rect')
      .data(data)
      .join('rect')
      .attr('x', d => x(String(d.hour))!)
      .attr('y', d => y(Math.max(0, d.avg_dep_delay)))
      .attr('width', x.bandwidth())
      .attr('height', d => Math.max(0, innerH - y(Math.max(0, d.avg_dep_delay))))
      .attr('fill', d => delayColor(d.avg_dep_delay))
      .attr('rx', 2)

    // iqr
    g.selectAll('.iqr')
      .data(data)
      .join('rect')
      .attr('class', 'iqr')
      .attr('x', d => x(String(d.hour))! + x.bandwidth() * 0.3)
      .attr('y', d => y(Math.max(0, d.p75_dep_delay ?? d.avg_dep_delay)))
      .attr('width', x.bandwidth() * 0.4)
      .attr('height', d => Math.max(0,
        y(Math.max(0, d.p25_dep_delay ?? 0)) - y(Math.max(0, d.p75_dep_delay ?? d.avg_dep_delay))
      ))
      .attr('fill', '#58a6ff33')
      .attr('stroke', '#58a6ff66')
      .attr('stroke-width', 0.5)

    // axes
    g.append('g')
      .attr('transform', `translate(0,${innerH})`)
      .call(
        d3.axisBottom(x)
          .tickValues(data.filter(d => d.hour % 3 === 0).map(d => String(d.hour)))
          .tickFormat(h => `${h}h`)
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
        ax.selectAll('.tick line').attr('stroke-dasharray', '2 2').attr('x2', innerW)
          .attr('stroke', '#21262d')
      })

    svg.append('text')
      .attr('x', margin.left - 2)
      .attr('y', margin.top - 2)
      .attr('font-size', 10)
      .attr('fill', '#6e7681')
      .text('Avg dep delay (min)')
  }, [data])

  return <svg ref={ref} style={{ width: '100%', height: 140 }} />
}

// prop bar

function PropRow({ label, value, max, color }: { label: string; value: number; max: number; color: string }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 5 }}>
      <span style={{ width: 36, fontSize: 11, color: '#58a6ff', fontWeight: 600 }}>{label}</span>
      <div style={{ flex: 1, background: '#21262d', borderRadius: 3, height: 10, overflow: 'hidden' }}>
        <div style={{ width: `${(value / max) * 100}%`, height: '100%', background: color, borderRadius: 3 }} />
      </div>
      <span style={{ width: 42, fontSize: 10, color: '#8b949e', textAlign: 'right' }}>{value.toLocaleString()}</span>
    </div>
  )
}

// main component

const card: React.CSSProperties = {
  background: '#161b22',
  border: '1px solid #30363d',
  borderRadius: 8,
  padding: '14px 16px',
  marginBottom: 12,
}
const label: React.CSSProperties = { fontSize: 11, color: '#6e7681', marginBottom: 2 }
const value: React.CSSProperties = { fontSize: 20, fontWeight: 700, color: '#f0f6fc' }
const statRow: React.CSSProperties = { display: 'flex', gap: 12, marginBottom: 12, flexWrap: 'wrap' }
const statBox: React.CSSProperties = {
  flex: '1 1 100px', background: '#21262d', borderRadius: 6, padding: '10px 12px',
}

export default function AirportDetail({ code, onBack }: Props) {
  const { data: airport, isLoading: apLoading } = useQuery({
    queryKey: ['airport', code],
    queryFn: () => fetchAirport(code),
  })
  const { data: hourly } = useQuery({
    queryKey: ['hourly', code],
    queryFn: () => fetchAirportHourly(code),
  })
  const { data: prop } = useQuery({
    queryKey: ['propagation', code],
    queryFn: () => fetchAirportPropagation(code),
  })

  if (apLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#8b949e' }}>
        Loading…
      </div>
    )
  }

  if (!airport) {
    return (
      <div style={{ padding: 24, color: '#f78166' }}>Airport '{code}' not found in dataset.</div>
    )
  }

  const maxProp = Math.max(
    ...(prop?.as_hub ?? []).map((p: PropagationSummary) => p.propagation_count), 1
  )

  return (
    <div style={{ height: '100%', overflow: 'auto', padding: 20, background: '#0d1117' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
        <button
          onClick={onBack}
          style={{
            background: '#21262d', border: '1px solid #30363d', color: '#8b949e',
            borderRadius: 6, padding: '5px 12px', cursor: 'pointer', fontSize: 12,
          }}
        >← Back</button>
        <div>
          <div style={{ fontSize: 24, fontWeight: 800, color: '#f0f6fc' }}>{code}</div>
          <div style={{ fontSize: 13, color: '#8b949e' }}>
            {airport.full_name || airport.city}
            {airport.state ? `, ${airport.state}` : ''}
          </div>
        </div>
        <div style={{
          marginLeft: 'auto', background: delayColor(airport.avg_dep_delay) + '22',
          border: `1px solid ${delayColor(airport.avg_dep_delay)}`,
          borderRadius: 6, padding: '6px 14px', fontSize: 13, color: delayColor(airport.avg_dep_delay),
        }}>
          Avg delay: {min(airport.avg_dep_delay)}
        </div>
      </div>

      {/* Key stats */}
      <div style={statRow}>
        {[
          { label: 'Total Flights', val: fmt(airport.total_flights) },
          { label: 'On-Time Rate', val: pct(airport.on_time_rate) },
          { label: 'Cancel Rate', val: pct(airport.cancellation_rate) },
          { label: 'Destinations', val: fmt(airport.num_destinations) },
          { label: 'Airlines', val: fmt(airport.num_airlines) },
          { label: 'Community', val: String(airport.community_id ?? '-') },
        ].map(({ label: l, val: v }) => (
          <div key={l} style={statBox}>
            <div style={label}>{l}</div>
            <div style={value}>{v}</div>
          </div>
        ))}
      </div>

      {/* Delay breakdown */}
      {[airport.avg_carrier_delay, airport.avg_weather_delay, airport.avg_nas_delay, airport.avg_late_aircraft_delay].some(v => v != null) && (
        <div style={card}>
          <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 10, color: '#c9d1d9' }}>
            Delay Breakdown (avg minutes)
          </div>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
            {[
              { label: 'Carrier', val: airport.avg_carrier_delay },
              { label: 'Weather', val: airport.avg_weather_delay },
              { label: 'NAS / ATC', val: airport.avg_nas_delay },
              { label: 'Late Aircraft', val: airport.avg_late_aircraft_delay },
            ].map(({ label: l, val: v }) => (
              <div key={l} style={{ ...statBox, flex: '1 1 80px' }}>
                <div style={label}>{l}</div>
                <div style={{ ...value, fontSize: 16 }}>{min(v)}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Network metrics */}
      <div style={card}>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 10, color: '#c9d1d9' }}>
          Network Centrality
        </div>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          {[
            { label: 'Degree', val: airport.degree_centrality?.toFixed(3) ?? '-' },
            { label: 'Betweenness', val: airport.betweenness_centrality?.toFixed(4) ?? '-' },
            { label: 'PageRank', val: airport.pagerank?.toFixed(5) ?? '-' },
          ].map(({ label: l, val: v }) => (
            <div key={l} style={{ ...statBox, flex: '1 1 100px' }}>
              <div style={label}>{l}</div>
              <div style={{ ...value, fontSize: 16 }}>{v}</div>
            </div>
          ))}
        </div>
      </div>

      {/* hourly delay chart */}
      {hourly && hourly.length > 0 && (
        <div style={card}>
          <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8, color: '#c9d1d9' }}>
            Delay by Hour of Day
          </div>
          <div style={{ fontSize: 11, color: '#6e7681', marginBottom: 8 }}>
            Bars = avg departure delay : Blue band = IQR
          </div>
          <HourlyChart data={hourly} />
        </div>
      )}

      {/* propagation */}
      {prop && (prop.as_hub.length > 0 || prop.as_destination.length > 0) && (
        <div style={card}>
          <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 10, color: '#c9d1d9' }}>
            Delay Propagation
          </div>

          {prop.as_hub.length > 0 && (
            <>
              <div style={{ fontSize: 11, color: '#8b949e', marginBottom: 8 }}>
                Routes most affected by delays originating here ({code} as hub):
              </div>
              {prop.as_hub.slice(0, 10).map((p: PropagationSummary) => (
                <PropRow
                  key={p.outbound_dest}
                  label={p.outbound_dest}
                  value={p.propagation_count}
                  max={maxProp}
                  color="#f78166"
                />
              ))}
            </>
          )}

          {prop.as_destination.length > 0 && (
            <>
              <div style={{ fontSize: 11, color: '#8b949e', margin: '10px 0 8px', paddingTop: 8, borderTop: '1px solid #21262d' }}>
                Hubs that most often propagate delays into {code}:
              </div>
              {prop.as_destination.slice(0, 10).map((p: PropagationSummary) => (
                <PropRow
                  key={p.hub_airport}
                  label={p.hub_airport}
                  value={p.propagation_count}
                  max={maxProp}
                  color="#58a6ff"
                />
              ))}
            </>
          )}
        </div>
      )}
    </div>
  )
}
