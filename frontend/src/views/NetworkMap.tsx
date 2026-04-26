/**
u.s. network map
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import * as d3 from 'd3'
import * as topojson from 'topojson-client'
import type { Topology } from 'topojson-specification'
import { useQuery } from '@tanstack/react-query'
import { fetchGraph, fetchPropagationSummary, fetchAllHourly } from '../api'
import type { AirportNode, RouteEdge, PropagationSummary } from '../types'

const US_TOPO_URL = 'https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json'

interface Props {
  onSelectAirport: (code: string) => void
  showPropagation: boolean
  filterHour: [number, number]
}

function delayColor(avgDelay: number | null | undefined): string {
  if (avgDelay == null) return '#484f58'
  if (avgDelay <= 5)   return '#2ea043'
  if (avgDelay <= 15)  return '#d29922'
  if (avgDelay <= 30)  return '#f78166'
  return '#da3633'
}

const TOOLTIP_STYLE: React.CSSProperties = {
  position: 'absolute',
  background: '#161b22ee',
  border: '1px solid #30363d',
  borderRadius: 6,
  padding: '8px 12px',
  fontSize: 12,
  color: '#e6edf3',
  pointerEvents: 'none',
  maxWidth: 240,
  zIndex: 100,
  lineHeight: 1.6,
}

export default function NetworkMap({ onSelectAirport, showPropagation, filterHour }: Props) {
  const svgRef = useRef<SVGSVGElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [tooltip, setTooltip] = useState<{ x: number; y: number; html: string } | null>(null)
  const [topoData, setTopoData] = useState<Topology | null>(null)

  const { data: graph, isLoading: graphLoading } = useQuery({
    queryKey: ['graph'],
    queryFn: () => fetchGraph({ top_edges: 2000, min_flights: 100 }),
  })

  const { data: propData } = useQuery({
    queryKey: ['propagation-summary'],
    queryFn: () => fetchPropagationSummary({ min_count: 10, limit: 500 }),
    enabled: showPropagation,
  })

  // fetch hourly delay averages for the selected hour
  const { data: hourlyData } = useQuery({
    queryKey: ['hourly-all', filterHour[0], filterHour[1]],
    queryFn: () => fetchAllHourly(filterHour[0], filterHour[1]),
    staleTime: 60_000,
  })

  // map airport_code to avg_dep_delay for the current hour filter
  const hourlyDelayMap = useMemo(() => {
    const m = new Map<string, number>()
    if (hourlyData) {
      for (const row of hourlyData) {
        m.set(row.airport_code, row.avg_dep_delay)
      }
    }
    return m
  }, [hourlyData])

  // fetch us map topology
  useEffect(() => {
    fetch(US_TOPO_URL)
      .then(r => r.json())
      .then(setTopoData)
      .catch(console.error)
  }, [])

  const draw = useCallback(() => {
    if (!svgRef.current || !containerRef.current || !graph || !topoData) return

    const { width, height } = containerRef.current.getBoundingClientRect()
    if (!width || !height) return

    const svg = d3.select(svgRef.current)
    svg.attr('width', width).attr('height', height)
    svg.selectAll('*').remove()

    // projection
    const projection = d3.geoAlbersUsa()
      .translate([width / 2, height / 2])
      .scale(Math.min(width, height) * 1.2)

    const pathGen = d3.geoPath().projection(projection)

    // basemap
    const statesGeo = topojson.feature(topoData, (topoData as any).objects.states) as unknown as GeoJSON.FeatureCollection

    const g = svg.append('g')

    g.append('g')
      .attr('class', 'states')
      .selectAll('path')
      .data(statesGeo.features)
      .join('path')
      .attr('d', pathGen)
      .attr('fill', '#161b22')
      .attr('stroke', '#30363d')
      .attr('stroke-width', 0.5)

    const nodeMap = new Map<string, AirportNode>()
    graph.nodes.forEach(n => nodeMap.set(n.id ?? n.airport_code, n))

    const project = (n: AirportNode): [number, number] | null => {
      if (n.lat == null || n.lon == null) return null
      return projection([n.lon, n.lat]) ?? null
    }

    const nodeDelay = (n: AirportNode): number => {
      const code = n.id ?? n.airport_code
      return hourlyDelayMap.get(code) ?? n.avg_dep_delay ?? 0
    }

    // edges
    const edges = graph.edges.filter(e => {
      const s = nodeMap.get(e.source)
      const t = nodeMap.get(e.target)
      return s && t && project(s) && project(t)
    })

    g.append('g').attr('class', 'edges')
      .selectAll<SVGPathElement, RouteEdge>('path')
      .data(edges)
      .join('path')
      .attr('d', e => {
        const s = nodeMap.get(e.source)!
        const t = nodeMap.get(e.target)!
        const [sx, sy] = project(s)!
        const [tx, ty] = project(t)!
        const dx = tx - sx, dy = ty - sy
        const mx = (sx + tx) / 2 - dy * 0.15
        const my = (sy + ty) / 2 + dx * 0.15
        return `M${sx},${sy} Q${mx},${my} ${tx},${ty}`
      })
      .attr('fill', 'none')
      .attr('stroke', e => delayColor(e.avg_dep_delay))
      .attr('stroke-width', e => Math.max(0.3, Math.log1p(e.total_flights / 5000) * 0.8))
      .attr('stroke-opacity', 0.3)

    // propagation overlay
    if (showPropagation && propData && propData.length > 0) {
      const maxCount = d3.max(propData, (d: PropagationSummary) => d.propagation_count) ?? 1

      g.append('g').attr('class', 'propagation')
        .selectAll<SVGPathElement, PropagationSummary>('path')
        .data(propData.filter((p: PropagationSummary) => {
          const h = nodeMap.get(p.hub_airport)
          const d = nodeMap.get(p.outbound_dest)
          return h && d && project(h) && project(d)
        }))
        .join('path')
        .attr('d', (p: PropagationSummary) => {
          const h = nodeMap.get(p.hub_airport)!
          const d = nodeMap.get(p.outbound_dest)!
          const [sx, sy] = project(h)!
          const [tx, ty] = project(d)!
          const dx = tx - sx, dy = ty - sy
          const mx = (sx + tx) / 2 - dy * 0.2
          const my = (sy + ty) / 2 + dx * 0.2
          return `M${sx},${sy} Q${mx},${my} ${tx},${ty}`
        })
        .attr('fill', 'none')
        .attr('stroke', '#f78166')
        .attr('stroke-width', (p: PropagationSummary) => Math.max(0.5, (p.propagation_count / maxCount) * 3))
        .attr('stroke-opacity', 0.65)
        .attr('stroke-dasharray', '4 3')
    }

    // nodes
    const maxFlights = d3.max(graph.nodes, n => n.total_flights) ?? 1
    const rScale = d3.scaleSqrt().domain([0, maxFlights]).range([3, 18])

    type NodeDatum = { n: AirportNode; pos: [number, number] }
    const nodesWithPos: NodeDatum[] = graph.nodes
      .map(n => ({ n, pos: project(n) }))
      .filter((d): d is NodeDatum => d.pos !== null)

    const nodeG = g.append('g').attr('class', 'nodes')

    nodeG.selectAll<SVGCircleElement, NodeDatum>('circle')
      .data(nodesWithPos)
      .join('circle')
      .attr('cx', d => d.pos[0])
      .attr('cy', d => d.pos[1])
      .attr('r', d => rScale(d.n.total_flights))
      .attr('fill', d => delayColor(nodeDelay(d.n)))
      .attr('stroke', '#0d1117')
      .attr('stroke-width', 1)
      .attr('opacity', 0.88)
      .style('cursor', 'pointer')
      .on('mouseenter', function(event: MouseEvent, d: NodeDatum) {
        const rect = containerRef.current!.getBoundingClientRect()
        const filteredDelay = nodeDelay(d.n)
        const isFiltered = filterHour[0] !== 0 || filterHour[1] !== 23
        setTooltip({
          x: event.clientX - rect.left + 14,
          y: event.clientY - rect.top - 10,
          html: [
            `<strong>${d.n.id ?? d.n.airport_code}</strong> - ${d.n.city ?? ''}, ${d.n.state ?? ''}`,
            d.n.full_name ? `<span style="color:#6e7681">${d.n.full_name}</span>` : '',
            `<hr style="border:none;border-top:1px solid #30363d;margin:4px 0"/>`,
            `Flights: ${(d.n.total_flights ?? 0).toLocaleString()}`,
            isFiltered
              ? `Delay ${filterHour[0]}h-${filterHour[1]}h: <b style="color:${delayColor(filteredDelay)}">${filteredDelay.toFixed(1)} min</b>`
              : `Avg dep delay: <b style="color:${delayColor(filteredDelay)}">${filteredDelay.toFixed(1)} min</b>`,
            `On-time: ${((d.n.on_time_rate ?? 0) * 100).toFixed(1)}%`,
            `Destinations: ${d.n.num_destinations ?? '-'}`,
            `Community: ${d.n.community_id ?? 'N/A'}`,
          ].filter(Boolean).join('<br/>'),
        })
        d3.select(this).attr('stroke', '#58a6ff').attr('stroke-width', 2.5)
      })
      .on('mouseleave', function() {
        setTooltip(null)
        d3.select(this).attr('stroke', '#0d1117').attr('stroke-width', 1)
      })
      .on('click', (_: MouseEvent, d: NodeDatum) => {
        onSelectAirport(d.n.id ?? d.n.airport_code)
      })

    // major hubs labels
    const labelFloorFlights = rScale.invert(7)
    nodeG.selectAll<SVGTextElement, NodeDatum>('text')
      .data(nodesWithPos.filter(d => d.n.total_flights >= labelFloorFlights))
      .join('text')
      .attr('x', d => d.pos[0])
      .attr('y', d => d.pos[1] - rScale(d.n.total_flights) - 3)
      .attr('text-anchor', 'middle')
      .attr('font-size', 8)
      .attr('fill', '#8b949e')
      .attr('pointer-events', 'none')
      .text(d => d.n.id ?? d.n.airport_code)

    // zooming and panning
    svg.call(
      d3.zoom<SVGSVGElement, unknown>()
        .scaleExtent([0.5, 12])
        .on('zoom', event => { g.attr('transform', event.transform.toString()) })
    )
  }, [graph, topoData, showPropagation, propData, onSelectAirport, hourlyDelayMap, filterHour])

  useEffect(() => { draw() }, [draw])

  useEffect(() => {
    const obs = new ResizeObserver(draw)
    if (containerRef.current) obs.observe(containerRef.current)
    return () => obs.disconnect()
  }, [draw])

  const isFiltered = filterHour[0] !== 0 || filterHour[1] !== 23

  return (
    <div
      ref={containerRef}
      style={{ width: '100%', height: '100%', position: 'relative', background: '#0d1117' }}
    >
      {(graphLoading || !topoData) && (
        <div style={{
          position: 'absolute', inset: 0, display: 'flex', alignItems: 'center',
          justifyContent: 'center', color: '#8b949e', fontSize: 14, flexDirection: 'column', gap: 8,
        }}>
          <div style={{ fontSize: 24 }}>✈</div>
          <div>{graphLoading ? 'Loading flight data...' : 'Loading basemap...'}</div>
        </div>
      )}
      <svg ref={svgRef} style={{ display: 'block', width: '100%', height: '100%' }} />

      {/* hour filter indicator */}
      {isFiltered && (
        <div style={{
          position: 'absolute', top: 10, left: 10,
          background: '#1f6feb33', border: '1px solid #1f6feb',
          borderRadius: 6, padding: '5px 10px', fontSize: 11, color: '#58a6ff',
          pointerEvents: 'none',
        }}>
          Showing delays: {String(filterHour[0]).padStart(2,'0')}:00 - {String(filterHour[1]).padStart(2,'0')}:59
        </div>
      )}

      {tooltip && (
        <div
          style={{ ...TOOLTIP_STYLE, left: tooltip.x, top: tooltip.y }}
          dangerouslySetInnerHTML={{ __html: tooltip.html }}
        />
      )}
      <div style={{
        position: 'absolute', bottom: 10, right: 14, fontSize: 10,
        color: '#484f58', pointerEvents: 'none',
      }}>
        Scroll to zoom : Drag to pan : Click airport for detail
      </div>
    </div>
  )
}
