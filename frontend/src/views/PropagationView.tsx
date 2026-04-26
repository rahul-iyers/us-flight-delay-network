/**
 delay propagation tree
 */
import React, { useCallback, useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'
import { useQuery } from '@tanstack/react-query'
import { fetchPropagationTree, fetchAirportHourly } from '../api'
import type { HourlyDelay, PropagationTreeData, PropagationTreeNode } from '../types'

// constants

const POPULAR = ['ATL', 'ORD', 'DFW', 'DEN', 'LAX', 'CLT', 'LAS', 'PHX', 'MCO', 'SEA', 'JFK', 'SFO']

// helpers

function delayColor(avg: number): string {
  if (avg <= 0)  return '#2ea043'
  if (avg <= 5)  return '#2ea043'
  if (avg <= 15) return '#d29922'
  if (avg <= 30) return '#f78166'
  return '#da3633'
}

function radialPoint(angle: number, r: number): [number, number] {
  return [r * Math.sin(angle), -r * Math.cos(angle)]
}

function fmtCount(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000)     return (n / 1_000).toFixed(1) + 'k'
  return String(n)
}

// d3 radial tree drawing

interface DrawCallbacks {
  onNodeClick: (airport: string) => void
  onHover: (state: TooltipState | null) => void
}

function drawTree(svgEl: SVGSVGElement, data: PropagationTreeData, cb: DrawCallbacks) {
  const W = svgEl.clientWidth  || 800
  const H = svgEl.clientHeight || 580
  const cx = W / 2
  const cy = H / 2
  const radius = Math.min(W, H) / 2 - 90

  const svg = d3.select(svgEl)
  svg.selectAll('*').remove()

  // zoom/panning functionality
  const g = svg.append('g')
  const zoom = d3.zoom<SVGSVGElement, unknown>()
    .scaleExtent([0.2, 5])
    .on('zoom', e => g.attr('transform', e.transform.toString()))
  svg.call(zoom)
  svg.call(zoom.transform, d3.zoomIdentity.translate(cx, cy))

  let root: d3.HierarchyNode<PropagationTreeNode>
  try {
    const stratify = d3.stratify<PropagationTreeNode>()
      .id(d => d.id)
      .parentId(d => d.parent ?? null)
    root = stratify(data.nodes)
  } catch {
    return
  }

  // radial tree
  const treeLayout = d3.tree<PropagationTreeNode>()
    .size([2 * Math.PI, radius])
    .separation((a, b) => (a.parent === b.parent ? 1 : 2) / Math.max(1, a.depth))

  const computed = treeLayout(root)

  // scales
  const counts = data.nodes.filter(n => n.hop > 0).map(n => n.propagation_count)
  const maxCount = d3.max(counts) ?? 1
  const sizeScale = d3.scaleSqrt().domain([0, maxCount]).range([4, 20])

  // concentric rings
  const maxHop = d3.max(data.nodes, d => d.hop) ?? 0
  for (let h = 1; h <= maxHop; h++) {
    const r = (radius * h) / maxHop
    g.append('circle')
      .attr('r', r)
      .attr('fill', 'none')
      .attr('stroke', '#21262d')
      .attr('stroke-dasharray', '3 4')
      .attr('opacity', 0.5)
  }

  // links
  const hopOpacity = [0, 0.75, 0.50, 0.30, 0.18]

  const linkGen = d3.linkRadial<
    d3.HierarchyLink<PropagationTreeNode>,
    d3.HierarchyPointNode<PropagationTreeNode>
  >()
    .angle(d => d.x)
    .radius(d => d.y)

  g.selectAll<SVGPathElement, d3.HierarchyLink<PropagationTreeNode>>('.link')
    .data(computed.links())
    .join('path')
    .attr('class', 'link')
    .attr('fill', 'none')
    .attr('stroke', d => delayColor(d.target.data.avg_delay))
    .attr('stroke-opacity', d => hopOpacity[d.target.data.hop] ?? 0.15)
    .attr('stroke-width', d => {
      const frac = d.target.data.propagation_count / maxCount
      return Math.max(0.8, Math.sqrt(frac) * 5)
    })
    .attr('d', linkGen as never)
    .attr('opacity', 0)
    .transition()
    .delay(d => d.target.data.hop * 350)
    .duration(450)
    .attr('opacity', 1)

  // nodes
  const nodeG = g.selectAll<SVGGElement, d3.HierarchyPointNode<PropagationTreeNode>>('.node')
    .data(computed.descendants())
    .join('g')
    .attr('class', 'node')
    .attr('transform', d => {
      const [x, y] = radialPoint(d.x, d.y)
      return `translate(${x},${y})`
    })
    .style('cursor', 'pointer')
    .on('click', (_, d) => cb.onNodeClick(d.data.airport))
    .on('mousemove', (e, d) => cb.onHover({ x: e.clientX, y: e.clientY, node: d.data }))
    .on('mouseout', () => cb.onHover(null))

  // circle
  nodeG.append('circle')
    .attr('r', d => d.data.hop === 0 ? 22 : sizeScale(d.data.propagation_count))
    .attr('fill', d => d.data.hop === 0 ? '#1f6feb' : delayColor(d.data.avg_delay))
    .attr('stroke', d => d.data.hop === 0 ? '#58a6ff' : '#0d1117')
    .attr('stroke-width', d => d.data.hop === 0 ? 2.5 : 1)
    .attr('opacity', 0)
    .transition()
    .delay(d => d.data.hop * 350)
    .duration(400)
    .attr('opacity', 1)

  //airport code label
  nodeG.append('text')
    .attr('dy', '0.32em')
    .attr('x', d => {
      if (d.data.hop === 0) return 0
      const r = sizeScale(d.data.propagation_count) + 4
      return d.x < Math.PI ? r : -r
    })
    .attr('text-anchor', d => {
      if (d.data.hop === 0) return 'middle'
      return d.x < Math.PI ? 'start' : 'end'
    })
    .attr('font-size', d => d.data.hop === 0 ? 13 : d.data.hop === 1 ? 10 : 9)
    .attr('font-weight', d => d.data.hop <= 1 ? '600' : '400')
    .attr('fill', '#c9d1d9')
    .attr('pointer-events', 'none')
    .text(d => d.data.airport)
    .attr('opacity', 0)
    .transition()
    .delay(d => d.data.hop * 350 + 200)
    .duration(300)
    .attr('opacity', 1)

  nodeG.filter(d => d.data.hop === 1)
    .append('text')
    .attr('dy', d => -(sizeScale(d.data.propagation_count) + 6))
    .attr('text-anchor', 'middle')
    .attr('font-size', 8)
    .attr('fill', '#6e7681')
    .attr('pointer-events', 'none')
    .text(d => fmtCount(d.data.propagation_count))
    .attr('opacity', 0)
    .transition()
    .delay(d => d.data.hop * 350 + 350)
    .duration(300)
    .attr('opacity', 1)
}

//subcomponents

interface TooltipState { x: number; y: number; node: PropagationTreeNode }

function Tooltip({ state }: { state: TooltipState }) {
  const { x, y, node } = state
  const flip = x > window.innerWidth * 0.65
  return (
    <div style={{
      position: 'fixed',
      left: flip ? x - 180 : x + 14,
      top: Math.min(y - 10, window.innerHeight - 220),
      background: '#1c2128',
      border: '1px solid #30363d',
      borderRadius: 6,
      padding: '10px 14px',
      fontSize: 12,
      color: '#c9d1d9',
      pointerEvents: 'none',
      zIndex: 1000,
      width: 185,
      boxShadow: '0 6px 20px #0009',
    }}>
      <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 2 }}>{node.airport}</div>
      {(node.city || node.state) && (
        <div style={{ color: '#8b949e', fontSize: 11, marginBottom: 6 }}>
          {node.city}{node.city && node.state ? ', ' : ''}{node.state}
        </div>
      )}
      <div style={{ color: '#484f58', fontSize: 10, marginBottom: 8 }}>
        Hop {node.hop} from root
      </div>
      {node.hop > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: '4px 10px' }}>
          <span style={{ color: '#6e7681' }}>Avg outbound delay</span>
          <span style={{ color: delayColor(node.avg_delay), fontWeight: 600 }}>
            {node.avg_delay.toFixed(1)} min
          </span>
          <span style={{ color: '#6e7681' }}>Propagation events</span>
          <span>{node.propagation_count.toLocaleString()}</span>
          <span style={{ color: '#6e7681' }}>Avg inbound delay</span>
          <span>{node.avg_inbound_delay.toFixed(1)} min</span>
          <span style={{ color: '#6e7681' }}>Avg turnaround</span>
          <span>{node.avg_turnaround.toFixed(0)} min</span>
          <span style={{ color: '#6e7681' }}>Airlines</span>
          <span>{node.airlines_affected}</span>
        </div>
      )}
      <div style={{ marginTop: 8, fontSize: 10, color: '#58a6ff' }}>
        {node.hop === 0 ? 'Hover nodes to explore · scroll to zoom' : 'Click to re-root here'}
      </div>
    </div>
  )
}

function Legend() {
  return (
    <div style={{
      position: 'absolute', bottom: 12, right: 12,
      background: '#161b22cc', border: '1px solid #30363d',
      borderRadius: 6, padding: '8px 12px', fontSize: 11, color: '#8b949e',
      backdropFilter: 'blur(4px)',
    }}>
      <div style={{ fontWeight: 600, marginBottom: 6, color: '#c9d1d9', fontSize: 10, letterSpacing: '0.05em', textTransform: 'uppercase' }}>
        Legend
      </div>
      {([
        ['#2ea043', '≤ 5 min'],
        ['#d29922', '5–15 min'],
        ['#f78166', '15–30 min'],
        ['#da3633', '> 30 min'],
      ] as [string, string][]).map(([c, l]) => (
        <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
          <div style={{ width: 9, height: 9, borderRadius: '50%', background: c, flexShrink: 0 }} />
          <span>{l}</span>
        </div>
      ))}
      <div style={{ borderTop: '1px solid #21262d', marginTop: 8, paddingTop: 7 }}>
        <div style={{ marginBottom: 3 }}>Node size → propagation count</div>
        <div style={{ marginBottom: 3 }}>Edge width → link strength</div>
        <div>Opacity fades with hop depth</div>
      </div>
    </div>
  )
}

function HourlyStrip({ data, airport }: { data: HourlyDelay[]; airport: string }) {
  const ref = useRef<SVGSVGElement>(null)

  useEffect(() => {
    if (!ref.current || !data.length) return
    const W = ref.current.clientWidth || 600
    const H = 52
    const m = { l: 28, r: 8, t: 4, b: 18 }
    const iW = W - m.l - m.r
    const iH = H - m.t - m.b

    const svg = d3.select(ref.current).attr('width', W).attr('height', H)
    svg.selectAll('*').remove()
    const g = svg.append('g').attr('transform', `translate(${m.l},${m.t})`)

    const allHours = Array.from({ length: 24 }, (_, i) => i)
    const rowByHour = new Map(data.map(d => [d.hour, d]))

    const x = d3.scaleBand().domain(allHours.map(String)).range([0, iW]).padding(0.12)
    const maxDelay = d3.max(data, d => d.avg_dep_delay) ?? 1
    const y = d3.scaleLinear().domain([0, maxDelay]).nice().range([iH, 0])

    g.selectAll('rect')
      .data(allHours)
      .join('rect')
      .attr('x', h => x(String(h))!)
      .attr('width', x.bandwidth())
      .attr('y', h => y(Math.max(0, rowByHour.get(h)?.avg_dep_delay ?? 0)))
      .attr('height', h => iH - y(Math.max(0, rowByHour.get(h)?.avg_dep_delay ?? 0)))
      .attr('fill', h => delayColor(rowByHour.get(h)?.avg_dep_delay ?? 0))
      .attr('rx', 1)
      .attr('opacity', 0.85)

    g.append('g')
      .attr('transform', `translate(0,${iH})`)
      .call(d3.axisBottom(x).tickValues(['0', '6', '12', '18', '23']).tickFormat(h => `${h}h`))
      .call(ax => {
        ax.selectAll('text').attr('fill', '#6e7681').attr('font-size', 9)
        ax.selectAll('line,path').attr('stroke', '#21262d')
      })

    svg.append('text')
      .attr('transform', `translate(${m.l - 4},${m.t + iH / 2}) rotate(-90)`)
      .attr('text-anchor', 'middle')
      .attr('fill', '#484f58')
      .attr('font-size', 8)
      .text('min')
  }, [data])

  return (
    <div style={{ borderTop: '1px solid #21262d', background: '#0d1117', padding: '4px 8px 2px' }}>
      <div style={{ fontSize: 10, color: '#484f58', marginBottom: 1 }}>
        {airport} · avg departure delay by hour of day
      </div>
      <svg ref={ref} style={{ width: '100%', height: 52 }} />
    </div>
  )
}

// styles

const labelSt: React.CSSProperties = {
  fontSize: 10, fontWeight: 600, color: '#6e7681',
  textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6,
}
const chipSt: React.CSSProperties = {
  padding: '2px 6px', borderRadius: 4, fontSize: 10, cursor: 'pointer',
  border: '1px solid', fontFamily: 'monospace', letterSpacing: '0.04em',
}

// main comp

export default function PropagationView() {
  const [rootAirport, setRootAirport]   = useState('ATL')
  const [inputText,   setInputText]     = useState('ATL')
  const [hops,        setHops]          = useState(3)
  const [minCount,    setMinCount]      = useState(200)
  const [tooltip,     setTooltip]       = useState<TooltipState | null>(null)

  const svgRef = useRef<SVGSVGElement>(null)

  const { data: treeData, isLoading, isError } = useQuery({
    queryKey: ['propagation-tree', rootAirport, hops, minCount],
    queryFn: () => fetchPropagationTree({ airport: rootAirport, hops, min_count: minCount }),
    retry: 1,
  })

  const { data: hourly } = useQuery({
    queryKey: ['hourly', rootAirport],
    queryFn: () => fetchAirportHourly(rootAirport),
    retry: false,
  })

  const navigate = useCallback((airport: string) => {
    if (airport === rootAirport) return
    setRootAirport(airport)
    setInputText(airport)
    setTooltip(null)
  }, [rootAirport])

  const reset = useCallback((code: string) => {
    setRootAirport(code)
    setInputText(code)
    setTooltip(null)
  }, [])


  useEffect(() => {
    if (!svgRef.current || !treeData || treeData.node_count <= 1) return
    drawTree(svgRef.current, treeData, {
      onNodeClick: airport => navigate(airport),
      onHover: setTooltip,
    })
  }, [treeData, navigate])

  const nonRootNodes  = treeData?.nodes.filter(n => n.hop > 0) ?? []
  const maxDelay      = nonRootNodes.length ? Math.max(...nonRootNodes.map(n => n.avg_delay)) : 0
  const totalProp     = nonRootNodes.reduce((s, n) => s + n.propagation_count, 0)
  const isEmpty       = !isLoading && !isError && (treeData?.node_count ?? 0) <= 1

  return (
    <div style={{ display: 'flex', height: '100%', background: '#0d1117', overflow: 'hidden' }}>

      {/* left panel */}
      <div style={{
        width: 196, flexShrink: 0, background: '#161b22', borderRight: '1px solid #30363d',
        display: 'flex', flexDirection: 'column', padding: 14, gap: 14, overflow: 'auto',
      }}>

        {/* Airport selector */}
        <div>
          <div style={labelSt}>Root Airport</div>
          <div style={{
                flex: 1, background: '#21262d', border: '1px solid #30363d', borderRadius: 4,
                color: '#f0f6fc', padding: '5px 8px', fontSize: 15, fontWeight: 700,
                letterSpacing: 2, fontFamily: 'monospace', textAlign: 'center',
              }}>
            {inputText}
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginTop: 8 }}>
            {POPULAR.map(code => (
              <button
                key={code}
                onClick={() => reset(code)}
                style={{
                  ...chipSt,
                  background: code === rootAirport ? '#1f6feb' : '#21262d',
                  color:      code === rootAirport ? '#fff'    : '#8b949e',
                  borderColor: code === rootAirport ? '#1f6feb' : '#30363d',
                }}
              >
                {code}
              </button>
            ))}
          </div>
        </div>

        {/* Hops slider */}
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
            <span style={labelSt}>Hops</span>
            <span style={{ fontSize: 13, fontWeight: 700, color: '#58a6ff' }}>{hops}</span>
          </div>
          <input type="range" min={1} max={4} value={hops}
            onChange={e => setHops(+e.target.value)}
            style={{ width: '100%', accentColor: '#1f6feb' }} />
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 9, color: '#484f58', marginTop: 2 }}>
            <span>1</span><span>2</span><span>3</span><span>4</span>
          </div>
        </div>

        {/* Min propagations slider */}
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
            <span style={labelSt}>Min events</span>
            <span style={{ fontSize: 11, fontWeight: 700, color: '#58a6ff' }}>{minCount.toLocaleString()}</span>
          </div>
          <input type="range" min={10} max={2000} step={10} value={minCount}
            onChange={e => setMinCount(+e.target.value)}
            style={{ width: '100%', accentColor: '#1f6feb' }} />
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 9, color: '#484f58', marginTop: 2 }}>
            <span>10</span><span>2000</span>
          </div>
          <div style={{ fontSize: 10, color: '#484f58', marginTop: 4 }}>
            Lower = more airports shown
          </div>
        </div>

        {/* Stats */}
        {treeData && treeData.node_count > 1 && (
          <div style={{ marginTop: 'auto', paddingTop: 12, borderTop: '1px solid #21262d', fontSize: 11 }}>
            <div style={{ ...labelSt, marginBottom: 8 }}>Tree Stats</div>
            {([
              ['Airports',      treeData.node_count],
              ['Total events',  fmtCount(totalProp)],
              ['Max delay',     maxDelay > 0 ? `${maxDelay.toFixed(1)} min` : '—'],
            ] as [string, string | number][]).map(([l, v]) => (
              <div key={String(l)} style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 5, color: '#6e7681' }}>
                <span>{l}</span>
                <span style={{ color: '#c9d1d9', fontWeight: 600 }}>{v}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* main area */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', position: 'relative' }}>

        {/* Header */}
        <div style={{
          padding: '9px 16px', borderBottom: '1px solid #21262d',
          display: 'flex', alignItems: 'center', gap: 14, flexShrink: 0,
        }}>
          <div style={{ fontSize: 14, fontWeight: 700, color: '#f0f6fc' }}>
            Delay Propagation — {rootAirport}
          </div>
          {treeData && treeData.node_count > 1 && (
            <div style={{ fontSize: 11, color: '#6e7681' }}>
              {treeData.node_count - 1} downstream airports across {hops} hop{hops !== 1 ? 's' : ''}
            </div>
          )}
          <div style={{ marginLeft: 'auto', fontSize: 11, color: '#484f58' }}>
            Scroll to zoom · drag to pan · click node to re-root
          </div>
        </div>

        {/* SVG area */}
        <div style={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
          {isLoading && (
            <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6e7681', fontSize: 13 }}>
              Building propagation tree…
            </div>
          )}
          {isError && (
            <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 8 }}>
              <div style={{ color: '#f78166', fontSize: 13 }}>No propagation data found for {rootAirport}</div>
              <div style={{ color: '#6e7681', fontSize: 11 }}>Try a major hub: ATL, ORD, DFW, DEN</div>
            </div>
          )}
          {isEmpty && (
            <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 8 }}>
              <div style={{ color: '#8b949e', fontSize: 13 }}>No significant delay propagation detected for {rootAirport}</div>
              <div style={{ color: '#6e7681', fontSize: 11 }}>Lower the "Min events" threshold or try a larger hub</div>
            </div>
          )}
          <svg
            ref={svgRef}
            style={{ width: '100%', height: '100%', display: 'block' }}
          />
          {treeData && treeData.node_count > 1 && <Legend />}
        </div>

        {/* Hourly strip */}
        {hourly && hourly.length > 0 && (
          <HourlyStrip data={hourly} airport={rootAirport} />
        )}
      </div>

      {/* Floating tooltip */}
      {tooltip && <Tooltip state={tooltip} />}
    </div>
  )
}
