import React from 'react'
import type { ViewName } from '../types'

interface Props {
  activeView: ViewName
  onViewChange: (v: ViewName) => void
  selectedAirport: string | null
  onClearAirport: () => void
  showPropagation: boolean
  onTogglePropagation: () => void
  filterHour: [number, number]
  onHourChange: (h: [number, number]) => void
}

const NAV: { id: ViewName; label: string; }[] = [
  { id: 'map', label: 'Network Map' },
  { id: 'airport', label: 'Airport Detail' },
  { id: 'propagation', label: 'Delay Propagation' },
  { id: 'airlines', label: 'Airline Comparison' },
]

const s = {
  sidebar: {
    width: 220,
    minWidth: 220,
    height: '100%',
    background: '#161b22',
    borderRight: '1px solid #30363d',
    display: 'flex',
    flexDirection: 'column' as const,
    padding: '16px 0',
    zIndex: 10,
  },
  title: {
    padding: '0 16px 16px',
    borderBottom: '1px solid #30363d',
    fontSize: 13,
    fontWeight: 700,
    color: '#58a6ff',
    letterSpacing: '0.04em',
    textTransform: 'uppercase' as const,
  },
  nav: { padding: '12px 0' },
  navItem: (active: boolean): React.CSSProperties => ({
    display: 'flex',
    alignItems: 'center',
    gap: 10,
    padding: '9px 16px',
    cursor: 'pointer',
    fontSize: 13,
    color: active ? '#f0f6fc' : '#8b949e',
    background: active ? '#21262d' : 'transparent',
    borderLeft: `3px solid ${active ? '#58a6ff' : 'transparent'}`,
    transition: 'all 0.15s',
    userSelect: 'none',
  }),
  section: {
    padding: '12px 16px',
    borderTop: '1px solid #30363d',
    fontSize: 12,
    color: '#8b949e',
  },
  sectionTitle: {
    fontSize: 11,
    fontWeight: 600,
    textTransform: 'uppercase' as const,
    letterSpacing: '0.06em',
    color: '#6e7681',
    marginBottom: 10,
  },
  toggle: (on: boolean): React.CSSProperties => ({
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '6px 0',
    cursor: 'pointer',
    color: on ? '#58a6ff' : '#8b949e',
    userSelect: 'none',
  }),
  pill: (on: boolean): React.CSSProperties => ({
    width: 32,
    height: 16,
    borderRadius: 8,
    background: on ? '#1f6feb' : '#30363d',
    position: 'relative',
    transition: 'background 0.2s',
  }),
  pillDot: (on: boolean): React.CSSProperties => ({
    position: 'absolute',
    top: 2,
    left: on ? 16 : 2,
    width: 12,
    height: 12,
    borderRadius: '50%',
    background: '#fff',
    transition: 'left 0.2s',
  }),
  rangeLabel: {
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: 6,
    color: '#8b949e',
  },
  airportBadge: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 6,
    background: '#1f6feb22',
    border: '1px solid #1f6feb',
    borderRadius: 4,
    padding: '4px 8px',
    color: '#58a6ff',
    fontSize: 12,
    marginTop: 8,
    cursor: 'pointer',
  },
  footer: {
    marginTop: 'auto',
    padding: '12px 16px',
    fontSize: 11,
    color: '#484f58',
    borderTop: '1px solid #30363d',
  },
}

export default function Sidebar({
  activeView, onViewChange, selectedAirport, onClearAirport,
  showPropagation, onTogglePropagation, filterHour, onHourChange,
}: Props) {
  return (
    <nav style={s.sidebar}>
      <div style={s.title}>Flight Delay<br />Network</div>

      <div style={s.nav}>
        {NAV.map(({ id, label }) => (
          <div
            key={id}
            style={s.navItem(activeView === id)}
            onClick={() => onViewChange(id)}
          >
            <span>{label}</span>
          </div>
        ))}
      </div>

      {/* selected airport badge */}
      {selectedAirport && (
        <div style={s.section}>
          <div style={s.sectionTitle}>Selected Airport</div>
          <div style={s.airportBadge} onClick={onClearAirport} title="Click to clear">
            {selectedAirport}
          </div>
        </div>
      )}

      {/* overlays section */}
      <div style={s.section}>
        <div style={s.sectionTitle}>Overlays</div>
        <div style={s.toggle(showPropagation)} onClick={onTogglePropagation}>
          <span>Show Propagation</span>
          <div style={s.pill(showPropagation)}>
            <div style={s.pillDot(showPropagation)} />
          </div>
        </div>
      </div>

      {/* hour filter */}
      <div style={s.section}>
        <div style={s.sectionTitle}>Departure Hour</div>
        <div style={s.rangeLabel}>
          <span>{filterHour[0]}:00</span>
          <span>{filterHour[1]}:59</span>
        </div>
        <input
          type="range"
          min={0} max={23}
          value={filterHour[0]}
          style={{ width: '100%', marginBottom: 4 }}
          onChange={e => onHourChange([+e.target.value, filterHour[1]])}
        />
        <input
          type="range"
          min={0} max={23}
          value={filterHour[1]}
          style={{ width: '100%' }}
          onChange={e => onHourChange([filterHour[0], +e.target.value])}
        />
      </div>

      {/* legend */}
      <div style={s.section}>
        <div style={s.sectionTitle}>Delay Legend</div>
        {[
          ['#2ea043', '< 5 min'],
          ['#d29922', '5-15 min'],
          ['#f78166', '15-30 min'],
          ['#da3633', '> 30 min'],
        ].map(([color, label]) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <div style={{ width: 10, height: 10, borderRadius: '50%', background: color }} />
            <span style={{ fontSize: 11 }}>{label}</span>
          </div>
        ))}
      </div>

      <div style={s.footer}>CSE 6242 - Team 21</div>
    </nav>
  )
}
