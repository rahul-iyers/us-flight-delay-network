import React, { useState } from 'react'
import type { ViewName, AppState } from './types'
import Sidebar from './components/Sidebar'
import NetworkMap from './views/NetworkMap'
import AirportDetail from './views/AirportDetail'
import AirlineComparison from './views/AirlineComparison'
import PropagationView from './views/PropagationView'

const STYLES: Record<string, React.CSSProperties> = {
  shell: {
    display: 'flex',
    width: '100%',
    height: '100%',
    overflow: 'hidden',
  },
  main: {
    flex: 1,
    position: 'relative',
    overflow: 'hidden',
  },
}

export default function App() {
  const [state, setState] = useState<AppState>({
    activeView: 'map',
    selectedAirport: null,
    filterAirline: null,
    filterHour: [0, 23],
    showPropagation: false,
  })

  const setView = (v: ViewName) => setState(s => ({ ...s, activeView: v }))
  const selectAirport = (code: string | null) =>
    setState(s => ({ ...s, selectedAirport: code, activeView: code ? 'airport' : s.activeView }))

  return (
    <div style={STYLES.shell}>
      <Sidebar
        activeView={state.activeView}
        onViewChange={setView}
        selectedAirport={state.selectedAirport}
        onClearAirport={() => selectAirport(null)}
        showPropagation={state.showPropagation}
        onTogglePropagation={() => setState(s => ({ ...s, showPropagation: !s.showPropagation }))}
        filterHour={state.filterHour}
        onHourChange={(h) => setState(s => ({ ...s, filterHour: h }))}
      />
      <main style={STYLES.main}>
        {state.activeView === 'map' && (
          <NetworkMap
            onSelectAirport={selectAirport}
            showPropagation={state.showPropagation}
            filterHour={state.filterHour}
          />
        )}
        {state.activeView === 'airport' && state.selectedAirport && (
          <AirportDetail
            code={state.selectedAirport}
            onBack={() => setView('map')}
          />
        )}
{state.activeView === 'airlines' && (
          <AirlineComparison />
        )}
        {state.activeView === 'propagation' && (
          <PropagationView />
        )}
        {state.activeView === 'airport' && !state.selectedAirport && (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#8b949e' }}>
            Click an airport on the map to see its detail view.
          </div>
        )}
      </main>
    </div>
  )
}
