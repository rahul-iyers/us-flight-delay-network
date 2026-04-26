
// here, we define interfaces for our ts so we didn't run into any type errors during development

export interface AirportNode {
  id: string            
  airport_code: string
  lat: number | null
  lon: number | null
  city: string
  state: string
  full_name: string
  total_flights: number
  avg_dep_delay: number
  cancellation_rate: number
  on_time_rate: number
  degree_centrality: number
  betweenness_centrality: number
  pagerank: number
  community_id: number
  num_airlines: number
  num_destinations: number
}

export interface AirportStats extends AirportNode {
  median_dep_delay: number
  dep_delay_rate: number
  avg_carrier_delay: number | null
  avg_weather_delay: number | null
  avg_nas_delay: number | null
  avg_late_aircraft_delay: number | null
}

export interface HourlyDelay {
  airport_code: string
  hour: number
  flight_count: number
  avg_dep_delay: number
  p25_dep_delay: number
  p75_dep_delay: number
  dep_delay_rate: number
}

// route / edge
export interface RouteEdge {
  source: string 
  target: string 
  origin: string
  dest: string
  total_flights: number
  avg_dep_delay: number
  cancellation_rate: number
  dep_delay_rate: number
  avg_distance: number
  delay_severity: number 
}

// network
export interface GraphPayload {
  nodes: AirportNode[]
  edges: RouteEdge[]
  communities: Record<string, number>
}

// propagation
export interface PropagationSummary {
  hub_airport: string
  outbound_dest: string
  method: string
  propagation_count: number
  avg_inbound_delay: number
  avg_outbound_delay: number
  avg_turnaround_minutes: number
  airlines_affected: number
}

export interface AirportPropagation {
  airport: string
  as_hub: PropagationSummary[]
  as_destination: PropagationSummary[]
}

export interface PropagationHub {
  hub_airport: string
  total_propagations: number
  unique_destinations: number
  avg_outbound_delay: number
}

// airline
export interface AirlineStat {
  airline_code: string
  airline_name: string
  total_flights: number
  avg_dep_delay: number
  median_dep_delay: number
  cancellation_rate: number
  dep_delay_rate: number
  on_time_rate: number
  airports_served: number
  routes_served: number
}

// propagation tree
export interface PropagationTreeNode {
  id: string
  parent: string | null
  airport: string
  hop: number
  avg_delay: number
  propagation_count: number
  avg_inbound_delay: number
  avg_turnaround: number
  airlines_affected: number
  city: string
  state: string
  lat: number | null
  lon: number | null
}

export interface PropagationTreeData {
  root: string
  node_count: number
  nodes: PropagationTreeNode[]
}

// ui state
export type ViewName = 'map' | 'airport' | 'airlines' | 'propagation'
export interface AppState {
  activeView: ViewName
  selectedAirport: string | null
  filterAirline: string | null
  filterHour: [number, number]   // [0, 23]
  showPropagation: boolean
}
