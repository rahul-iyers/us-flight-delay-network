/**
 * Thin API client — all requests go through the Vite proxy at /api → localhost:8000
 */

import type {
  AirportNode, AirportStats, HourlyDelay,
  RouteEdge, GraphPayload,
  PropagationSummary, AirportPropagation, PropagationHub,
  AirlineStat, MonthlyTrend,
} from './types'

const BASE = '/api'

async function get<T>(path: string, params?: Record<string, string | number | boolean>): Promise<T> {
  const url = new URL(BASE + path, window.location.origin)
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) url.searchParams.set(k, String(v))
    })
  }
  const res = await fetch(url.toString())
  if (!res.ok) throw new Error(`API error ${res.status}: ${await res.text()}`)
  return res.json() as Promise<T>
}

// ─── Airports ────────────────────────────────────────────────────────────────

export const fetchAirports = (params?: { state?: string; min_flights?: number; limit?: number }) =>
  get<AirportNode[]>('/airports/', params as Record<string, number>)

export const fetchAirport = (code: string) =>
  get<AirportStats>(`/airports/${code}`)

export const fetchAirportHourly = (code: string) =>
  get<HourlyDelay[]>(`/airports/${code}/hourly`)

export const fetchAllHourly = (hourStart: number, hourEnd: number) =>
  get<{ airport_code: string; avg_dep_delay: number; flight_count: number }[]>(
    '/airports/hourly/all',
    { hour_start: hourStart, hour_end: hourEnd }
  )

// ─── Network / Graph ─────────────────────────────────────────────────────────

export const fetchGraph = (params?: { top_edges?: number; min_flights?: number }) =>
  get<GraphPayload>('/network/graph', params as Record<string, number>)

export const fetchCommunities = () =>
  get<Record<string, string[]>>('/network/communities')

// ─── Propagation ─────────────────────────────────────────────────────────────

export const fetchPropagationSummary = (params?: {
  hub?: string; dest?: string; min_count?: number; limit?: number
}) => get<PropagationSummary[]>('/propagation/summary', params as Record<string, string | number>)

export const fetchAirportPropagation = (code: string) =>
  get<AirportPropagation>(`/propagation/airport/${code}`)

export const fetchTopHubs = (limit = 20) =>
  get<PropagationHub[]>('/propagation/top-hubs', { limit })

// ─── Airlines ────────────────────────────────────────────────────────────────

export const fetchAirlines = (limit = 50) =>
  get<AirlineStat[]>('/airlines/', { limit })

export const fetchMonthlyTrends = () =>
  get<MonthlyTrend[]>('/airlines/monthly')
