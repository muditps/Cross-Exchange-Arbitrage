import { useQuery } from '@tanstack/react-query';
import type { ClosedOpportunity, OpportunityPage, SimulationSummary } from '@/types';

const PAGE_SIZE = 20;
const REFETCH_INTERVAL_MS = 30_000;
const STALE_TIME_MS = 25_000;

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Fetch failed: ${response.status} ${url}`);
  }
  return response.json() as Promise<T>;
}

export interface OpportunityFilters {
  pair: string;
  buyExchange: string;
  sellExchange: string;
  page: number;
}

/**
 * Fetches a paginated list of CLOSED/EXPIRED opportunities from the REST API.
 *
 * All filter params become query string params on the backend endpoint, and are
 * separately synced to the URL by OpportunitiesPage so the view is bookmarkable.
 *
 * Polls every 30s — new opportunities close infrequently; tight polling wastes server
 * resources without adding meaningful freshness to a historical table.
 *
 * @param filters active filter values (empty string = no filter applied)
 */
export function useHistoricalOpportunities(filters: OpportunityFilters) {
  const { pair, buyExchange, sellExchange, page } = filters;

  const params = new URLSearchParams();
  if (pair) params.set('pair', pair);
  if (buyExchange) params.set('buyExchange', buyExchange);
  if (sellExchange) params.set('sellExchange', sellExchange);
  params.set('page', String(page));
  params.set('size', String(PAGE_SIZE));

  return useQuery<OpportunityPage>({
    queryKey: ['opportunities', 'historical', pair, buyExchange, sellExchange, page],
    queryFn: () => fetchJson<OpportunityPage>(`/api/opportunities?${params.toString()}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}

/**
 * Fetches the full detail of a single opportunity by ID for the detail modal.
 * Only enabled when `id` is non-null (modal is open).
 */
export function useOpportunityDetail(id: string | null) {
  return useQuery<ClosedOpportunity>({
    queryKey: ['opportunities', 'detail', id],
    queryFn: () => fetchJson<ClosedOpportunity>(`/api/opportunities/${id}`),
    enabled: id !== null,
    staleTime: 60_000,
  });
}

/**
 * Fetches the simulation result for one opportunity. 204 responses resolve to null.
 * Only enabled when `id` is non-null (modal is open).
 */
export function useOpportunitySimulation(id: string | null) {
  return useQuery<SimulationSummary | null>({
    queryKey: ['opportunities', 'simulation', id],
    queryFn: async () => {
      const res = await fetch(`/api/opportunities/${id}/simulation`);
      if (res.status === 204 || res.status === 404) return null;
      if (!res.ok) throw new Error(`Simulation fetch failed: ${res.status}`);
      return res.json() as Promise<SimulationSummary>;
    },
    enabled: id !== null,
    staleTime: 60_000,
  });
}
