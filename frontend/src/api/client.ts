import type { SearchState } from '../lib/searchState';
import type { Me, MetadataResponse, SearchResponse, Segment } from './types';

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function getJSON<T>(path: string, params?: URLSearchParams, signal?: AbortSignal): Promise<T> {
  const url = params ? `${path}?${params}` : path;
  const res = await fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' }, signal });
  if (res.status === 401) {
    window.location.assign('/login');
    throw new ApiError(401, 'נדרשת התחברות');
  }
  if (!res.ok) {
    let message = 'משהו השתבש. נסו שוב.';
    try {
      const body = await res.json();
      if (typeof body.detail === 'string') message = body.detail;
    } catch {
      // Non-JSON error body; keep the generic message.
    }
    throw new ApiError(res.status, message);
  }
  return res.json() as Promise<T>;
}

/** Query params shared by search, metadata and CSV export. */
export function filterParams(state: SearchState): URLSearchParams {
  const p = new URLSearchParams({ q: state.q, mode: state.mode });
  if (state.dateFrom) p.set('date_from', state.dateFrom);
  if (state.dateTo) p.set('date_to', state.dateTo);
  for (const s of state.sources) p.append('source', s);
  return p;
}

export function search(state: SearchState, signal?: AbortSignal) {
  const p = filterParams(state);
  p.set('page', String(state.page));
  p.set('per_page', String(state.perPage));
  p.set('shuffle', String(state.shuffle));
  if (state.seed !== null) p.set('seed', String(state.seed));
  return getJSON<SearchResponse>('/api/search', p, signal);
}

export function searchMetadata(state: SearchState, signal?: AbortSignal) {
  // Source counts ignore the source filter itself so every source stays listed.
  return getJSON<MetadataResponse>('/api/search/metadata', filterParams({ ...state, sources: [] }), signal);
}

export function episodeSegments(episodeIdx: number, center: number, radius = 5, signal?: AbortSignal) {
  const p = new URLSearchParams({ center: String(center), radius: String(radius) });
  return getJSON<Segment[]>(`/api/episodes/${episodeIdx}/segments`, p, signal);
}

export function me(signal?: AbortSignal) {
  return getJSON<Me>('/api/me', undefined, signal);
}

export function csvExportUrl(state: SearchState) {
  return `/api/export/csv?${filterParams(state)}`;
}

export function audioExportUrl(uuid: string, start: number, end: number) {
  const p = new URLSearchParams({ start: start.toFixed(2), end: end.toFixed(2) });
  return `/api/export/audio/${encodeURIComponent(uuid)}?${p}`;
}

export function audioUrl(uuid: string) {
  return `/audio/${encodeURIComponent(uuid)}.opus`;
}
