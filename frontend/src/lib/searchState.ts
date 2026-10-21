import type { SearchMode } from '../api/types';

/** Everything that defines a results page; mirrored 1:1 in the /search URL. */
export interface SearchState {
  q: string;
  mode: SearchMode;
  page: number;
  perPage: number;
  shuffle: boolean;
  seed: number | null;
  dateFrom: string | null;
  dateTo: string | null;
  sources: string[];
}

export const DEFAULT_PER_PAGE = 50;
export const PER_PAGE_OPTIONS = [20, 50, 100, 200];
const MODES: SearchMode[] = ['exact', 'partial', 'regex'];

export function readSearchState(params: URLSearchParams): SearchState {
  const mode = params.get('mode') as SearchMode;
  const int = (key: string, fallback: number) => {
    const n = Number.parseInt(params.get(key) ?? '', 10);
    return Number.isFinite(n) && n > 0 ? n : fallback;
  };
  const seed = Number.parseInt(params.get('seed') ?? '', 10);
  return {
    q: params.get('q') ?? '',
    mode: MODES.includes(mode) ? mode : 'exact',
    page: int('page', 1),
    perPage: int('per_page', DEFAULT_PER_PAGE),
    shuffle: params.get('shuffle') !== '0',
    seed: Number.isFinite(seed) ? seed : null,
    dateFrom: params.get('date_from'),
    dateTo: params.get('date_to'),
    sources: params.getAll('source'),
  };
}

/** URL params for the /search page (and the same shape the API accepts). */
export function toSearchParams(state: SearchState): URLSearchParams {
  const p = new URLSearchParams();
  p.set('q', state.q);
  p.set('mode', state.mode);
  if (state.page > 1) p.set('page', String(state.page));
  if (state.perPage !== DEFAULT_PER_PAGE) p.set('per_page', String(state.perPage));
  if (!state.shuffle) p.set('shuffle', '0');
  if (state.shuffle && state.seed !== null) p.set('seed', String(state.seed));
  if (state.dateFrom) p.set('date_from', state.dateFrom);
  if (state.dateTo) p.set('date_to', state.dateTo);
  for (const s of state.sources) p.append('source', s);
  return p;
}

export function newSearch(q: string, mode: SearchMode, base?: Partial<SearchState>): SearchState {
  return {
    q,
    mode,
    page: 1,
    perPage: base?.perPage ?? DEFAULT_PER_PAGE,
    shuffle: base?.shuffle ?? true,
    seed: null,
    dateFrom: base?.dateFrom ?? null,
    dateTo: base?.dateTo ?? null,
    sources: base?.sources ?? [],
  };
}
