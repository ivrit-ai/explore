export type SearchMode = 'exact' | 'partial' | 'regex';

/** [start, end) in code points, relative to the hit's segment text. */
export interface Span {
  start: number;
  end: number;
}

export interface Hit {
  char_offset: number;
  segment_idx: number;
  start_sec: number;
  end_sec: number;
  text: string;
  match: Span;
  groups: (Span | null)[];
}

export interface Episode {
  episode_idx: number;
  uuid: string;
  source: string;
  title: string;
  date: string | null;
  duration_sec: number;
  hits: Hit[];
}

export interface SearchResponse {
  query: string;
  mode: SearchMode;
  page: number;
  per_page: number;
  seed: number | null;
  has_more: boolean;
  total_hits: number;
  episodes: Episode[];
}

export interface SourceCount {
  name: string;
  count: number;
}

export interface MetadataResponse {
  sources: SourceCount[];
  date_range: { min: string | null; max: string | null };
  total_docs: number;
}

export interface Segment {
  segment_idx: number;
  start_sec: number;
  end_sec: number;
  text: string;
}

export interface Me {
  email: string;
}
