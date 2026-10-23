import type { Episode, Hit } from '../api/types';

/** Hits fetched per "show more" click. */
export const HITS_PAGE = 50;

/** Above this many rows on screen, most episodes start collapsed. */
export const RENDER_BUDGET = 300;

/** How many episodes stay open when the page is over budget. */
export const OPEN_WHEN_OVER_BUDGET = 3;

/**
 * Episodes to start collapsed. Rendering every hit of a broad query (e.g. a
 * one-letter partial search) blocked the main thread, so a large page opens
 * only its first few episodes.
 */
export function initialClosed(episodes: Episode[]): Set<number> {
  const rows = episodes.reduce((n, e) => n + e.hits.length, 0);
  if (rows <= RENDER_BUDGET) return new Set();
  return new Set(episodes.slice(OPEN_WHEN_OVER_BUDGET).map((e) => e.episode_idx));
}

/** Append hits loaded later to each episode, skipping any already present. */
export function withExtraHits(episodes: Episode[], extra: Map<number, Hit[]>): Episode[] {
  if (!extra.size) return episodes;
  return episodes.map((e) => {
    const more = extra.get(e.episode_idx);
    if (!more?.length) return e;
    const seen = new Set(e.hits.map((h) => h.char_offset));
    return { ...e, hits: [...e.hits, ...more.filter((h) => !seen.has(h.char_offset))] };
  });
}

/** Hits of this episode the server has but the page has not loaded yet. */
export function remainingHits(episode: Episode): number {
  return Math.max(0, episode.total_hits - episode.hits.length);
}
