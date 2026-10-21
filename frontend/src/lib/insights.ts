import type { Episode, Hit } from '../api/types';
import { spanText, wordAround } from './highlight';

export interface WordForm {
  key: string;
  before: string;
  core: string;
  after: string;
  count: number;
}

/** Partial mode: which whole words the term was found in, on this page. */
export function wordForms(episodes: Episode[]): WordForm[] {
  const forms = new Map<string, WordForm>();
  for (const e of episodes) {
    for (const h of e.hits) {
      const w = wordAround(h.text, h.match);
      const key = w.before + w.core + w.after;
      const existing = forms.get(key);
      if (existing) existing.count++;
      else forms.set(key, { key, ...w, count: 1 });
    }
  }
  return [...forms.values()].sort((a, b) => b.count - a.count);
}

export function hitWord(hit: Hit): string {
  const w = wordAround(hit.text, hit.match);
  return w.before + w.core + w.after;
}

export interface Combo {
  key: string;
  parts: string[];
  count: number;
}

/** Regex mode: distinct combinations of captured groups, on this page. */
export function groupCombos(episodes: Episode[]): Combo[] {
  const combos = new Map<string, Combo>();
  for (const e of episodes) {
    for (const h of e.hits) {
      const parts = h.groups.length ? h.groups.map((g) => spanText(h.text, g)) : [spanText(h.text, h.match)];
      const key = parts.join('\u0000');
      const existing = combos.get(key);
      if (existing) existing.count++;
      else combos.set(key, { key, parts, count: 1 });
    }
  }
  return [...combos.values()].sort((a, b) => b.count - a.count);
}

export function hitComboKey(hit: Hit): string {
  const parts = hit.groups.length ? hit.groups.map((g) => spanText(hit.text, g)) : [spanText(hit.text, hit.match)];
  return parts.join('\u0000');
}
