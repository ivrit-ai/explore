import type { SearchMode, Span } from '../api/types';

export type PieceKind = 'plain' | 'match' | 'group';

export interface Piece {
  text: string;
  kind: PieceKind;
  /** Regex capture group number (1-based) when kind === 'group'. */
  group: number;
  /** Partial mode: inside the whole word that contains the match. */
  inWord: boolean;
  /** Last piece of a group run, where the group number is shown. */
  groupEnd: boolean;
}

const WORD_CHAR = /[\p{L}\p{N}\p{M}]/u;

/**
 * Split segment text into styled runs. Spans are code-point offsets from the API,
 * so the text is indexed by code point (Array.from), not UTF-16 unit.
 */
export function buildPieces(text: string, match: Span, groups: (Span | null)[], mode: SearchMode): Piece[] {
  const chars = Array.from(text);
  const n = chars.length;
  const clamp = (v: number) => Math.min(Math.max(v, 0), n);
  const kind: PieceKind[] = new Array(n).fill('plain');
  const group: number[] = new Array(n).fill(0);
  const inWord: boolean[] = new Array(n).fill(false);

  const ms = clamp(match.start);
  const me = clamp(match.end);
  const useGroups = mode === 'regex' && groups.some((g) => g && g.end > g.start);

  if (useGroups) {
    groups.forEach((g, gi) => {
      if (!g) return;
      for (let i = clamp(g.start); i < clamp(g.end); i++) {
        // Outer groups are listed first; inner ones refine the colour.
        kind[i] = 'group';
        group[i] = gi + 1;
      }
    });
  } else {
    for (let i = ms; i < me; i++) kind[i] = 'match';
  }

  if (mode === 'partial' && me > ms) {
    let s = ms;
    while (s > 0 && WORD_CHAR.test(chars[s - 1])) s--;
    let e = me;
    while (e < n && WORD_CHAR.test(chars[e])) e++;
    for (let i = s; i < e; i++) inWord[i] = true;
  }

  const pieces: Piece[] = [];
  for (let i = 0; i < n; i++) {
    const last = pieces[pieces.length - 1];
    if (last && last.kind === kind[i] && last.group === group[i] && last.inWord === inWord[i]) {
      last.text += chars[i];
    } else {
      pieces.push({ text: chars[i], kind: kind[i], group: group[i], inWord: inWord[i], groupEnd: false });
    }
  }
  pieces.forEach((p, i) => {
    const next = pieces[i + 1];
    p.groupEnd = p.kind === 'group' && !(next && next.kind === 'group' && next.group === p.group);
  });
  return pieces;
}

/** The whole word around a match, split into the parts before/inside/after it. */
export function wordAround(text: string, match: Span): { before: string; core: string; after: string } {
  const chars = Array.from(text);
  let s = match.start;
  while (s > 0 && WORD_CHAR.test(chars[s - 1])) s--;
  let e = match.end;
  while (e < chars.length && WORD_CHAR.test(chars[e])) e++;
  return {
    before: chars.slice(s, match.start).join(''),
    core: chars.slice(match.start, match.end).join(''),
    after: chars.slice(match.end, e).join(''),
  };
}

export function spanText(text: string, span: Span | null): string {
  if (!span) return '';
  return Array.from(text).slice(span.start, span.end).join('');
}

/** Groups cycle through four tints; the number is always shown alongside. */
export function groupTint(group: number): 1 | 2 | 3 | 4 {
  return (((group - 1) % 4) + 1) as 1 | 2 | 3 | 4;
}
