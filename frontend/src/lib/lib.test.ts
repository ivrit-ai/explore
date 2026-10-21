import { describe, expect, it } from 'vitest';
import { buildPieces, wordAround } from './highlight';
import { patternPieces } from './regexPattern';
import { groupCombos, wordForms } from './insights';
import { readSearchState, toSearchParams } from './searchState';
import { formatDate, formatTime } from './format';
import type { Episode } from '../api/types';

describe('buildPieces', () => {
  it('highlights the exact match', () => {
    const pieces = buildPieces('והקשר בינה לבין', { start: 6, end: 10 }, [], 'exact');
    expect(pieces.map((p) => [p.text, p.kind])).toEqual([
      ['והקשר ', 'plain'],
      ['בינה', 'match'],
      [' לבין', 'plain'],
    ]);
  });

  it('marks the containing word in partial mode', () => {
    const pieces = buildPieces('מלמדים עברית בערב', { start: 8, end: 12 }, [], 'partial');
    const word = pieces.filter((p) => p.inWord).map((p) => p.text).join('');
    expect(word).toBe('עברית');
    expect(pieces.find((p) => p.kind === 'match')?.text).toBe('ברית');
  });

  it('colours regex groups and marks the end of each group', () => {
    const pieces = buildPieces('שלום חברים וברוכים', { start: 0, end: 10 }, [{ start: 0, end: 4 }, { start: 5, end: 10 }], 'regex');
    const groups = pieces.filter((p) => p.kind === 'group');
    expect(groups.map((p) => [p.text, p.group, p.groupEnd])).toEqual([
      ['שלום', 1, true],
      ['חברים', 2, true],
    ]);
  });

  it('indexes by code point, not UTF-16 unit', () => {
    const pieces = buildPieces('😀 בינה', { start: 2, end: 6 }, [], 'exact');
    expect(pieces.find((p) => p.kind === 'match')?.text).toBe('בינה');
  });

  it('highlights the whole match when a regex has no groups', () => {
    const pieces = buildPieces('מספר 12', { start: 0, end: 7 }, [], 'regex');
    expect(pieces).toHaveLength(1);
    expect(pieces[0].kind).toBe('match');
  });
});

describe('wordAround', () => {
  it('expands to word boundaries', () => {
    expect(wordAround('ארצות הברית נכנסה', { start: 7, end: 11 })).toEqual({ before: 'ה', core: 'ברית', after: '' });
  });
});

describe('patternPieces', () => {
  it('numbers capture groups and skips non-capturing ones', () => {
    const { pieces, groupCount } = patternPieces('(שלום|היי)\\s+(?:ו)?(עולם)');
    expect(groupCount).toBe(2);
    expect(pieces.filter((p) => p.group > 0).map((p) => [p.text, p.group])).toEqual([
      ['שלום|היי', 1],
      ['עולם', 2],
    ]);
  });

  it('treats escaped parentheses and classes as literals', () => {
    expect(patternPieces('\\(א\\)[()]').groupCount).toBe(0);
  });

  it('counts named groups but not lookbehinds', () => {
    expect(patternPieces('(?P<x>א)(?<=ב)(?<y>ג)').groupCount).toBe(2);
  });
});

const episodes: Episode[] = [
  {
    episode_idx: 0,
    uuid: 'u',
    source: 's',
    title: 't',
    date: null,
    duration_sec: 60,
    hits: [
      { char_offset: 0, segment_idx: 0, start_sec: 0, end_sec: 1, text: 'שלום חברים', match: { start: 0, end: 10 }, groups: [{ start: 0, end: 4 }, { start: 5, end: 10 }] },
      { char_offset: 0, segment_idx: 1, start_sec: 0, end_sec: 1, text: 'שלום חברים', match: { start: 0, end: 10 }, groups: [{ start: 0, end: 4 }, { start: 5, end: 10 }] },
      { char_offset: 0, segment_idx: 2, start_sec: 0, end_sec: 1, text: 'היי חברים', match: { start: 0, end: 9 }, groups: [{ start: 0, end: 3 }, { start: 4, end: 9 }] },
    ],
  },
];

describe('insights', () => {
  it('counts regex group combinations', () => {
    expect(groupCombos(episodes).map((c) => [c.parts.join(' '), c.count])).toEqual([
      ['שלום חברים', 2],
      ['היי חברים', 1],
    ]);
  });

  it('counts word forms', () => {
    const forms = wordForms(episodes);
    expect(forms[0].key).toBe('שלום חברים');
    expect(forms[0].count).toBe(2);
  });
});

describe('searchState', () => {
  it('round-trips through the URL', () => {
    const params = new URLSearchParams('q=בינה&mode=regex&page=3&shuffle=0&source=929&source=אחד ביום&date_from=2020-01-01');
    const state = readSearchState(params);
    expect(state).toMatchObject({ q: 'בינה', mode: 'regex', page: 3, shuffle: false, sources: ['929', 'אחד ביום'] });
    expect(readSearchState(toSearchParams(state))).toEqual(state);
  });

  it('falls back to safe defaults', () => {
    expect(readSearchState(new URLSearchParams('mode=evil&page=-4'))).toMatchObject({ mode: 'exact', page: 1, shuffle: true });
  });
});

describe('format', () => {
  it('formats times and dates', () => {
    expect(formatTime(83.4)).toBe('01:23');
    expect(formatTime(3725)).toBe('1:02:05');
    expect(formatDate('2020-06-08')).toBe('08.06.2020');
  });
});
