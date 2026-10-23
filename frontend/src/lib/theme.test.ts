import { describe, expect, it } from 'vitest';
import { THEME_KEY, applyTheme, readChoice, resolveTheme, saveChoice } from './theme';

function memoryStorage(initial: Record<string, string> = {}) {
  const data = new Map(Object.entries(initial));
  return {
    data,
    getItem: (k: string) => data.get(k) ?? null,
    setItem: (k: string, v: string) => void data.set(k, v),
    removeItem: (k: string) => void data.delete(k),
  };
}

const throwing = {
  setItem: () => {
    throw new Error('blocked');
  },
  removeItem: () => {
    throw new Error('blocked');
  },
};

describe('theme choice', () => {
  it('reads a saved choice and defaults to system', () => {
    expect(readChoice(memoryStorage({ [THEME_KEY]: 'light' }))).toBe('light');
    expect(readChoice(memoryStorage({ [THEME_KEY]: 'dark' }))).toBe('dark');
    expect(readChoice(memoryStorage({ [THEME_KEY]: 'purple' }))).toBe('system');
    expect(readChoice(memoryStorage())).toBe('system');
    expect(readChoice(null)).toBe('system');
  });

  it('stores light/dark and clears the key for system', () => {
    const storage = memoryStorage();
    saveChoice(storage, 'dark');
    expect(storage.data.get(THEME_KEY)).toBe('dark');
    saveChoice(storage, 'system');
    expect(storage.data.has(THEME_KEY)).toBe(false);
  });

  it('survives storage that refuses writes', () => {
    expect(() => saveChoice(throwing, 'light')).not.toThrow();
  });

  it('resolves system from the OS preference', () => {
    expect(resolveTheme('system', true)).toBe('light');
    expect(resolveTheme('system', false)).toBe('dark');
    expect(resolveTheme('dark', true)).toBe('dark');
    expect(resolveTheme('light', false)).toBe('light');
  });

  it('sets data-theme and the browser chrome colour', () => {
    const meta = { content: '', setAttribute(_: string, v: string) { this.content = v; } };
    const doc = { documentElement: { dataset: {} as Record<string, string> }, querySelector: () => meta };
    applyTheme('light', doc as unknown as Document);
    expect(doc.documentElement.dataset.theme).toBe('light');
    expect(meta.content).toBe('#faf8f4');
  });
});

// Every colour must come from tokens.css, or one theme silently misses it.
const sources = {
  ...import.meta.glob('../**/*.css', { query: '?raw', import: 'default', eager: true }),
  ...import.meta.glob('../**/*.tsx', { query: '?raw', import: 'default', eager: true }),
} as Record<string, string>;
const COLOUR_LITERAL = /#[0-9a-fA-F]{3,8}\b|\b(?:rgba?|hsla?)\(/;

describe('colour tokens', () => {
  it('finds the stylesheets', () => {
    expect(Object.keys(sources).length).toBeGreaterThan(20);
  });

  it.each(Object.entries(sources).filter(([path]) => !path.endsWith('styles/tokens.css')))(
    '%s uses tokens only',
    (_path, text) => {
      expect(text.match(COLOUR_LITERAL)?.[0] ?? null).toBeNull();
    },
  );
});
