export type ThemeChoice = 'system' | 'light' | 'dark';
export type Theme = 'light' | 'dark';

/** Also read by public/static/theme-init.js before the app loads. */
export const THEME_KEY = 'explore.theme';

/** Browser chrome colour per theme; matches --bg in tokens.css. */
export const THEME_COLOR: Record<Theme, string> = { dark: '#121110', light: '#faf8f4' };

/** localStorage, or null where the browser blocks it (private modes, disabled storage). */
export function themeStorage(): Storage | null {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

export function readChoice(storage: Pick<Storage, 'getItem'> | null): ThemeChoice {
  const value = storage?.getItem(THEME_KEY);
  return value === 'light' || value === 'dark' ? value : 'system';
}

/** "system" is stored as no value, so the page keeps following the OS. */
export function saveChoice(storage: Pick<Storage, 'setItem' | 'removeItem'> | null, choice: ThemeChoice): void {
  try {
    if (choice === 'system') storage?.removeItem(THEME_KEY);
    else storage?.setItem(THEME_KEY, choice);
  } catch {
    // Storage full or blocked: the choice still applies for this visit.
  }
}

export function resolveTheme(choice: ThemeChoice, prefersLight: boolean): Theme {
  if (choice === 'system') return prefersLight ? 'light' : 'dark';
  return choice;
}

export function applyTheme(theme: Theme, doc: Document = document): void {
  doc.documentElement.dataset.theme = theme;
  doc.querySelector('meta[name="theme-color"]')?.setAttribute('content', THEME_COLOR[theme]);
}
