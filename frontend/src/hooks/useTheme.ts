import { useEffect, useState } from 'react';
import { applyTheme, readChoice, resolveTheme, saveChoice, themeStorage, type ThemeChoice } from '../lib/theme';
import { useMediaQuery } from './useMediaQuery';

/** The saved theme choice; "system" follows the OS setting live. */
export function useTheme() {
  const [choice, setChoice] = useState<ThemeChoice>(() => readChoice(themeStorage()));
  const prefersLight = useMediaQuery('(prefers-color-scheme: light)');

  useEffect(() => {
    applyTheme(resolveTheme(choice, prefersLight));
  }, [choice, prefersLight]);

  const choose = (next: ThemeChoice) => {
    saveChoice(themeStorage(), next);
    setChoice(next);
  };

  return { choice, choose };
}
