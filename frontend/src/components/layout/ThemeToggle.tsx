import { useTheme } from '../../hooks/useTheme';
import type { ThemeChoice } from '../../lib/theme';
import { MoonIcon, SunIcon, SystemIcon } from '../icons';
import { IconButton } from '../ui/IconButton';

const NEXT: Record<ThemeChoice, ThemeChoice> = { system: 'light', light: 'dark', dark: 'system' };
const LABEL: Record<ThemeChoice, string> = { system: 'לפי המערכת', light: 'בהיר', dark: 'כהה' };
const ICON = { system: SystemIcon, light: SunIcon, dark: MoonIcon };

/** One button that cycles System → Light → Dark. */
export function ThemeToggle() {
  const { choice, choose } = useTheme();
  const Icon = ICON[choice];
  return (
    <IconButton
      label={`ערכת צבעים: ${LABEL[choice]} (לחיצה: ${LABEL[NEXT[choice]]})`}
      onClick={() => choose(NEXT[choice])}
    >
      <Icon />
    </IconButton>
  );
}
