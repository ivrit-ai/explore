import type { SearchMode } from '../../api/types';
import highlight from '../results/Highlight.module.css';
import styles from './SearchForm.module.css';

const M = ({ children }: { children: string }) => <mark className={highlight.match}>{children}</mark>;

/** One line that shows what the selected mode will match. */
export function ModeHint({ mode }: { mode: SearchMode }) {
  return (
    <p className={styles.hint} aria-live="polite">
      {mode === 'exact' && (
        <>
          ״ברית״ תמצא רק את <M>ברית</M> עצמה, לא את ״עברית״.
        </>
      )}
      {mode === 'partial' && (
        <>
          ״חבר״ תמצא גם את <M>חבר</M>ים ואת <M>חבר</M>ות.
        </>
      )}
      {mode === 'regex' && (
        <>
          ביטוי רגולרי בתחביר Python, למשל{' '}
          <code dir="ltr" className={styles.code}>
            \bשלום\b
          </code>{' '}
          למילה שלמה.
        </>
      )}
    </p>
  );
}
