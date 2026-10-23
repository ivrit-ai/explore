import { useId, useState, type FormEvent } from 'react';
import type { SearchMode } from '../../api/types';
import { PER_PAGE_OPTIONS } from '../../lib/searchState';
import { ArrowLeftIcon, SearchIcon } from '../icons';
import { Button } from '../ui/Button';
import { SegmentedControl } from '../ui/SegmentedControl';
import { Switch } from '../ui/Switch';
import { ModeHint } from './ModeHint';
import styles from './SearchForm.module.css';

export interface SearchDraft {
  q: string;
  mode: SearchMode;
  perPage: number;
  shuffle: boolean;
}

interface Props {
  value: SearchDraft;
  onChange: (value: SearchDraft) => void;
  onSubmit: (value: SearchDraft) => void;
  variant: 'hero' | 'compact';
}

export const MODE_OPTIONS = [
  { value: 'exact' as const, label: 'מילים שלמות' },
  { value: 'partial' as const, label: 'חלקי' },
  {
    value: 'regex' as const,
    label: (
      <>
        Regex{' '}
        <span className="mono" dir="ltr" style={{ fontSize: 11, color: 'var(--tx-3)' }}>
          /.*/
        </span>
      </>
    ),
  },
];

export function SearchForm({ value, onChange, onSubmit, variant }: Props) {
  const inputId = useId();
  const [error, setError] = useState<string | null>(null);
  const set = (patch: Partial<SearchDraft>) => onChange({ ...value, ...patch });
  const isRegex = value.mode === 'regex';

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (!value.q.trim()) {
      setError('נא להזין מונח לחיפוש');
      return;
    }
    setError(null);
    onSubmit({ ...value, q: value.q.trim() });
  };

  if (variant === 'compact') {
    return (
      <form className={styles.compact} onSubmit={submit} role="search">
        <label htmlFor={inputId} className="sr-only">
          {isRegex ? 'ביטוי רגולרי' : 'מונח חיפוש'}
        </label>
        <div className={styles.field}>
          <SearchIcon size={17} className={styles.icon} />
          <input
            id={inputId}
            type="search"
            dir={isRegex ? 'ltr' : 'rtl'}
            className={`${styles.input} ${isRegex ? styles.regexInput : ''}`}
            value={value.q}
            onChange={(e) => set({ q: e.target.value })}
            maxLength={200}
            enterKeyHint="search"
          />
        </div>
        <div className={styles.modes}>
          <SegmentedControl label="סוג חיפוש" options={MODE_OPTIONS} value={value.mode} onChange={(mode) => set({ mode })} />
        </div>
        <Button type="submit" variant="primary" className={styles.compactSubmit}>
          חיפוש
        </Button>
        {error && (
          <p className={styles.error} role="alert">
            {error}
          </p>
        )}
      </form>
    );
  }

  return (
    <form className={styles.hero} onSubmit={submit} role="search">
      <h2 className={styles.heroTitle}>חיפוש בתמלולים</h2>
      <label htmlFor={inputId} className="sr-only">
        {isRegex ? 'ביטוי רגולרי' : 'מונח חיפוש'}
      </label>
      <div className={`${styles.field} ${styles.heroField}`}>
        <SearchIcon size={20} className={styles.icon} />
        <input
          id={inputId}
          type="search"
          dir={isRegex ? 'ltr' : 'rtl'}
          className={`${styles.input} ${isRegex ? styles.regexInput : ''}`}
          placeholder={isRegex ? '(שלום|היי)\\s+חברים' : 'מילה, שם או ביטוי…'}
          value={value.q}
          onChange={(e) => set({ q: e.target.value })}
          maxLength={200}
          enterKeyHint="search"
          aria-invalid={!!error}
          aria-describedby={error ? `${inputId}-error` : undefined}
        />
      </div>
      {error && (
        <p id={`${inputId}-error`} className={styles.error} role="alert">
          {error}
        </p>
      )}
      <SegmentedControl label="סוג חיפוש" options={MODE_OPTIONS} value={value.mode} onChange={(mode) => set({ mode })} />
      <ModeHint mode={value.mode} />
      <div className={styles.options}>
        <label className={styles.perPage}>
          פרקים לעמוד
          <select value={value.perPage} onChange={(e) => set({ perPage: Number(e.target.value) })}>
            {PER_PAGE_OPTIONS.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <span className={styles.divider} aria-hidden="true" />
        <Switch label="ערבוב תוצאות" checked={value.shuffle} onChange={(shuffle) => set({ shuffle })} />
      </div>
      <Button type="submit" variant="primary" size="lg" block trailing={<ArrowLeftIcon size={20} />}>
        חיפוש
      </Button>
    </form>
  );
}
