import { useMemo, useState } from 'react';
import { searchMetadata } from '../../api/client';
import { useAsync } from '../../hooks/useAsync';
import type { SearchState } from '../../lib/searchState';
import { ArrowLeftIcon } from '../icons';
import { Button } from '../ui/Button';
import { RollingNumber } from '../ui/RollingNumber';
import { SegmentedControl } from '../ui/SegmentedControl';
import { SourceRow } from './SourceRow';
import styles from './FilterPanel.module.css';

export interface FilterValues {
  dateFrom: string | null;
  dateTo: string | null;
  sources: string[];
}

type Preset = 'all' | 'year' | 'five' | 'range';

const iso = (d: Date) => d.toISOString().slice(0, 10);

function presetDates(preset: Preset): Pick<FilterValues, 'dateFrom' | 'dateTo'> {
  const now = new Date();
  if (preset === 'year') return { dateFrom: `${now.getFullYear()}-01-01`, dateTo: null };
  if (preset === 'five') {
    const d = new Date(now);
    d.setFullYear(d.getFullYear() - 5);
    return { dateFrom: iso(d), dateTo: null };
  }
  return { dateFrom: null, dateTo: null };
}

interface Props {
  state: SearchState;
  onApply: (values: FilterValues) => void;
  layout: 'sidebar' | 'sheet';
}

export function FilterPanel({ state, onApply, layout }: Props) {
  const [dateFrom, setDateFrom] = useState(state.dateFrom);
  const [dateTo, setDateTo] = useState(state.dateTo);
  const [preset, setPreset] = useState<Preset>(state.dateFrom || state.dateTo ? 'range' : 'all');
  // Empty selection means "all sources".
  const [selected, setSelected] = useState<Set<string>>(new Set(state.sources));
  const [filter, setFilter] = useState('');

  const draft = { ...state, dateFrom, dateTo };
  const { data: meta, loading } = useAsync(
    (signal) => searchMetadata(draft, signal),
    `${state.q}|${state.mode}|${dateFrom}|${dateTo}`,
  );

  const sources = useMemo(() => {
    const listed = meta?.sources ?? [];
    const names = new Set(listed.map((s) => s.name));
    const missing = [...selected].filter((n) => !names.has(n)).map((name) => ({ name, count: 0 }));
    return [...listed, ...missing];
  }, [meta, selected]);

  const visible = filter ? sources.filter((s) => s.name.includes(filter)) : sources;
  const allSelected = selected.size === 0;
  const count = sources.reduce((sum, s) => sum + (allSelected || selected.has(s.name) ? s.count : 0), 0);

  const toggle = (name: string) => {
    setSelected((prev) => {
      // From "all", unticking one source means "every other source".
      const next = prev.size === 0 ? new Set(sources.map((s) => s.name)) : new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next.size === sources.length ? new Set() : next;
    });
  };

  const choosePreset = (p: Preset) => {
    setPreset(p);
    if (p !== 'range') {
      const d = presetDates(p);
      setDateFrom(d.dateFrom);
      setDateTo(d.dateTo);
    }
  };

  const reset = () => {
    choosePreset('all');
    setSelected(new Set());
    setFilter('');
  };

  return (
    <div className={`${styles.panel} ${styles[layout]}`}>
      <div className={styles.fields}>
        {layout === 'sidebar' && (
          <div className={styles.head}>
            <h2 className={styles.heading}>סינון</h2>
            <Button variant="ghost" onClick={reset}>
              איפוס
            </Button>
          </div>
        )}

        <fieldset className={styles.fieldset}>
          <legend className={styles.legend}>תאריך שידור</legend>
          <SegmentedControl<Preset>
            label="טווח תאריכים"
            size="sm"
            value={preset}
            onChange={choosePreset}
            options={[
              { value: 'all', label: 'הכל' },
              { value: 'year', label: 'השנה' },
              { value: 'five', label: '5 שנים' },
              { value: 'range', label: 'טווח' },
            ]}
          />
          {preset === 'range' && (
            <div className={styles.dates}>
              <label className={styles.date}>
                <span>מתאריך</span>
                <input type="date" value={dateFrom ?? ''} max={dateTo ?? undefined} onChange={(e) => setDateFrom(e.target.value || null)} />
              </label>
              <label className={styles.date}>
                <span>עד תאריך</span>
                <input type="date" value={dateTo ?? ''} min={dateFrom ?? undefined} onChange={(e) => setDateTo(e.target.value || null)} />
              </label>
            </div>
          )}
        </fieldset>

        <fieldset className={styles.fieldset}>
          <legend className={styles.legend}>
            <span className={styles.legendRow}>
              <span className="tabular">
                מקורות · {allSelected ? sources.length : selected.size}/{sources.length}
              </span>
              {!allSelected && (
                <Button variant="ghost" onClick={() => setSelected(new Set())}>
                  בחירת הכל
                </Button>
              )}
            </span>
          </legend>
          {sources.length > 8 && (
            <>
              <label htmlFor={`src-filter-${layout}`} className="sr-only">
                חיפוש מקור
              </label>
              <input
                id={`src-filter-${layout}`}
                type="search"
                className={styles.search}
                placeholder="חיפוש מקור…"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
              />
            </>
          )}
          <div className={styles.sources}>
            {loading && !meta && <p className={styles.status}>טוען מקורות…</p>}
            {meta && sources.length === 0 && <p className={styles.status}>אין מקורות בטווח הזה</p>}
            {visible.map((s) => (
              <SourceRow key={s.name} name={s.name} count={s.count} checked={allSelected || selected.has(s.name)} onToggle={() => toggle(s.name)} />
            ))}
          </div>
        </fieldset>
      </div>

      <div className={styles.footer}>
        {layout === 'sheet' && (
          <Button variant="secondary" size="lg" onClick={reset}>
            איפוס
          </Button>
        )}
        <Button
          variant="primary"
          size="lg"
          block
          trailing={<ArrowLeftIcon size={18} />}
          onClick={() => onApply({ dateFrom, dateTo, sources: [...selected] })}
        >
          הצגת <RollingNumber value={count} /> פרקים
        </Button>
      </div>
    </div>
  );
}
