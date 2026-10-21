import type { ReactNode } from 'react';
import type { Combo, WordForm } from '../../lib/insights';
import { groupTint } from '../../lib/highlight';
import { patternPieces } from '../../lib/regexPattern';
import styles from './Insights.module.css';

/** A regex with each capture group in its tint and numbered. */
export function RegexPattern({ pattern }: { pattern: string }) {
  const { pieces } = patternPieces(pattern);
  return (
    <span className={styles.pattern} dir="ltr">
      {pieces.map((p, i) =>
        p.group === -1 ? (
          <span key={i} className={styles.paren}>
            {p.text}
          </span>
        ) : p.group > 0 ? (
          <span key={i} className={`${styles.groupText} ${styles[`g${groupTint(p.group)}`]}`}>
            {p.text}
            <sub className={styles.num}>{p.group}</sub>
          </span>
        ) : (
          <span key={i} className={styles.plain}>
            {p.text}
          </span>
        ),
      )}
    </span>
  );
}

interface CountRowsProps<T> {
  title: string;
  items: T[];
  render: (item: T) => ReactNode;
  keyOf: (item: T) => string;
  countOf: (item: T) => number;
  selected: string | null;
  onSelect: (key: string | null) => void;
}

function CountRows<T>({ title, items, render, keyOf, countOf, selected, onSelect }: CountRowsProps<T>) {
  const max = Math.max(1, ...items.map(countOf));
  return (
    <section className={styles.panel} aria-label={title}>
      <div className={styles.head}>
        <h2 className={styles.title}>{title}</h2>
        <span className={styles.hint}>{selected ? '' : 'לחיצה מסננת את התוצאות בעמוד'}</span>
        {selected && (
          <button type="button" className={styles.clear} onClick={() => onSelect(null)}>
            הצגת הכל
          </button>
        )}
      </div>
      <div className={styles.rows}>
        {items.slice(0, 12).map((item) => {
          const key = keyOf(item);
          return (
            <button
              type="button"
              key={key}
              className={styles.row}
              aria-pressed={selected === key}
              onClick={() => onSelect(selected === key ? null : key)}
            >
              <span className={styles.label}>{render(item)}</span>
              <span className={styles.bar} dir="ltr" aria-hidden="true">
                <span style={{ width: `${(countOf(item) / max) * 100}%` }} />
              </span>
              <span className={`${styles.count} mono`}>{countOf(item)}</span>
            </button>
          );
        })}
      </div>
    </section>
  );
}

export function WordFormsPanel({ forms, selected, onSelect }: { forms: WordForm[]; selected: string | null; onSelect: (k: string | null) => void }) {
  if (forms.length < 2) return null;
  return (
    <CountRows
      title="המילים שנמצאו"
      items={forms}
      keyOf={(f) => f.key}
      countOf={(f) => f.count}
      selected={selected}
      onSelect={onSelect}
      render={(f) => (
        <>
          {f.before}
          <mark className={styles.match}>{f.core}</mark>
          {f.after}
        </>
      )}
    />
  );
}

export function CombosPanel({ combos, selected, onSelect }: { combos: Combo[]; selected: string | null; onSelect: (k: string | null) => void }) {
  if (combos.length < 2) return null;
  return (
    <CountRows
      title="הצירופים שנמצאו"
      items={combos}
      keyOf={(c) => c.key}
      countOf={(c) => c.count}
      selected={selected}
      onSelect={onSelect}
      render={(c) =>
        c.parts.map((part, i) => (
          <span key={i}>
            {i > 0 && ' '}
            <span className={`${styles.groupText} ${styles[`g${groupTint(i + 1)}`]}`}>{part || '∅'}</span>
          </span>
        ))
      }
    />
  );
}
