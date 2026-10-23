import styles from './SourceRow.module.css';

interface Props {
  name: string;
  count: number;
  checked: boolean;
  onToggle: () => void;
}

/** A source with a check mark that draws itself and a tabular count. */
export function SourceRow({ name, count, checked, onToggle }: Props) {
  return (
    <button type="button" role="checkbox" aria-checked={checked} className={styles.row} onClick={onToggle}>
      <span className={styles.box} aria-hidden="true">
        <svg width="18" height="18" viewBox="0 0 20 20">
          <path className={styles.check} d="M5 10.5l3.2 3.2L15 7" />
        </svg>
      </span>
      <span className={styles.name}>{name}</span>
      <span className={`${styles.count} mono`}>{count.toLocaleString('en-US')}</span>
    </button>
  );
}
