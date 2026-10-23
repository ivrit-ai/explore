import styles from './Switch.module.css';

interface Props {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: string;
  /** "pill" shows the label inside a toggle chip (e.g. "רק הקטע"). */
  variant?: 'switch' | 'pill';
}

export function Switch({ checked, onChange, label, variant = 'switch' }: Props) {
  if (variant === 'pill') {
    return (
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        className={styles.pill}
        onClick={() => onChange(!checked)}
      >
        <span className={styles.dot} aria-hidden="true" />
        {label}
      </button>
    );
  }
  return (
    <label className={styles.row}>
      {label}
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        aria-label={label}
        className={styles.track}
        onClick={() => onChange(!checked)}
      >
        <span className={styles.knob} aria-hidden="true" />
      </button>
    </label>
  );
}
