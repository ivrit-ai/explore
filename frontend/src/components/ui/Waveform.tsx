import styles from './Waveform.module.css';

/** Looping equaliser bars: shown only while loading or playing. */
export function Waveform({ bars = 5, height = 18 }: { bars?: number; height?: number }) {
  return (
    <span className={styles.wave} style={{ height }} aria-hidden="true">
      {Array.from({ length: bars }, (_, i) => (
        <span key={i} className={styles.bar} style={{ animationDelay: `${i * 120}ms` }} />
      ))}
    </span>
  );
}
