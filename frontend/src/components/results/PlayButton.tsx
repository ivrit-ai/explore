import { PlayIcon } from '../icons';
import { Waveform } from '../ui/Waveform';
import styles from './PlayButton.module.css';

const R = 20.5;
const C = 2 * Math.PI * R;

interface Props {
  /** How far into the episode the match is, 0..1. */
  progress: number;
  active: boolean;
  playing: boolean;
  loading?: boolean;
  label: string;
  onClick: () => void;
}

/** Round play button; its ring shows where in the episode the match sits. */
export function PlayButton({ progress, active, playing, loading, label, onClick }: Props) {
  const dash = `${(C * Math.min(1, Math.max(0.03, progress))).toFixed(1)} ${C.toFixed(1)}`;
  return (
    <button type="button" className={styles.button} aria-label={label} aria-pressed={active && playing} onClick={onClick}>
      <svg className={styles.ring} width="44" height="44" viewBox="0 0 44 44" aria-hidden="true">
        <circle cx="22" cy="22" r={R} className={styles.track} />
        <circle cx="22" cy="22" r={R} className={styles.progress} strokeDasharray={dash} />
      </svg>
      <span className={`${styles.disc} ${active ? styles.active : ''}`}>
        {active && loading ? (
          <span className={styles.spinner} aria-hidden="true" />
        ) : active && playing ? (
          <Waveform bars={3} height={14} />
        ) : (
          <PlayIcon size={14} className={styles.play} />
        )}
      </span>
    </button>
  );
}
