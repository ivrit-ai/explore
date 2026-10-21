import { SPEEDS, usePlayer } from '../../player/PlayerProvider';
import styles from './Controls.module.css';

/** Oversized ±5s scrub button with a knurled dial texture. */
export function DialButton({ seconds, size = 76 }: { seconds: 5 | -5; size?: number }) {
  const player = usePlayer();
  const back = seconds < 0;
  return (
    <button
      type="button"
      className={`${styles.dial} ${back ? styles.back : styles.fwd}`}
      style={{ width: size, height: size }}
      aria-label={back ? '5 שניות אחורה' : '5 שניות קדימה'}
      onClick={() => player.seekBy(seconds)}
    >
      <span className={styles.texture} aria-hidden="true" />
      <span className={styles.dialLabel}>
        <span className="mono">{back ? '−5' : '+5'}</span>
        {size >= 64 && <span className={styles.unit}>שנ׳</span>}
      </span>
    </button>
  );
}

/** Cycles 1× → 1.25× → 1.5× → 2×; the value rolls vertically. */
export function SpeedControl({ showLabel = true }: { showLabel?: boolean }) {
  const player = usePlayer();
  const speed = SPEEDS[player.speedIndex];
  return (
    <button type="button" className={styles.speed} onClick={player.cycleSpeed} aria-label={`מהירות ניגון ${speed}×`}>
      {showLabel && <span className={styles.speedLabel}>מהירות</span>}
      <span className={styles.speedWindow} aria-hidden="true">
        <span className={styles.speedStrip} style={{ transform: `translateY(${-22 * player.speedIndex}px)` }}>
          {SPEEDS.map((s) => (
            <span key={s} className="mono">
              {s}×
            </span>
          ))}
        </span>
      </span>
    </button>
  );
}
