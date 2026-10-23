import { audioExportUrl } from '../../api/client';
import { formatTime } from '../../lib/format';
import { usePlayer, usePlayerTime } from '../../player/PlayerProvider';
import { DownloadIcon, PauseIcon, PlayIcon } from '../icons';
import { ButtonLink } from '../ui/Button';
import styles from './PlayerBar.module.css';

/** Seconds of padding around the matched segment in exported clips. */
export const CLIP_PAD_S = 1;

/** Playhead line with the clip window and a glowing head. */
export function Playhead({ thick }: { thick?: boolean }) {
  const player = usePlayer();
  const { time, duration } = usePlayerTime();
  const hit = player.current?.hit;
  const total = duration || player.current?.episode.duration_sec || 0;
  const pct = (v: number) => (total ? Math.min(100, (v / total) * 100) : 0);
  return (
    <div className={`${styles.playhead} ${thick ? styles.thick : ''}`} dir="ltr" aria-hidden="true">
      {hit && (
        <span
          className={styles.clip}
          style={{ left: `${pct(hit.start_sec)}%`, width: `${Math.max(0.6, pct(hit.end_sec) - pct(hit.start_sec))}%` }}
        />
      )}
      <span className={styles.progress} style={{ width: `${pct(time)}%` }} />
      <span className={styles.head} style={{ left: `${pct(time)}%` }} />
    </div>
  );
}

export function PlayPause({ size = 56 }: { size?: number }) {
  const player = usePlayer();
  return (
    <button
      type="button"
      className={styles.playPause}
      style={{ width: size, height: size }}
      aria-label={player.playing ? 'השהיה' : 'ניגון'}
      onClick={player.toggle}
    >
      {player.playing ? <PauseIcon size={size / 2.5} /> : <PlayIcon size={size / 2.5} />}
    </button>
  );
}

export function ExportAudioButton({ block }: { block?: boolean }) {
  const player = usePlayer();
  const current = player.current;
  if (!current) return null;
  const { hit, episode } = current;
  const start = Math.max(0, hit.start_sec - CLIP_PAD_S);
  const end = hit.end_sec + CLIP_PAD_S;
  return (
    <ButtonLink
      variant="secondary"
      icon={<DownloadIcon size={16} />}
      href={audioExportUrl(episode.uuid, start, end)}
      download
      block={block}
      trailing={
        block ? (
          <span className={`${styles.range} mono`} dir="ltr">
            {formatTime(start)}–{formatTime(end)}
          </span>
        ) : undefined
      }
    >
      ייצוא אודיו
    </ButtonLink>
  );
}
