import type { SearchMode } from '../../api/types';
import { formatDate, formatTime } from '../../lib/format';
import { usePlayer, usePlayerTime } from '../../player/PlayerProvider';
import { NextIcon, PrevIcon } from '../icons';
import { ContextPanel } from '../results/ContextPanel';
import { BottomSheet } from '../ui/BottomSheet';
import { IconButton } from '../ui/IconButton';
import { Switch } from '../ui/Switch';
import { DialButton, SpeedControl } from './Controls';
import { ExportAudioButton, PlayPause, Playhead } from './Parts';
import styles from './PlayerSheet.module.css';

/** Full-screen mobile player with the synced transcript. */
export function PlayerSheet({ open, onClose, mode }: { open: boolean; onClose: () => void; mode: SearchMode }) {
  const player = usePlayer();
  const { time, duration } = usePlayerTime();
  const current = player.current;
  if (!current) return null;
  const { episode, hit } = current;
  const total = duration || episode.duration_sec;

  return (
    <BottomSheet open={open} onClose={onClose} title={`התאמה ${player.position.index} מתוך ${player.position.total}`} tall>
      <div className={styles.layout}>
        <header className={styles.meta}>
          <p className="tabular">
            <span className={styles.source}>{episode.source}</span>
            {episode.date && <> · {formatDate(episode.date)}</>}
          </p>
          <h3 className={styles.title}>{episode.title}</h3>
        </header>

        <section className={styles.transcript} aria-label="תמלול מסונכרן">
          <ContextPanel episode={episode} hit={hit} mode={mode} radius={6} large />
        </section>

        <div className={styles.controls}>
          <Playhead thick />
          <div className={`${styles.times} mono`} dir="ltr">
            <span>{formatTime(time)}</span>
            <span>−{formatTime(Math.max(0, total - time))}</span>
          </div>
          <div className={styles.transport} dir="ltr">
            <IconButton label="התאמה קודמת" onClick={player.prev}>
              <PrevIcon />
            </IconButton>
            <DialButton seconds={-5} />
            <PlayPause size={76} />
            <DialButton seconds={5} />
            <IconButton label="התאמה הבאה" onClick={player.next}>
              <NextIcon />
            </IconButton>
          </div>
          <div className={styles.tools}>
            <SpeedControl />
            <Switch variant="pill" label="רק הקטע" checked={player.clipOnly} onChange={player.setClipOnly} />
          </div>
          <ExportAudioButton block />
          {player.error && <p className={styles.error}>{player.error}</p>}
        </div>
      </div>
    </BottomSheet>
  );
}
