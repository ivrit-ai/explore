import { useState } from 'react';
import type { SearchMode } from '../../api/types';
import { useIsMobile } from '../../hooks/useMediaQuery';
import { formatTime } from '../../lib/format';
import { usePlayer, usePlayerTime } from '../../player/PlayerProvider';
import { NextIcon, PrevIcon } from '../icons';
import { IconButton } from '../ui/IconButton';
import { Switch } from '../ui/Switch';
import { DialButton, SpeedControl } from './Controls';
import { ExportAudioButton, PlayPause, Playhead } from './Parts';
import { PlayerSheet } from './PlayerSheet';
import styles from './PlayerBar.module.css';

/** Sticky bottom player shared by every result. Mini version on mobile. */
export function PlayerBar({ mode }: { mode: SearchMode }) {
  const player = usePlayer();
  const { time, duration } = usePlayerTime();
  const isMobile = useIsMobile();
  const [sheetOpen, setSheetOpen] = useState(false);
  const current = player.current;
  if (!current) return null;
  const { episode } = current;
  const total = duration || episode.duration_sec;

  if (isMobile) {
    return (
      <>
        <div className={styles.mini} role="region" aria-label="נגן">
          <Playhead />
          <div className={styles.miniRow}>
            <PlayPause size={44} />
            <button type="button" className={styles.miniInfo} onClick={() => setSheetOpen(true)} aria-label="פתיחת הנגן">
              <span className={styles.title}>{episode.title}</span>
              <span className={`${styles.sub} tabular`}>
                <span className="mono" dir="ltr">
                  {formatTime(time)} / {formatTime(total)}
                </span>
                {' · '}התאמה {player.position.index} מתוך {player.position.total}
              </span>
            </button>
            <IconButton label="התאמה הבאה" onClick={player.next}>
              <NextIcon />
            </IconButton>
          </div>
          {player.error && <p className={styles.error}>{player.error}</p>}
        </div>
        <PlayerSheet open={sheetOpen} onClose={() => setSheetOpen(false)} mode={mode} />
      </>
    );
  }

  return (
    <div className={styles.bar} role="region" aria-label="נגן">
      <Playhead />
      <div className={styles.row}>
        <div className={styles.info}>
          <span className={styles.title}>{episode.title}</span>
          <span className={`${styles.sub} tabular`}>
            {episode.source} · התאמה {player.position.index} מתוך {player.position.total}
            {player.error && <span className={styles.errorInline}> · {player.error}</span>}
          </span>
        </div>
        <div className={styles.transport} dir="ltr">
          <IconButton label="התאמה קודמת" onClick={player.prev}>
            <PrevIcon />
          </IconButton>
          <DialButton seconds={-5} size={56} />
          <PlayPause />
          <DialButton seconds={5} size={56} />
          <IconButton label="התאמה הבאה" onClick={player.next}>
            <NextIcon />
          </IconButton>
        </div>
        <div className={styles.tools}>
          <span className={`${styles.clock} mono`} dir="ltr">
            {formatTime(time)} / {formatTime(total)}
          </span>
          <SpeedControl showLabel={false} />
          <Switch variant="pill" label="רק הקטע" checked={player.clipOnly} onChange={player.setClipOnly} />
          <ExportAudioButton />
        </div>
      </div>
    </div>
  );
}
