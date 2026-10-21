import { episodeSegments } from '../../api/client';
import type { Episode, Hit, SearchMode } from '../../api/types';
import { useAsync } from '../../hooks/useAsync';
import { formatTime } from '../../lib/format';
import { usePlayer, usePlayerTime } from '../../player/PlayerProvider';
import { Highlight } from './Highlight';
import styles from './ContextPanel.module.css';

interface Props {
  episode: Episode;
  hit: Hit;
  mode: SearchMode;
  radius?: number;
  /** Larger type for the full-screen mobile player. */
  large?: boolean;
}

/** Transcript lines around a hit; click a line to play from it. */
export function ContextPanel({ episode, hit, mode, radius = 5, large }: Props) {
  const player = usePlayer();
  const { time } = usePlayerTime();
  const { data, error, loading } = useAsync(
    (signal) => episodeSegments(episode.episode_idx, hit.segment_idx, radius, signal),
    `${episode.episode_idx}:${hit.segment_idx}:${radius}`,
  );
  const thisEpisodePlaying = player.current?.episode.episode_idx === episode.episode_idx;

  if (error) return <p className={styles.status}>{error}</p>;
  if (loading && !data) return <p className={styles.status}>טוען את ההקשר…</p>;

  return (
    <ol className={`${styles.lines} ${large ? styles.large : ''}`}>
      {data?.map((seg) => {
        const isHit = seg.segment_idx === hit.segment_idx;
        const isLive = thisEpisodePlaying && time >= seg.start_sec && time < seg.end_sec;
        return (
          <li key={seg.segment_idx}>
            <button
              type="button"
              className={`${styles.line} ${isHit ? styles.hit : ''} ${isLive ? styles.live : ''}`}
              aria-current={isLive || undefined}
              onClick={() => player.play({ episode, hit }, seg.start_sec)}
            >
              <span className={`${styles.time} mono`}>{formatTime(seg.start_sec)}</span>
              <span className={styles.text}>
                {isHit ? <Highlight text={hit.text} match={hit.match} groups={hit.groups} mode={mode} /> : seg.text}
              </span>
            </button>
          </li>
        );
      })}
    </ol>
  );
}
