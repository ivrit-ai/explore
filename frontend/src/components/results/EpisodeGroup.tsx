import { useState } from 'react';
import type { Episode, Hit, SearchMode } from '../../api/types';
import { formatDate, formatTime } from '../../lib/format';
import { usePlayer } from '../../player/PlayerProvider';
import { ChevronDownIcon } from '../icons';
import { HitRow } from './HitRow';
import styles from './EpisodeGroup.module.css';
import { HITS_PAGE } from '../../lib/hitPaging';

const matchCount = (n: number) => (n === 1 ? 'התאמה אחת' : `${n} התאמות`);

interface Props {
  episode: Episode;
  hits: Hit[];
  mode: SearchMode;
  open: boolean;
  onToggle: () => void;
  /** Count shown in the header; defaults to the hits passed in. */
  total?: number;
  /** Hits the server has beyond those loaded; shows a "show more" button. */
  remaining?: number;
  loadingMore?: boolean;
  onLoadMore?: () => void;
}

/** One episode: serif title, a metadata line, and its matches indented beneath. */
export function EpisodeGroup({ episode, hits, mode, open, onToggle, total, remaining = 0, loadingMore, onLoadMore }: Props) {
  const player = usePlayer();
  const [contextFor, setContextFor] = useState<number | null>(null);
  const active = player.current?.episode.episode_idx === episode.episode_idx;
  const bodyId = `episode-${episode.episode_idx}`;

  return (
    <section className={`${styles.group} ${active ? styles.active : ''}`}>
      <h2 className={styles.heading}>
        <button type="button" className={styles.header} aria-expanded={open} aria-controls={bodyId} onClick={onToggle}>
          <span className={styles.titles}>
            <span className={styles.title}>{episode.title}</span>
            <span className={`${styles.meta} tabular`}>
              <span className={styles.source}>{episode.source}</span>
              {episode.date && <> · {formatDate(episode.date)}</>}
              {episode.duration_sec > 0 && (
                <>
                  {' · '}
                  <span className="mono" dir="ltr">
                    {formatTime(episode.duration_sec)}
                  </span>
                </>
              )}
              {' · '}
              {matchCount(total ?? hits.length)}
            </span>
          </span>
          <span className={styles.chevron} aria-hidden="true">
            <ChevronDownIcon size={16} />
          </span>
        </button>
      </h2>
      {open && (
        <div id={bodyId} className={styles.hits}>
          {hits.map((hit) => (
            <HitRow
              key={hit.char_offset}
              episode={episode}
              hit={hit}
              mode={mode}
              contextOpen={contextFor === hit.char_offset}
              onToggleContext={() => setContextFor((c) => (c === hit.char_offset ? null : hit.char_offset))}
            />
          ))}
          {remaining > 0 && onLoadMore && (
            <button type="button" className={styles.more} onClick={onLoadMore} disabled={loadingMore}>
              {loadingMore ? 'טוען…' : `הצג עוד ${Math.min(remaining, HITS_PAGE)} (נותרו ${remaining})`}
            </button>
          )}
        </div>
      )}
    </section>
  );
}
