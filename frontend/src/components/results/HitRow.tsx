import { useEffect, useState } from 'react';
import { audioExportUrl } from '../../api/client';
import type { Episode, Hit, SearchMode } from '../../api/types';
import { formatTime } from '../../lib/format';
import { usePlayer } from '../../player/PlayerProvider';
import { CheckIcon, DownloadIcon, LinkIcon } from '../icons';
import { CLIP_PAD_S } from '../player/Parts';
import { Button, ButtonLink } from '../ui/Button';
import { IconButton } from '../ui/IconButton';
import { ContextPanel } from './ContextPanel';
import { Highlight } from './Highlight';
import { PlayButton } from './PlayButton';
import styles from './HitRow.module.css';

interface Props {
  episode: Episode;
  hit: Hit;
  mode: SearchMode;
  contextOpen: boolean;
  onToggleContext: () => void;
}

export function HitRow({ episode, hit, mode, contextOpen, onToggleContext }: Props) {
  const player = usePlayer();
  const [copied, setCopied] = useState(false);
  const active = player.isCurrent(episode, hit);

  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => setCopied(false), 1500);
    return () => clearTimeout(t);
  }, [copied]);

  const onPlay = () => (active ? player.toggle() : player.play({ episode, hit }));
  const copyLink = () => {
    const url = `${window.location.origin}/audio/${encodeURIComponent(episode.uuid)}.opus#t=${Math.floor(hit.start_sec)}`;
    navigator.clipboard?.writeText(url).then(() => setCopied(true));
  };

  return (
    <div className={`${styles.row} ${active ? styles.active : ''}`}>
      <div className={styles.main}>
        <PlayButton
          progress={episode.duration_sec ? hit.start_sec / episode.duration_sec : 0}
          active={active}
          playing={active && player.playing}
          loading={active && player.loading}
          label={active && player.playing ? 'השהיה' : `ניגון מ-${formatTime(hit.start_sec)}`}
          onClick={onPlay}
        />
        <div className={styles.body}>
          <span className={`${styles.time} mono`}>
            {formatTime(hit.start_sec)}
            {active && <span className={styles.live}> · {player.playing ? 'מתנגן' : 'מושהה'}</span>}
          </span>
          <p className={styles.text}>
            <Highlight text={hit.text} match={hit.match} groups={hit.groups} mode={mode} />
          </p>
        </div>
        <div className={styles.actions}>
          <Button variant="secondary" aria-expanded={contextOpen} onClick={onToggleContext}>
            הקשר
          </Button>
          <ButtonLink
            variant="secondary"
            icon={<DownloadIcon size={16} />}
            href={audioExportUrl(episode.uuid, Math.max(0, hit.start_sec - CLIP_PAD_S), hit.end_sec + CLIP_PAD_S)}
            download
          >
            ייצוא אודיו
          </ButtonLink>
          <IconButton label={copied ? 'הקישור הועתק' : 'העתקת קישור לרגע הזה'} onClick={copyLink}>
            {copied ? <CheckIcon size={16} /> : <LinkIcon size={16} />}
          </IconButton>
        </div>
      </div>
      {contextOpen && (
        <div className={styles.context}>
          <ContextPanel episode={episode} hit={hit} mode={mode} />
        </div>
      )}
    </div>
  );
}
