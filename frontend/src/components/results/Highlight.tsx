import type { SearchMode, Span } from '../../api/types';
import { buildPieces, groupTint } from '../../lib/highlight';
import styles from './Highlight.module.css';

interface Props {
  text: string;
  match: Span;
  groups: (Span | null)[];
  mode: SearchMode;
}

/** Segment text with the match (or each regex group) marked. */
export function Highlight({ text, match, groups, mode }: Props) {
  return (
    <>
      {buildPieces(text, match, groups, mode).map((p, i) => {
        if (p.kind === 'match') {
          const mark = <mark className={styles.match}>{p.text}</mark>;
          return p.inWord ? (
            <span key={i} className={styles.word}>
              {mark}
            </span>
          ) : (
            <span key={i}>{mark}</span>
          );
        }
        if (p.kind === 'group') {
          return (
            <mark key={i} className={`${styles.group} ${styles[`g${groupTint(p.group)}`]}`}>
              {p.text}
              {p.groupEnd && <sub className={styles.num}>{p.group}</sub>}
            </mark>
          );
        }
        return p.inWord ? (
          <span key={i} className={styles.word}>
            {p.text}
          </span>
        ) : (
          <span key={i}>{p.text}</span>
        );
      })}
    </>
  );
}
