import { Link } from 'react-router-dom';
import styles from './Pagination.module.css';

interface Props {
  page: number;
  hasMore: boolean;
  episodesOnPage: number;
  perPage: number;
  hrefFor: (page: number) => string;
}

export function Pagination({ page, hasMore, episodesOnPage, perPage, hrefFor }: Props) {
  if (page === 1 && !hasMore) return null;
  const first = (page - 1) * perPage + 1;
  return (
    <nav aria-label="עמודים" className={styles.nav}>
      <span className={`${styles.info} tabular`}>
        עמוד {page} · פרקים {first}–{first + episodesOnPage - 1}
      </span>
      <div className={styles.controls}>
        {page > 1 ? (
          <Link className={styles.button} to={hrefFor(page - 1)}>
            → הקודם
          </Link>
        ) : (
          <span className={`${styles.button} ${styles.disabled}`} aria-disabled="true">
            → הקודם
          </span>
        )}
        <span className={`${styles.button} ${styles.current} tabular`} aria-current="page">
          {page}
        </span>
        {hasMore ? (
          <Link className={styles.button} to={hrefFor(page + 1)}>
            הבא ←
          </Link>
        ) : (
          <span className={`${styles.button} ${styles.disabled}`} aria-disabled="true">
            הבא ←
          </span>
        )}
      </div>
    </nav>
  );
}
