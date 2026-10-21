import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { csvExportUrl, search } from '../api/client';
import type { Episode, SearchMode } from '../api/types';
import { FilterPanel, type FilterValues } from '../components/filters/FilterPanel';
import { DownloadIcon, FilterIcon, ShuffleIcon } from '../components/icons';
import { AppHeader } from '../components/layout/AppHeader';
import { Footer } from '../components/layout/Footer';
import { PlayerBar } from '../components/player/PlayerBar';
import { EpisodeGroup } from '../components/results/EpisodeGroup';
import { CombosPanel, RegexPattern, WordFormsPanel } from '../components/results/Insights';
import { Pagination } from '../components/results/Pagination';
import { SearchForm, type SearchDraft } from '../components/search/SearchForm';
import { BottomSheet } from '../components/ui/BottomSheet';
import { Button, ButtonLink } from '../components/ui/Button';
import { useAsync } from '../hooks/useAsync';
import { useIsMobile } from '../hooks/useMediaQuery';
import { groupCombos, hitComboKey, hitWord, wordForms } from '../lib/insights';
import { newSearch, readSearchState, toSearchParams, type SearchState } from '../lib/searchState';
import { usePlayer } from '../player/PlayerProvider';
import styles from './ResultsPage.module.css';

const MODE_LABEL: Record<SearchMode, string> = { exact: 'מילים שלמות', partial: 'חלקי', regex: 'ביטוי רגולרי' };

const randomSeed = () => Math.floor(Math.random() * 2 ** 31);

export function ResultsPage() {
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const isMobile = useIsMobile();
  const player = usePlayer();
  const state = useMemo(() => readSearchState(params), [params]);

  const [draft, setDraft] = useState<SearchDraft>(state);
  const [closed, setClosed] = useState<Set<number>>(new Set());
  const [focus, setFocus] = useState<string | null>(null);
  const [filtersOpen, setFiltersOpen] = useState(false);

  // Shuffled results need a fixed seed so paging and reloads keep the order.
  const needsSeed = state.shuffle && state.seed === null;
  const key = needsSeed || !state.q.trim() ? '' : toSearchParams(state).toString();
  const filterKey = `${state.q}|${state.mode}|${state.dateFrom}|${state.dateTo}|${state.sources.join(',')}`;

  useEffect(() => {
    if (!state.q.trim()) navigate('/', { replace: true });
    else if (needsSeed) setParams(toSearchParams({ ...state, seed: randomSeed() }), { replace: true });
  }, [state, needsSeed, navigate, setParams]);

  useEffect(() => {
    setDraft({ q: state.q, mode: state.mode, perPage: state.perPage, shuffle: state.shuffle });
    document.title = `${state.q} — ivrit.ai Explore`;
  }, [state.q, state.mode, state.perPage, state.shuffle]);

  useEffect(() => {
    setClosed(new Set());
    setFocus(null);
    window.scrollTo({ top: 0 });
  }, [key]);

  const { data, error, loading, reload } = useAsync(key ? (signal) => search(state, signal) : null, key);

  const { setQueue } = player;
  useEffect(() => {
    if (data) setQueue(data.episodes.flatMap((episode) => episode.hits.map((hit) => ({ episode, hit }))));
  }, [data, setQueue]);

  // Jumping to a match in a collapsed episode opens it.
  const playingEpisode = player.current?.episode.episode_idx;
  useEffect(() => {
    if (playingEpisode === undefined) return;
    setClosed((prev) => {
      if (!prev.has(playingEpisode)) return prev;
      const next = new Set(prev);
      next.delete(playingEpisode);
      return next;
    });
  }, [playingEpisode]);

  const go = (next: SearchState) => navigate(`/search?${toSearchParams(next)}`);
  const onSearch = (d: SearchDraft) => go(newSearch(d.q, d.mode, { ...state, perPage: d.perPage, shuffle: d.shuffle }));
  const onFilters = (f: FilterValues) => {
    setFiltersOpen(false);
    go({ ...state, ...f, page: 1, seed: null });
  };

  const episodes = data?.episodes ?? [];
  const forms = state.mode === 'partial' ? wordForms(episodes) : [];
  const combos = state.mode === 'regex' && episodes.some((e) => e.hits.some((h) => h.groups.length)) ? groupCombos(episodes) : [];
  const keyOfHit = state.mode === 'partial' ? hitWord : hitComboKey;
  const visible: Episode[] = focus
    ? episodes.map((e) => ({ ...e, hits: e.hits.filter((h) => keyOfHit(h) === focus) })).filter((e) => e.hits.length)
    : episodes;

  const activeFilters = (state.dateFrom || state.dateTo ? 1 : 0) + (state.sources.length ? 1 : 0);
  const filterPanel = (layout: 'sidebar' | 'sheet') => <FilterPanel key={filterKey} state={state} onApply={onFilters} layout={layout} />;

  return (
    <div className={styles.page} style={{ ['--player-h' as string]: player.current ? (isMobile ? '72px' : '84px') : '0px' }}>
      <AppHeader sticky>
        <SearchForm variant="compact" value={draft} onChange={setDraft} onSubmit={onSearch} />
      </AppHeader>

      <div className={styles.layout}>
        {!isMobile && <aside className={styles.sidebar} aria-label="סינון">{filterPanel('sidebar')}</aside>}

        <main className={styles.main} aria-busy={loading}>
          <div className={styles.summary}>
            <div className={styles.summaryText}>
              <h1 className={state.mode === 'regex' ? styles.patternTitle : styles.title}>
                {state.mode === 'regex' ? <RegexPattern pattern={state.q} /> : `״${state.q}״`}
              </h1>
              <p className={`${styles.meta} tabular`}>
                {data ? (
                  <>
                    <strong>{data.total_hits.toLocaleString('he-IL')}</strong> התאמות ב-<strong>{episodes.length}</strong> פרקים
                  </>
                ) : (
                  'מחפש…'
                )}
                {' · '}
                {MODE_LABEL[state.mode]}
                {state.shuffle && ' · סדר מעורבב'}
              </p>
            </div>
            <div className={styles.actions}>
              {isMobile && (
                <Button icon={<FilterIcon size={16} />} onClick={() => setFiltersOpen(true)} trailing={activeFilters ? <span className={styles.badge}>{activeFilters}</span> : undefined}>
                  סינון
                </Button>
              )}
              {state.shuffle && (
                <Button icon={<ShuffleIcon size={16} />} onClick={() => go({ ...state, page: 1, seed: randomSeed() })}>
                  ערבוב מחדש
                </Button>
              )}
              <ButtonLink icon={<DownloadIcon size={16} />} href={csvExportUrl(state)} download>
                ייצוא CSV
              </ButtonLink>
            </div>
          </div>

          {error && (
            <div className={styles.notice} role="alert">
              <p>{error}</p>
              <Button onClick={reload}>ניסיון נוסף</Button>
            </div>
          )}

          {loading && !data && !error && (
            <div className={styles.skeleton} aria-hidden="true">
              {[0, 1, 2].map((i) => (
                <div key={i} className={styles.skeletonGroup}>
                  <span />
                  <span />
                  <span />
                </div>
              ))}
            </div>
          )}

          {data && episodes.length === 0 && (
            <div className={styles.notice}>
              <p>לא נמצאו תוצאות עבור ״{state.q}״.</p>
              <div className={styles.noticeActions}>
                {state.mode === 'exact' && <Button onClick={() => go({ ...state, mode: 'partial', page: 1 })}>חיפוש חלקי</Button>}
                {activeFilters > 0 && (
                  <Button onClick={() => go({ ...state, dateFrom: null, dateTo: null, sources: [], page: 1 })}>ניקוי הסינון</Button>
                )}
              </div>
            </div>
          )}

          {data && episodes.length > 0 && (
            <div className={loading ? styles.stale : undefined}>
              {state.mode === 'partial' && <WordFormsPanel forms={forms} selected={focus} onSelect={setFocus} />}
              {state.mode === 'regex' && <CombosPanel combos={combos} selected={focus} onSelect={setFocus} />}

              {visible.map((episode) => (
                <EpisodeGroup
                  key={episode.episode_idx}
                  episode={episode}
                  hits={episode.hits}
                  mode={state.mode}
                  open={!closed.has(episode.episode_idx)}
                  onToggle={() =>
                    setClosed((prev) => {
                      const next = new Set(prev);
                      if (next.has(episode.episode_idx)) next.delete(episode.episode_idx);
                      else next.add(episode.episode_idx);
                      return next;
                    })
                  }
                />
              ))}

              <Pagination
                page={state.page}
                hasMore={data.has_more}
                episodesOnPage={episodes.length}
                perPage={state.perPage}
                hrefFor={(page) => `/search?${toSearchParams({ ...state, page })}`}
              />
            </div>
          )}
        </main>
      </div>

      <Footer />
      <PlayerBar mode={state.mode} />

      {isMobile && (
        <BottomSheet open={filtersOpen} onClose={() => setFiltersOpen(false)} title="סינון">
          {filterPanel('sheet')}
        </BottomSheet>
      )}
    </div>
  );
}
