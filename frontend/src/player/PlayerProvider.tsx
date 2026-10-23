import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { audioUrl } from '../api/client';
import type { Episode, Hit } from '../api/types';

export interface PlayerItem {
  episode: Episode;
  hit: Hit;
}

export const SPEEDS = [1, 1.25, 1.5, 2] as const;

interface PlayerApi {
  current: PlayerItem | null;
  playing: boolean;
  loading: boolean;
  error: string | null;
  speedIndex: number;
  clipOnly: boolean;
  position: { index: number; total: number };
  setQueue: (items: PlayerItem[]) => void;
  play: (item: PlayerItem, startAt?: number) => void;
  toggle: () => void;
  seekBy: (seconds: number) => void;
  next: () => void;
  prev: () => void;
  cycleSpeed: () => void;
  setClipOnly: (on: boolean) => void;
  isCurrent: (episode: Episode, hit: Hit) => boolean;
}

interface PlayerTime {
  time: number;
  duration: number;
}

const PlayerContext = createContext<PlayerApi | null>(null);
// Time changes several times a second; keep it apart so hit rows don't re-render.
const PlayerTimeContext = createContext<PlayerTime>({ time: 0, duration: 0 });

const sameHit = (a: PlayerItem | null, episode: Episode, hit: Hit) =>
  !!a && a.episode.episode_idx === episode.episode_idx && a.hit.char_offset === hit.char_offset;

export function PlayerProvider({ children }: { children: ReactNode }) {
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const [current, setCurrent] = useState<PlayerItem | null>(null);
  const [queue, setQueueState] = useState<PlayerItem[]>([]);
  const [playing, setPlaying] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [speedIndex, setSpeedIndex] = useState(0);
  const [clipOnly, setClipOnly] = useState(false);
  const [time, setTime] = useState<PlayerTime>({ time: 0, duration: 0 });

  const currentRef = useRef(current);
  currentRef.current = current;
  const clipOnlyRef = useRef(clipOnly);
  clipOnlyRef.current = clipOnly;

  useEffect(() => {
    const audio = new Audio();
    audio.preload = 'metadata';
    audioRef.current = audio;
    const sync = () => setTime({ time: audio.currentTime, duration: Number.isFinite(audio.duration) ? audio.duration : 0 });
    const onTime = () => {
      sync();
      const item = currentRef.current;
      if (clipOnlyRef.current && item && audio.currentTime >= item.hit.end_sec) audio.pause();
    };
    const onPlay = () => setPlaying(true);
    const onPause = () => setPlaying(false);
    const onWaiting = () => setLoading(true);
    const onReady = () => setLoading(false);
    const onError = () => {
      setLoading(false);
      setPlaying(false);
      setError('לא ניתן לטעון את האודיו של הפרק הזה');
    };
    audio.addEventListener('timeupdate', onTime);
    audio.addEventListener('durationchange', sync);
    audio.addEventListener('play', onPlay);
    audio.addEventListener('pause', onPause);
    audio.addEventListener('ended', onPause);
    audio.addEventListener('waiting', onWaiting);
    audio.addEventListener('playing', onReady);
    audio.addEventListener('canplay', onReady);
    audio.addEventListener('error', onError);
    return () => {
      audio.pause();
      audio.removeAttribute('src');
      audio.removeEventListener('timeupdate', onTime);
      audio.removeEventListener('durationchange', sync);
      audio.removeEventListener('play', onPlay);
      audio.removeEventListener('pause', onPause);
      audio.removeEventListener('ended', onPause);
      audio.removeEventListener('waiting', onWaiting);
      audio.removeEventListener('playing', onReady);
      audio.removeEventListener('canplay', onReady);
      audio.removeEventListener('error', onError);
    };
  }, []);

  const play = useCallback((item: PlayerItem, startAt?: number) => {
    const audio = audioRef.current;
    if (!audio) return;
    const src = audioUrl(item.episode.uuid);
    const target = startAt ?? item.hit.start_sec;
    setError(null);
    setCurrent(item);
    const start = () => {
      audio.currentTime = target;
      audio.play().catch(() => setPlaying(false));
    };
    if (!audio.src.endsWith(src)) {
      setLoading(true);
      audio.src = src;
      audio.addEventListener('loadedmetadata', start, { once: true });
    } else {
      start();
    }
  }, []);

  const toggle = useCallback(() => {
    const audio = audioRef.current;
    const item = currentRef.current;
    if (!audio || !item) return;
    if (audio.paused) {
      // Replaying a finished clip starts it again rather than doing nothing.
      if (clipOnlyRef.current && audio.currentTime >= item.hit.end_sec) audio.currentTime = item.hit.start_sec;
      audio.play().catch(() => setPlaying(false));
    } else {
      audio.pause();
    }
  }, []);

  const seekBy = useCallback((delta: number) => {
    const audio = audioRef.current;
    if (audio) audio.currentTime = Math.max(0, audio.currentTime + delta);
  }, []);

  const index = current ? queue.findIndex((q) => sameHit(current, q.episode, q.hit)) : -1;

  const step = useCallback(
    (dir: 1 | -1) => {
      if (!queue.length) return;
      const nextIndex = index < 0 ? 0 : (index + dir + queue.length) % queue.length;
      play(queue[nextIndex]);
    },
    [queue, index, play],
  );

  const cycleSpeed = useCallback(() => setSpeedIndex((i) => (i + 1) % SPEEDS.length), []);

  useEffect(() => {
    if (audioRef.current) audioRef.current.playbackRate = SPEEDS[speedIndex];
  }, [speedIndex, current]);

  useEffect(() => {
    if (!current || !('mediaSession' in navigator)) return;
    navigator.mediaSession.metadata = new MediaMetadata({ title: current.episode.title, artist: current.episode.source, album: 'ivrit.ai Explore' });
    navigator.mediaSession.setActionHandler('nexttrack', () => step(1));
    navigator.mediaSession.setActionHandler('previoustrack', () => step(-1));
  }, [current, step]);

  const api = useMemo<PlayerApi>(
    () => ({
      current,
      playing,
      loading,
      error,
      speedIndex,
      clipOnly,
      position: { index: index + 1, total: queue.length },
      setQueue: setQueueState,
      play,
      toggle,
      seekBy,
      next: () => step(1),
      prev: () => step(-1),
      cycleSpeed,
      setClipOnly,
      isCurrent: (episode, hit) => sameHit(current, episode, hit),
    }),
    [current, playing, loading, error, speedIndex, clipOnly, index, queue.length, play, toggle, seekBy, step, cycleSpeed],
  );

  return (
    <PlayerContext.Provider value={api}>
      <PlayerTimeContext.Provider value={time}>{children}</PlayerTimeContext.Provider>
    </PlayerContext.Provider>
  );
}

export function usePlayer(): PlayerApi {
  const ctx = useContext(PlayerContext);
  if (!ctx) throw new Error('usePlayer must be used inside <PlayerProvider>');
  return ctx;
}

export function usePlayerTime(): PlayerTime {
  return useContext(PlayerTimeContext);
}
