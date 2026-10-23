import { useCallback, useEffect, useState } from 'react';
import { ApiError } from '../api/client';

interface AsyncState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  reload: () => void;
}

/** Runs `fn` whenever `key` changes, aborting the previous request. */
export function useAsync<T>(fn: ((signal: AbortSignal) => Promise<T>) | null, key: string): AsyncState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    if (!fn) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fn(controller.signal)
      .then((result) => {
        if (!controller.signal.aborted) setData(result);
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted) return;
        setData(null);
        if (err instanceof ApiError) {
          setError(err.message);
        } else {
          // Network failure or a bug: show a retryable message, keep the cause visible.
          console.error(err);
          setError('החיבור לשרת נכשל. נסו שוב.');
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
    // `fn` is recreated each render; `key` captures what it depends on.
  }, [key, nonce]);

  const reload = useCallback(() => setNonce((n) => n + 1), []);
  return { data, error, loading, reload };
}
