// React surface for Electrolite. Thin hooks over the existing
// `ShapeClient` so multiple components can share one stream per
// shape and re-render on changes.
//
// Hooks:
//   useShape(url, opts)                    → { data, isLoading, ... }
//   preloadShape(url, opts)                → Promise<void>
//   getShapeStream(url, opts)              → { client, dispose }
//   getShape(url, opts)                    → { rows, subscribe, dispose }
//
// Cache scoping: by default, entries are shared by (url, transport).
// Callers whose `headers` / `onError` / `keyColumns` differ for the
// same URL must pass `cacheKey` to avoid sharing — otherwise the
// first caller's callbacks win and others' are ignored.

import { useCallback, useRef, useSyncExternalStore } from "react";
import { ShapeClient } from "../browser/electrolite.js";

export interface UseShapeOptions {
  transport?: "long-poll" | "sse";
  keyColumns?: string[];
  headers?: () => Record<string, string> | Promise<Record<string, string>>;
  onError?: (error: any, attempt: number) => Promise<unknown> | unknown;
  retry?: { minDelayMs?: number; maxDelayMs?: number };
  fetch?: typeof fetch;
  // Scope override. Two callers with different `headers`/`onError`
  // callbacks for the same URL MUST pass distinct cacheKey values to
  // avoid silently sharing one ShapeClient (and one auth context).
  cacheKey?: string;
}

interface Snapshot {
  rows: any[];
  lastSyncedAt: number | null;
  error: any;
  client: ShapeClient | null;
}

interface CacheEntry {
  key: string;
  client: ShapeClient;
  refCount: number;
  rows: any[];
  lastSyncedAt: number | null;
  error: any;
  subscribers: Set<() => void>;
  unsubscribe: () => void;
  unsubStatus: () => void;
  // Cached snapshot reused across getSnapshot calls when underlying
  // fields are unchanged. Required for useSyncExternalStore to not
  // tear or thrash.
  snapshot: Snapshot;
}

const cache = new Map<string, CacheEntry>();
const EMPTY_SNAPSHOT: Snapshot = Object.freeze({
  rows: Object.freeze([]) as any,
  lastSyncedAt: null,
  error: null,
  client: null,
}) as Snapshot;

function computeCacheKey(url: string, opts: UseShapeOptions): string {
  if (opts.cacheKey) return opts.cacheKey;
  // The transport differs the wire format enough that cached state
  // shouldn't be shared between long-poll and SSE clients.
  return `${opts.transport ?? "long-poll"}::${url}`;
}

function rebuildSnapshot(entry: CacheEntry): void {
  entry.snapshot = {
    rows: entry.rows,
    lastSyncedAt: entry.lastSyncedAt,
    error: entry.error,
    client: entry.client,
  };
}

function acquire(url: string, opts: UseShapeOptions): CacheEntry {
  const key = computeCacheKey(url, opts);
  let entry = cache.get(key);
  if (entry) {
    entry.refCount++;
    return entry;
  }
  const client = new ShapeClient(url, opts as any);
  const rows = client.currentRows();
  const newEntry: CacheEntry = {
    key,
    client,
    refCount: 1,
    rows,
    lastSyncedAt: null,
    error: null,
    subscribers: new Set(),
    unsubscribe: () => {},
    unsubStatus: () => {},
    snapshot: { rows, lastSyncedAt: null, error: null, client },
  };
  cache.set(key, newEntry);
  newEntry.unsubscribe = client.subscribe((nextRows: any[]) => {
    newEntry.rows = nextRows;
    rebuildSnapshot(newEntry);
    for (const cb of newEntry.subscribers) cb();
  });
  newEntry.unsubStatus = client.subscribeStatus((status: any) => {
    if (
      status?.type === "live" ||
      status?.type === "snapshot" ||
      status?.type === "replay"
    ) {
      newEntry.lastSyncedAt = Date.now();
      newEntry.error = null;
    } else if (status?.type === "error") {
      newEntry.error = status.error;
    } else {
      return;
    }
    rebuildSnapshot(newEntry);
    for (const cb of newEntry.subscribers) cb();
  });
  if (typeof (client as any).start === "function") {
    (client as any).start();
  } else {
    (client as any).request({ offset: -1 }).catch((e: any) => {
      newEntry.error = e;
      rebuildSnapshot(newEntry);
      for (const cb of newEntry.subscribers) cb();
    });
  }
  return newEntry;
}

function release(entry: CacheEntry): void {
  entry.refCount--;
  if (entry.refCount <= 0) {
    entry.unsubscribe();
    entry.unsubStatus();
    entry.client.stop();
    cache.delete(entry.key);
  }
}

export function useShape<T = any>(url: string, opts: UseShapeOptions = {}) {
  const key = computeCacheKey(url, opts);
  // Stash the latest opts so subscribe (only re-bound when key changes)
  // can construct ShapeClient with the freshest callbacks.
  const optsRef = useRef(opts);
  optsRef.current = opts;

  const subscribe = useCallback(
    (notify: () => void) => {
      const entry = acquire(url, optsRef.current);
      entry.subscribers.add(notify);
      return () => {
        entry.subscribers.delete(notify);
        release(entry);
      };
    },
    [key, url],
  );

  const getSnapshot = useCallback((): Snapshot => {
    const entry = cache.get(key);
    return entry ? entry.snapshot : EMPTY_SNAPSHOT;
  }, [key]);

  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  return {
    data: snapshot.rows as T[],
    isLoading: snapshot.lastSyncedAt === null && snapshot.error === null,
    lastSyncedAt: snapshot.lastSyncedAt,
    isError: snapshot.error !== null,
    error: snapshot.error,
    shape: snapshot.client,
  };
}

export function getShapeStream(
  url: string,
  opts: UseShapeOptions = {},
): { client: ShapeClient; dispose: () => void } {
  const entry = acquire(url, opts);
  let disposed = false;
  return {
    client: entry.client,
    dispose: () => {
      if (disposed) return;
      disposed = true;
      release(entry);
    },
  };
}

export function getShape(url: string, opts: UseShapeOptions = {}) {
  const entry = acquire(url, opts);
  let disposed = false;
  return {
    get rows() {
      return entry.rows;
    },
    subscribe(callback: (rows: any[]) => void): () => void {
      const cb = () => callback(entry.rows);
      entry.subscribers.add(cb);
      return () => entry.subscribers.delete(cb);
    },
    dispose: () => {
      if (disposed) return;
      disposed = true;
      release(entry);
    },
  };
}

// Preload a shape for SSR/route-loader use. Acquires and DOES NOT
// release — the cache stays warm so a subsequent useShape gets data
// instantly. Returns a `dispose` so callers that want to limit cache
// growth can clean up explicitly.
export async function preloadShape(
  url: string,
  opts: UseShapeOptions = {},
): Promise<{ dispose: () => void }> {
  const entry = acquire(url, opts);
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    release(entry);
  };
  if (entry.lastSyncedAt !== null || entry.error !== null) {
    return { dispose };
  }
  await new Promise<void>((resolve) => {
    const cb = () => {
      if (entry.lastSyncedAt !== null || entry.error !== null) {
        entry.subscribers.delete(cb);
        resolve();
      }
    };
    entry.subscribers.add(cb);
  });
  return { dispose };
}
