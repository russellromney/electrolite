// React surface for Electrolite. Thin hooks over the existing
// `ShapeClient` so multiple components can share one stream per
// shape and re-render on changes.
//
// Hooks:
//   useShape(url, opts)   → { data, isLoading, lastSyncedAt, isError, error }
//   preloadShape(url, opts) → Promise<void>      // for route loaders
//   getShapeStream(url, opts) → ShapeClient      // memoized
//   getShape(url, opts) → { rows, subscribe }    // materialized view

import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import { ShapeClient } from "../browser/electrolite.js";

export interface UseShapeOptions {
  transport?: "long-poll" | "sse";
  keyColumns?: string[];
  headers?: () => Record<string, string> | Promise<Record<string, string>>;
  onError?: (error: any, attempt: number) => Promise<unknown> | unknown;
  retry?: { minDelayMs?: number; maxDelayMs?: number };
  fetch?: typeof fetch;
}

interface CacheEntry {
  client: ShapeClient;
  refCount: number;
  rows: any[];
  lastSyncedAt: number | null;
  error: any;
  subscribers: Set<() => void>;
  unsubscribe: () => void;
  unsubStatus: () => void;
}

const cache = new Map<string, CacheEntry>();

function cacheKey(url: string, opts: UseShapeOptions): string {
  // The transport differs the wire format enough that cached state
  // shouldn't be shared between long-poll and SSE clients.
  return `${opts.transport ?? "long-poll"}::${url}`;
}

function acquire(url: string, opts: UseShapeOptions): CacheEntry {
  const key = cacheKey(url, opts);
  let entry = cache.get(key);
  if (entry) {
    entry.refCount++;
    return entry;
  }
  const client = new ShapeClient(url, opts as any);
  const entryNew: CacheEntry = {
    client,
    refCount: 1,
    rows: client.currentRows(),
    lastSyncedAt: null,
    error: null,
    subscribers: new Set(),
    unsubscribe: () => {},
    unsubStatus: () => {},
  };
  cache.set(key, entryNew);
  entryNew.unsubscribe = client.subscribe((rows: any[]) => {
    entryNew.rows = rows;
    for (const cb of entryNew.subscribers) cb();
  });
  entryNew.unsubStatus = client.subscribeStatus((status: any) => {
    if (status?.type === "live" || status?.type === "snapshot" || status?.type === "replay") {
      entryNew.lastSyncedAt = Date.now();
      entryNew.error = null;
    } else if (status?.type === "error") {
      entryNew.error = status.error;
    }
    for (const cb of entryNew.subscribers) cb();
  });
  // Kick off the initial request loop. ShapeClient.start() is the
  // lifecycle entry; if it doesn't exist, fall back to a manual
  // snapshot.
  if (typeof (client as any).start === "function") {
    (client as any).start();
  } else {
    // best-effort
    (client as any).request({ offset: -1 }).catch((e: any) => {
      entryNew.error = e;
      for (const cb of entryNew.subscribers) cb();
    });
  }
  return entryNew;
}

function release(url: string, opts: UseShapeOptions) {
  const key = cacheKey(url, opts);
  const entry = cache.get(key);
  if (!entry) return;
  entry.refCount--;
  if (entry.refCount <= 0) {
    entry.unsubscribe();
    entry.unsubStatus();
    entry.client.stop();
    cache.delete(key);
  }
}

export function useShape<T = any>(url: string, opts: UseShapeOptions = {}) {
  // Acquire on mount, release on unmount, share by cacheKey.
  const entryRef = useRef<CacheEntry | null>(null);
  if (!entryRef.current) {
    entryRef.current = acquire(url, opts);
  }
  const entry = entryRef.current;

  const [, force] = useState(0);
  useEffect(() => {
    const cb = () => force((n) => n + 1);
    entry.subscribers.add(cb);
    return () => {
      entry.subscribers.delete(cb);
      release(url, opts);
      entryRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cacheKey(url, opts)]);

  return {
    data: entry.rows as T[],
    isLoading: entry.lastSyncedAt === null && entry.error === null,
    lastSyncedAt: entry.lastSyncedAt,
    isError: entry.error !== null,
    error: entry.error,
    shape: entry.client,
  };
}

export function getShapeStream(url: string, opts: UseShapeOptions = {}): ShapeClient {
  return acquire(url, opts).client;
}

export function getShape(url: string, opts: UseShapeOptions = {}) {
  const entry = acquire(url, opts);
  return {
    get rows() {
      return entry.rows;
    },
    subscribe(callback: (rows: any[]) => void): () => void {
      const cb = () => callback(entry.rows);
      entry.subscribers.add(cb);
      return () => entry.subscribers.delete(cb);
    },
  };
}

export async function preloadShape(url: string, opts: UseShapeOptions = {}): Promise<void> {
  const entry = acquire(url, opts);
  // Wait for the first sync (snapshot or error).
  if (entry.lastSyncedAt !== null || entry.error !== null) {
    return;
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
}
