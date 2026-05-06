// LocalMutationBuffer — a small helper for Pattern B / C of
// docs/optimistic-writes.md.
//
// Stages local mutations so the UI can show them before the server
// confirms. As confirmed rows arrive (via the next ShapeClient
// replay), the buffer removes them from its pending set.
//
// Usage:
//   const buffer = new LocalMutationBuffer("todos:p1");
//   buffer.stage({ id: "abc", title: "first" });
//   const merged = buffer.merge(serverRows);

export class LocalMutationBuffer {
  constructor(name, options = {}) {
    this.name = name;
    this.storage = options.storage ?? null;
    this.retry = options.retry ?? null;
    this.keyField = options.keyField ?? "id";
    this.pending = new Map();
    if (this.storage) {
      const raw = this.storage.getItem(this._storageKey());
      if (raw) {
        try {
          for (const [k, v] of JSON.parse(raw)) {
            this.pending.set(k, v);
          }
        } catch {
          // Corrupt storage; start clean.
        }
      }
    }
    this.subscribers = new Set();
  }

  _storageKey() {
    return `electrolite:mutations:${this.name}`;
  }

  _persist() {
    if (this.storage) {
      this.storage.setItem(
        this._storageKey(),
        JSON.stringify([...this.pending.entries()]),
      );
    }
  }

  stage(row) {
    const key = row[this.keyField];
    if (key === undefined) {
      throw new Error(
        `LocalMutationBuffer requires '${this.keyField}' on staged rows`,
      );
    }
    this.pending.set(key, row);
    this._persist();
    this._notify();
  }

  rollback(key) {
    if (this.pending.delete(key)) {
      this._persist();
      this._notify();
    }
  }

  /**
   * Merge the buffer's pending rows with the authoritative
   * server-side rows. Pending rows are dropped as soon as they
   * appear in the server set (the server has confirmed them).
   */
  merge(serverRows) {
    if (this.pending.size === 0) {
      return serverRows;
    }
    const serverKeys = new Set();
    for (const row of serverRows) {
      serverKeys.add(row[this.keyField]);
    }
    let dropped = 0;
    for (const key of [...this.pending.keys()]) {
      if (serverKeys.has(key)) {
        this.pending.delete(key);
        dropped++;
      }
    }
    if (dropped > 0) {
      this._persist();
    }
    if (this.pending.size === 0) {
      return serverRows;
    }
    return [...serverRows, ...this.pending.values()];
  }

  /**
   * Replay any unconfirmed mutations through the user's `retry`
   * function. Useful on reconnect after a reload.
   */
  async replay() {
    if (!this.retry) return;
    for (const [, row] of this.pending) {
      try {
        await this.retry(row);
      } catch {
        // Leave it in the buffer; next replay() retries.
      }
    }
  }

  subscribe(callback) {
    this.subscribers.add(callback);
    return () => this.subscribers.delete(callback);
  }

  _notify() {
    for (const cb of this.subscribers) cb([...this.pending.values()]);
  }
}
