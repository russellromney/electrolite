export class ShapeClient {
  constructor(url, options = {}) {
    const {
      keyColumns,
      fetch: fetchFn = globalThis.fetch,
      live = true,
      retry = {},
    } = options;
    if (keyColumns !== undefined && (!Array.isArray(keyColumns) || keyColumns.length === 0)) {
      throw new Error("ShapeClient keyColumns must be a non-empty array");
    }
    if (typeof fetchFn !== "function") {
      throw new Error("ShapeClient requires fetch");
    }

    this.url = url;
    this.keyColumns = keyColumns ?? null;
    this.fetch = fetchFn;
    this.live = live;
    this.retryMinDelayMs = retry.minDelayMs ?? 250;
    this.retryMaxDelayMs = retry.maxDelayMs ?? 5_000;
    this.offset = -1;
    this.rows = new Map();
    this.pendingRows = null;
    this.pendingChanged = false;
    this.subscribers = new Set();
    this.statusSubscribers = new Set();
    this.stopped = false;
    this.abortController = null;
  }

  subscribe(callback) {
    this.subscribers.add(callback);
    callback(this.currentRows());
    return () => this.subscribers.delete(callback);
  }

  subscribeStatus(callback) {
    this.statusSubscribers.add(callback);
    callback({ type: "idle", offset: this.offset });
    return () => this.statusSubscribers.delete(callback);
  }

  stop() {
    this.stopped = true;
    this.abortController?.abort();
  }

  currentRows() {
    return Array.from(this.rows.values());
  }

  async start() {
    this.stopped = false;
    let delay = this.retryMinDelayMs;

    while (!this.stopped) {
      try {
        if (this.offset < 0) {
          await this.request({ offset: -1 });
        } else if (!this.live) {
          return;
        } else {
          await this.request({ offset: this.offset, live: true });
        }
        delay = this.retryMinDelayMs;
      } catch (error) {
        if (this.stopped || error?.name === "AbortError") {
          return;
        }
        this.notifyStatus({ type: "error", error, offset: this.offset });
        await this.sleep(delay);
        delay = Math.min(delay * 2, this.retryMaxDelayMs);
      }
    }
  }

  async request(params) {
    this.abortController = new AbortController();
    this.notifyStatus({
      type: params.live ? "live" : params.offset < 0 ? "snapshot" : "replay",
      offset: params.offset,
    });
    let response;
    try {
      response = await this.fetch(this.requestUrl(params), {
        signal: this.abortController.signal,
      });
    } finally {
      this.abortController = null;
    }
    if (response.status === 204) {
      this.notifyStatus({ type: "timeout", offset: this.offset });
      return false;
    }
    if (response.status === 409) {
      this.notifyStatus({ type: "resync_required", offset: this.offset });
      this.offset = -1;
      this.rows.clear();
      this.notify();
      return this.request({ offset: -1 });
    }
    if (!response.ok) {
      throw new Error(`Electrolite request failed: ${response.status}`);
    }

    const body = await response.json();
    return this.apply(body);
  }

  requestUrl(params) {
    const url = new URL(this.url, globalThis.location?.href);
    url.searchParams.set("offset", String(params.offset));
    if (params.live) {
      url.searchParams.set("live", "true");
    }
    return url;
  }

  apply(body) {
    if (body.type === "snapshot") {
      if (Array.isArray(body.key_columns) && body.key_columns.length > 0) {
        this.keyColumns = body.key_columns;
      }
      this.requireKeyColumns();
      this.rows.clear();
      this.pendingRows = null;
      this.pendingChanged = false;
      for (const row of body.rows) {
        this.rows.set(this.keyForRow(row), row);
      }
      this.offset = body.offset;
      this.notify();
      this.notifyStatus({ type: "ready", offset: this.offset });
      return true;
    }

    if (body.type === "replay") {
      let changed = false;
      const nextRows = new Map(this.pendingRows ?? this.rows);
      for (const message of body.messages) {
        changed = this.applyMessageTo(nextRows, message) || changed;
      }
      this.offset = body.offset;
      if (body.up_to_date === false) {
        this.pendingRows = nextRows;
        this.pendingChanged = this.pendingChanged || changed;
        this.notifyStatus({ type: "replay", offset: this.offset });
        return false;
      }

      this.rows = nextRows;
      changed = this.pendingChanged || changed;
      this.pendingRows = null;
      this.pendingChanged = false;
      if (changed) {
        this.notify();
      }
      this.notifyStatus({ type: "ready", offset: this.offset });
      return changed;
    }

    throw new Error(`Unknown Electrolite response type: ${body.type}`);
  }

  applyMessage(message) {
    return this.applyMessageTo(this.rows, message);
  }

  applyMessageTo(rows, message) {
    const key = JSON.stringify(message.key);
    if (message.type === "delete") {
      return rows.delete(key);
    }
    if (message.type === "insert" || message.type === "update") {
      rows.set(key, message.value);
      return true;
    }
    throw new Error(`Unknown Electrolite message type: ${message.type}`);
  }

  keyForRow(row) {
    this.requireKeyColumns();
    const key = {};
    for (const column of this.keyColumns) {
      key[column] = row[column];
    }
    return JSON.stringify(key);
  }

  requireKeyColumns() {
    if (!Array.isArray(this.keyColumns) || this.keyColumns.length === 0) {
      throw new Error("ShapeClient requires key_columns in snapshot or keyColumns option");
    }
  }

  notify() {
    const rows = this.currentRows();
    for (const subscriber of this.subscribers) {
      subscriber(rows);
    }
  }

  notifyStatus(status) {
    for (const subscriber of this.statusSubscribers) {
      subscriber(status);
    }
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
