export class ShapeClient {
  constructor(url, options = {}) {
    const {
      keyColumns,
      fetch: fetchFn = globalThis.fetch?.bind(globalThis),
      live = true,
      retry = {},
      persist = false,
      multiTab = false,
      channelFactory,
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
    this.logId = null;
    this.shapeHandle = null;
    this.offset = -1;
    this.needsReplay = false;
    this.resyncRequired = false;
    this.rows = new Map();
    this.pendingRows = null;
    this.pendingChanged = false;
    this.subscribers = new Set();
    this.statusSubscribers = new Set();
    this.eventSubscribers = new Set();
    this.stopped = false;
    this.abortController = null;
    this.storage = storageFromOption(persist, this.url);
    this.clientId = randomId();
    this.leaderKey = `electrolite:leader:${stableString(this.url)}`;
    this.leaderTtlMs = 5_000;
    this.channel = null;
    this.channelHandler = null;
    this.visibilityHandler = null;
    this.lifecycleHandler = null;
    this.multiTab = Boolean(multiTab);
    if (this.multiTab) {
      const makeChannel =
        channelFactory ??
        ((name) => new globalThis.BroadcastChannel(name));
      this.channel = makeChannel(`electrolite:${stableString(this.url)}`);
      this.channelHandler = (event) => this.onChannelMessage(event.data);
      if (this.channel.addEventListener) {
        this.channel.addEventListener("message", this.channelHandler);
      } else {
        this.channel.onmessage = this.channelHandler;
      }
      if (globalThis.document?.addEventListener) {
        this.visibilityHandler = () => {
          if (globalThis.document.visibilityState === "visible") {
            this.canUseNetwork();
          }
        };
        globalThis.document.addEventListener("visibilitychange", this.visibilityHandler);
      }
      if (globalThis.addEventListener) {
        this.lifecycleHandler = () => this.releaseLeadership();
        globalThis.addEventListener("pagehide", this.lifecycleHandler);
        globalThis.addEventListener("beforeunload", this.lifecycleHandler);
      }
    }
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

  subscribeEvents(callback) {
    this.eventSubscribers.add(callback);
    return () => this.eventSubscribers.delete(callback);
  }

  stop() {
    this.stopped = true;
    this.abortController?.abort();
    this.releaseLeadership();
    if (this.channel && this.channelHandler) {
      this.channel.removeEventListener?.("message", this.channelHandler);
      if (this.channel.onmessage === this.channelHandler) {
        this.channel.onmessage = null;
      }
      this.channel.close?.();
    }
    if (this.visibilityHandler) {
      globalThis.document?.removeEventListener?.("visibilitychange", this.visibilityHandler);
      this.visibilityHandler = null;
    }
    if (this.lifecycleHandler) {
      globalThis.removeEventListener?.("pagehide", this.lifecycleHandler);
      globalThis.removeEventListener?.("beforeunload", this.lifecycleHandler);
      this.lifecycleHandler = null;
    }
  }

  currentRows() {
    return Array.from(this.rows.values());
  }

  async start() {
    this.stopped = false;
    let delay = this.retryMinDelayMs;
    await this.hydrate();

    while (!this.stopped) {
      try {
        if (this.multiTab && !this.canUseNetwork()) {
          this.notifyStatus({ type: "standby", offset: this.offset });
          await this.sleep(Math.min(this.retryMinDelayMs, 500));
          continue;
        }
        if (this.offset < 0) {
          await this.request({ offset: -1 });
        } else if (this.needsReplay) {
          await this.request({ offset: this.offset });
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
    this.renewLeadership();
    this.abortController = new AbortController();
    this.notifyStatus({
      type: params.live ? "live" : params.offset < 0 ? "snapshot" : "replay",
      offset: params.offset,
    });
    let response;
    const heartbeat = params.live ? this.startLeadershipHeartbeat() : null;
    try {
      response = await this.fetch(this.requestUrl(params), {
        signal: this.abortController.signal,
      });
    } finally {
      heartbeat?.();
      this.abortController = null;
    }
    if (response.status === 204) {
      this.notifyStatus({ type: "timeout", offset: this.offset });
      return false;
    }
    if (response.status === 409) {
      this.notifyStatus({ type: "resync_required", offset: this.offset });
      await this.resetLocalState();
      return this.request({ offset: -1 });
    }
    if (!response.ok) {
      throw new Error(`Electrolite request failed: ${response.status}`);
    }

    const body = await response.json();
    const changed = this.apply(body);
    if (this.resyncRequired) {
      this.resyncRequired = false;
      await this.clearPersistence();
      return this.request({ offset: -1 });
    }
    return changed;
  }

  requestUrl(params) {
    const url = new URL(this.url, globalThis.location?.href);
    url.searchParams.set("offset", String(params.offset));
    if (params.live) {
      url.searchParams.set("live", "true");
    }
    if (this.logId && params.offset >= 0) {
      url.searchParams.set("log_id", this.logId);
    }
    return url;
  }

  apply(body) {
    if (body.type === "snapshot") {
      this.resyncRequired = false;
      this.logId = body.log_id ?? null;
      this.shapeHandle = body.shape_handle ?? null;
      this.needsReplay = false;
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
      this.emitEvent({ type: "snapshot", offset: this.offset, rows: this.currentRows() });
      this.emitEvent({ type: "offset", offset: this.offset });
      this.persistState();
      this.broadcast({ type: "state", body });
      this.notifyStatus({ type: "ready", offset: this.offset });
      return true;
    }

    if (body.type === "replay") {
      if (body.log_id && this.logId && body.log_id !== this.logId) {
        this.dropLocalState();
        this.resyncRequired = true;
        this.notifyStatus({ type: "resync_required", offset: this.offset });
        return false;
      }
      if (body.log_id) {
        this.logId = body.log_id;
      }
      if (body.shape_handle && this.shapeHandle && body.shape_handle !== this.shapeHandle) {
        this.dropLocalState();
        this.resyncRequired = true;
        this.notifyStatus({ type: "resync_required", offset: this.offset });
        return false;
      }
      if (body.shape_handle) {
        this.shapeHandle = body.shape_handle;
      }
      let changed = false;
      const nextRows = new Map(this.pendingRows ?? this.rows);
      for (const message of body.messages) {
        changed = this.applyMessageTo(nextRows, message) || changed;
        this.emitEvent({ type: message.type, offset: message.offset, message });
      }
      this.offset = body.offset;
      this.emitEvent({ type: "replay", offset: this.offset, messages: body.messages });
      this.emitEvent({ type: "offset", offset: this.offset });
      if (body.up_to_date === false) {
        this.needsReplay = true;
        this.pendingRows = nextRows;
        this.pendingChanged = this.pendingChanged || changed;
        this.notifyStatus({ type: "replay", offset: this.offset });
        return false;
      }

      this.needsReplay = false;
      this.rows = nextRows;
      changed = this.pendingChanged || changed;
      this.pendingRows = null;
      this.pendingChanged = false;
      if (changed) {
        this.notify();
      }
      this.persistState();
      this.broadcast({ type: "state", body });
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

  emitEvent(event) {
    for (const subscriber of this.eventSubscribers) {
      subscriber(event);
    }
  }

  async hydrate() {
    if (!this.storage || this.offset >= 0) {
      return false;
    }
    const state = await this.storage.load();
    if (!state) {
      return false;
    }
    if (state.offset >= 0 && !state.logId) {
      await this.clearPersistence();
      return false;
    }
    if (state.offset >= 0 && !state.shapeHandle) {
      await this.clearPersistence();
      return false;
    }
    if (Array.isArray(state.keyColumns)) {
      this.keyColumns = state.keyColumns;
    }
    this.logId = state.logId ?? null;
    this.shapeHandle = state.shapeHandle ?? null;
    this.offset = state.offset ?? -1;
    this.needsReplay = true;
    this.pendingRows = new Map((state.rows ?? []).map((row) => [this.keyForRow(row), row]));
    this.pendingChanged = true;
    this.emitEvent({
      type: "hydrate",
      offset: this.offset,
      row_count: this.pendingRows.size,
    });
    this.notifyStatus({ type: "validating_cache", offset: this.offset });
    return true;
  }

  persistState() {
    if (!this.storage) {
      return;
    }
    const state = {
      keyColumns: this.keyColumns,
      logId: this.logId,
      shapeHandle: this.shapeHandle,
      offset: this.offset,
      rows: this.currentRows(),
    };
    Promise.resolve(this.storage.save(state)).catch((error) => {
      this.notifyStatus({ type: "persistence_error", error, offset: this.offset });
    });
  }

  async clearPersistence() {
    if (!this.storage?.clear) {
      return;
    }
    await this.storage.clear();
  }

  async resetLocalState() {
    this.dropLocalState();
    await this.clearPersistence();
  }

  dropLocalState() {
    this.logId = null;
    this.shapeHandle = null;
    this.offset = -1;
    this.needsReplay = false;
    this.rows.clear();
    this.pendingRows = null;
    this.pendingChanged = false;
    this.notify();
  }

  broadcast(message) {
    if (!this.channel) {
      return;
    }
    this.channel.postMessage({ ...message, clientId: this.clientId });
  }

  onChannelMessage(message) {
    if (!message || message.clientId === this.clientId) {
      return;
    }
    if (message.type === "state") {
      this.applyRemote(message.body);
    }
  }

  applyRemote(body) {
    const previousChannel = this.channel;
    this.channel = null;
    try {
      this.apply(body);
      this.emitEvent({ type: "remote_apply", offset: this.offset });
    } finally {
      this.channel = previousChannel;
    }
  }

  canUseNetwork() {
    if (!this.multiTab) {
      return true;
    }
    if (!globalThis.localStorage) {
      return true;
    }
    const now = Date.now();
    const current = parseJson(globalThis.localStorage.getItem(this.leaderKey));
    if (!current || current.expiresAt <= now || current.clientId === this.clientId) {
      globalThis.localStorage.setItem(
        this.leaderKey,
        JSON.stringify({ clientId: this.clientId, expiresAt: now + this.leaderTtlMs }),
      );
      return true;
    }
    return false;
  }

  renewLeadership() {
    if (!this.multiTab || !globalThis.localStorage) {
      return;
    }
    const current = parseJson(globalThis.localStorage.getItem(this.leaderKey));
    if (current?.clientId === this.clientId) {
      globalThis.localStorage.setItem(
        this.leaderKey,
        JSON.stringify({ clientId: this.clientId, expiresAt: Date.now() + this.leaderTtlMs }),
      );
    }
  }

  startLeadershipHeartbeat() {
    if (!this.multiTab || !globalThis.setInterval || !globalThis.clearInterval) {
      return null;
    }
    const interval = globalThis.setInterval(
      () => this.renewLeadership(),
      Math.max(250, Math.floor(this.leaderTtlMs / 2)),
    );
    return () => globalThis.clearInterval(interval);
  }

  releaseLeadership() {
    if (!this.multiTab || !globalThis.localStorage) {
      return;
    }
    const current = parseJson(globalThis.localStorage.getItem(this.leaderKey));
    if (current?.clientId === this.clientId) {
      globalThis.localStorage.removeItem(this.leaderKey);
    }
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

export class IndexedDbShapeStorage {
  constructor(name) {
    this.name = name;
    this.dbName = "electrolite";
    this.storeName = "shapes";
  }

  async load() {
    const db = await this.open();
    return requestToPromise(db.transaction(this.storeName).objectStore(this.storeName).get(this.name));
  }

  async save(state) {
    const db = await this.open();
    const tx = db.transaction(this.storeName, "readwrite");
    tx.objectStore(this.storeName).put(state, this.name);
    await transactionDone(tx);
  }

  async clear() {
    const db = await this.open();
    const tx = db.transaction(this.storeName, "readwrite");
    tx.objectStore(this.storeName).delete(this.name);
    await transactionDone(tx);
  }

  open() {
    if (!globalThis.indexedDB) {
      return Promise.reject(new Error("IndexedDB is not available"));
    }
    const request = globalThis.indexedDB.open(this.dbName, 1);
    request.onupgradeneeded = () => {
      request.result.createObjectStore(this.storeName);
    };
    return requestToPromise(request);
  }
}

function storageFromOption(persist, url) {
  if (!persist) {
    return null;
  }
  if (persist === true) {
    return new IndexedDbShapeStorage(stableString(url));
  }
  return persist;
}

function requestToPromise(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function transactionDone(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}

function stableString(value) {
  return String(value).replace(/[^a-zA-Z0-9_.:-]/g, "_");
}

function randomId() {
  return globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random()}`;
}

function parseJson(value) {
  try {
    return value ? JSON.parse(value) : null;
  } catch {
    return null;
  }
}
