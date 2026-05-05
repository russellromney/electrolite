export class ShapeClient {
  constructor(url, options = {}) {
    const { keyColumns, fetch: fetchFn = globalThis.fetch, live = true } = options;
    if (!Array.isArray(keyColumns) || keyColumns.length === 0) {
      throw new Error("ShapeClient requires keyColumns");
    }
    if (typeof fetchFn !== "function") {
      throw new Error("ShapeClient requires fetch");
    }

    this.url = url;
    this.keyColumns = keyColumns;
    this.fetch = fetchFn;
    this.live = live;
    this.offset = -1;
    this.rows = new Map();
    this.subscribers = new Set();
    this.stopped = false;
  }

  subscribe(callback) {
    this.subscribers.add(callback);
    callback(this.currentRows());
    return () => this.subscribers.delete(callback);
  }

  stop() {
    this.stopped = true;
  }

  currentRows() {
    return Array.from(this.rows.values());
  }

  async start() {
    await this.request({ offset: -1 });

    while (this.live && !this.stopped) {
      await this.request({ offset: this.offset, live: true });
    }
  }

  async request(params) {
    const response = await this.fetch(this.requestUrl(params));
    if (response.status === 204) {
      return false;
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
      this.rows.clear();
      for (const row of body.rows) {
        this.rows.set(this.keyForRow(row), row);
      }
      this.offset = body.offset;
      this.notify();
      return true;
    }

    if (body.type === "replay") {
      let changed = false;
      for (const message of body.messages) {
        changed = this.applyMessage(message) || changed;
      }
      this.offset = body.offset;
      if (changed) {
        this.notify();
      }
      return changed;
    }

    throw new Error(`Unknown Electrolite response type: ${body.type}`);
  }

  applyMessage(message) {
    const key = JSON.stringify(message.key);
    if (message.type === "delete") {
      return this.rows.delete(key);
    }
    if (message.type === "insert" || message.type === "update") {
      this.rows.set(key, message.value);
      return true;
    }
    throw new Error(`Unknown Electrolite message type: ${message.type}`);
  }

  keyForRow(row) {
    const key = {};
    for (const column of this.keyColumns) {
      key[column] = row[column];
    }
    return JSON.stringify(key);
  }

  notify() {
    const rows = this.currentRows();
    for (const subscriber of this.subscribers) {
      subscriber(rows);
    }
  }
}
