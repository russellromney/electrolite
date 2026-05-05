import { JsElectroliteEngine } from "./electrolite-node-js.js";

export const all = () => ({ type: "all" });
export const eq = (column, value) => ({ type: "eq", column, value });
export const inList = (column, values) => ({ type: "in", column, values });
export const and = (predicates) => ({ type: "and", predicates });

export function shape(definition) {
  return { ...definition };
}

export function createElectrolite(options) {
  return new Electrolite(options);
}

export class Electrolite {
  constructor(options = {}) {
    const {
      dbPath,
      shapes = {},
      prefix = "/electrolite/v1",
      replayLimit = 1000,
      liveTimeoutMs = 20_000,
      pollIntervalMs = 250,
    } = options;
    if (!dbPath) {
      throw new Error("createElectrolite requires dbPath");
    }

    this.engine = new JsElectroliteEngine(String(dbPath));
    this.shapes = shapes;
    this.prefix = normalizePrefix(prefix);
    this.replayLimit = replayLimit;
    this.liveTimeoutMs = liveTimeoutMs;
    this.pollIntervalMs = pollIntervalMs;
    this.activeShapes = new Map();
    this.waiters = new Map();
  }

  installTriggers(table) {
    return JSON.parse(this.engine.installTriggersAuto(String(table)));
  }

  installTriggersFor(table, pkColumn) {
    return JSON.parse(this.engine.installTriggers(String(table), String(pkColumn)));
  }

  executeBatch(sql) {
    const offset = this.highWaterMark();
    this.engine.executeBatch(String(sql));
    this.notifyChangedFrom(offset);
  }

  execute(sql, params = []) {
    const offset = this.highWaterMark();
    const rows = this.engine.execute(String(sql), JSON.stringify(params));
    this.notifyChangedFrom(offset);
    return rows;
  }

  writeBatch(statements) {
    const offset = this.highWaterMark();
    const normalized = statements.map(([sql, params = []]) => ({ sql, params }));
    this.engine.writeBatch(JSON.stringify(normalized));
    this.notifyChangedFrom(offset);
  }

  highWaterMark() {
    return this.engine.highWaterMark();
  }

  logId() {
    return this.engine.logId();
  }

  compactLogToLastForTable(tableName, keepLast) {
    return JSON.parse(
      this.engine.compactLogToLastForTable(String(tableName), Number(keepLast)),
    );
  }

  notifyChanged() {
    const waiters = new Set();
    for (const shapeWaiters of this.waiters.values()) {
      for (const waiter of shapeWaiters) {
        waiters.add(waiter);
      }
    }
    this.waiters.clear();
    for (const waiter of waiters) {
      waiter();
    }
  }

  notifyChangedFrom(offset) {
    if (this.activeShapes.size === 0) {
      return;
    }

    const nextOffset = this.highWaterMark();
    if (nextOffset <= offset) {
      return;
    }

    const limit = Math.max(1, nextOffset - offset);
    for (const [shapeHandle, shape] of this.activeShapes) {
      try {
        const replay = JSON.parse(
          this.engine.replay(JSON.stringify(shape), Number(offset), limit),
        );
        if (replay.messages.length > 0) {
          this.notifyShapeChanged(shapeHandle);
        }
      } catch {
        this.notifyShapeChanged(shapeHandle);
      }
    }
  }

  notifyShapeChanged(shapeHandle) {
    const waiters = this.waiters.get(shapeHandle);
    if (!waiters) {
      return;
    }
    this.waiters.delete(shapeHandle);
    for (const waiter of waiters) {
      waiter();
    }
  }

  async fetch(request, context) {
    return this.handle(request, context);
  }

  async handle(request, context) {
    if (request.method !== "GET") {
      return jsonError(405, "method_not_allowed");
    }
    const route = this.parseRequest(request.url);
    if (!route) {
      return jsonError(404, "shape_not_found");
    }
    const definition = this.shapes[route.name];
    if (!definition) {
      return jsonError(404, "shape_not_found");
    }

    const built = await this.buildShape(definition, route, request, context);
    if (!built.allow) {
      return jsonError(404, "shape_not_found");
    }

    if (route.offset >= 0 && route.logId && route.logId !== this.logId()) {
      return jsonError(409, "resync_required");
    }

    if (route.offset < 0) {
      try {
        return jsonResponse(JSON.parse(this.engine.snapshot(JSON.stringify(built.shape))));
      } catch {
        return jsonError(500, "internal_server_error");
      }
    }

    if (route.live) {
      return this.liveResponse(built.shape, route.offset);
    }

    return this.replayResponse(built.shape, route.offset);
  }

  async buildShape(definition, route, request, context) {
    const params = paramsFor(definition.params ?? [], route.path);
    if (!params) {
      return { allow: false };
    }

    const scope = callMaybe(definition.scope, { params, request, context });
    const authScope =
      scope === undefined || scope === null ? route.name : String(await scope);
    const authorized = definition.authorize
      ? await definition.authorize({ params, request, context, scope: authScope })
      : true;
    if (!authorized) {
      return { allow: false };
    }

    const predicate = definition.where
      ? await definition.where({ params, request, context })
      : all();
    return {
      allow: true,
      shape: {
        name: route.path.length > 0 ? `${route.name}/${route.path.join("/")}` : route.name,
        table: definition.table,
        columns: definition.columns,
        predicate,
        auth_scope: authScope,
        schema_version: definition.schemaVersion ?? 1,
      },
    };
  }

  replayResponse(shape, offset) {
    try {
      return jsonResponse(
        JSON.parse(this.engine.replay(JSON.stringify(shape), Number(offset), this.replayLimit)),
      );
    } catch (error) {
      if (isResyncError(error)) {
        return jsonError(409, "resync_required");
      }
      return jsonError(500, "internal_server_error");
    }
  }

  async liveResponse(shape, offset) {
    const shapeHandle = this.engine.shapeHandle(JSON.stringify(shape));
    this.activeShapes.set(shapeHandle, shape);
    try {
      const deadline = Date.now() + this.liveTimeoutMs;
      while (Date.now() <= deadline) {
        let replay;
        try {
          replay = JSON.parse(
            this.engine.replay(JSON.stringify(shape), Number(offset), this.replayLimit),
          );
        } catch (error) {
          if (isResyncError(error)) {
            return jsonError(409, "resync_required");
          }
          return jsonError(500, "internal_server_error");
        }
        if (replay.offset > offset || replay.messages.length > 0) {
          return jsonResponse({
            type: "replay",
            ...replay,
            shape_handle: shapeHandle,
          });
        }

        const waiter = this.nextShapeChange(shapeHandle);
        try {
          await Promise.race([
            waiter.promise,
            sleep(Math.min(this.pollIntervalMs, Math.max(0, deadline - Date.now()))),
          ]);
        } finally {
          waiter.cancel();
        }
      }
      return new Response(null, { status: 204 });
    } finally {
      if (!this.waiters.has(shapeHandle)) {
        this.activeShapes.delete(shapeHandle);
      }
    }
  }

  nextShapeChange(shapeHandle) {
    let waiters = this.waiters.get(shapeHandle);
    if (!waiters) {
      waiters = new Set();
      this.waiters.set(shapeHandle, waiters);
    }
    let resolve;
    const promise = new Promise((done) => {
      resolve = done;
    });
    waiters.add(resolve);
    return {
      promise,
      cancel: () => {
        waiters.delete(resolve);
        if (waiters.size === 0 && this.waiters.get(shapeHandle) === waiters) {
          this.waiters.delete(shapeHandle);
        }
      },
    };
  }

  parseRequest(url) {
    const parsed = new URL(url, "http://electrolite.local");
    if (!parsed.pathname.startsWith(`${this.prefix}/`)) {
      return null;
    }
    const tail = parsed.pathname.slice(this.prefix.length + 1);
    const segments = tail.split("/").filter(Boolean).map(safeDecode);
    if (segments.some((segment) => segment === null) || segments.length === 0) {
      return null;
    }
    const offset = Number(parsed.searchParams.get("offset"));
    if (!Number.isFinite(offset)) {
      return null;
    }
    return {
      name: segments[0],
      path: segments.slice(1),
      offset,
      live: parsed.searchParams.get("live") === "true",
      logId: parsed.searchParams.get("log_id"),
    };
  }
}

function paramsFor(names, values) {
  if (names.length !== values.length) {
    return null;
  }
  return Object.fromEntries(names.map((name, index) => [name, values[index]]));
}

function callMaybe(fn, arg) {
  return typeof fn === "function" ? fn(arg) : fn;
}

function normalizePrefix(prefix) {
  const trimmed = String(prefix).replace(/^\/+|\/+$/g, "");
  return `/${trimmed}`;
}

function safeDecode(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return null;
  }
}

function jsonResponse(body) {
  return Response.json(body);
}

function jsonError(status, error) {
  return Response.json({ error }, { status });
}

function isResyncError(error) {
  const message = String(error?.message ?? error).toLowerCase();
  return (
    message.includes("resync")
    || message.includes("requested offset")
    || message.includes("older than retained offset")
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
