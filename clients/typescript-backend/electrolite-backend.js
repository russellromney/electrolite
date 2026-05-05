const DEFAULT_PREFIX = "/electrolite/v1";
const FORWARDED_REQUEST_HEADERS = new Set(["accept", "if-none-match"]);

export function trustedShapeHeaders(shape) {
  requireShapeField(shape, "name");
  requireShapeField(shape, "table");
  requireShapeField(shape, "auth_scope");
  if (!Array.isArray(shape.columns) || shape.columns.length === 0) {
    throw new Error("trustedShapeHeaders requires non-empty columns");
  }
  if (!shape.predicate || typeof shape.predicate !== "object") {
    throw new Error("trustedShapeHeaders requires predicate");
  }
  if (!Number.isInteger(shape.schema_version) || shape.schema_version < 0) {
    throw new Error("trustedShapeHeaders requires schema_version");
  }

  return {
    "x-electrolite-shape-name": shape.name,
    "x-electrolite-table": shape.table,
    "x-electrolite-columns": JSON.stringify(shape.columns),
    "x-electrolite-predicate": JSON.stringify(shape.predicate),
    "x-electrolite-auth-scope": shape.auth_scope,
    "x-electrolite-schema-version": String(shape.schema_version),
  };
}

export function createElectroliteProxy(options) {
  const {
    origin,
    prefix = DEFAULT_PREFIX,
    authorize,
    fetch: fetchFn = globalThis.fetch,
  } = options ?? {};
  if (!origin) {
    throw new Error("createElectroliteProxy requires origin");
  }
  if (typeof authorize !== "function") {
    throw new Error("createElectroliteProxy requires authorize");
  }
  if (typeof fetchFn !== "function") {
    throw new Error("createElectroliteProxy requires fetch");
  }

  const normalizedPrefix = normalizePrefix(prefix);

  return async function electroliteProxy(request) {
    if (request.method !== "GET") {
      return errorResponse(405, "method_not_allowed");
    }

    const route = parseElectroliteRequest(request.url, { prefix: normalizedPrefix });
    if (!route) {
      return errorResponse(404, "shape_not_found");
    }

    const decision = normalizeDecision(await authorize({ request, ...route }));
    if (!decision.allow) {
      return errorResponse(404, "shape_not_found");
    }

    const target = new URL(route.forwardPath + route.url.search, origin);
    const headers = forwardedHeaders(request.headers);
    applyHeaders(headers, decision.headers);

    return fetchFn(target, {
      method: "GET",
      headers,
      signal: request.signal,
    });
  };
}

export function parseElectroliteRequest(url, options = {}) {
  const prefix = normalizePrefix(options.prefix ?? DEFAULT_PREFIX);
  const parsed = new URL(url, "http://electrolite.local");
  if (!parsed.pathname.startsWith(`${prefix}/`)) {
    return null;
  }

  const forwardPath = parsed.pathname;
  const tail = parsed.pathname.slice(prefix.length + 1);
  const segments = tail.split("/").filter(Boolean);
  const kind = segments[0];
  const name = segments[1];
  if (!name || (kind !== "shape" && kind !== "factory")) {
    return null;
  }
  if (kind === "shape" && segments.length !== 2) {
    return null;
  }

  const offset = Number(parsed.searchParams.get("offset"));
  if (!Number.isFinite(offset)) {
    return null;
  }

  const pathSegments = kind === "factory" ? segments.slice(2) : [];
  const decodedName = safeDecode(name);
  const decodedPath = pathSegments.map(safeDecode);
  if (!decodedName || decodedPath.some((segment) => segment === null)) {
    return null;
  }

  return {
    kind,
    name: decodedName,
    path: decodedPath.join("/"),
    offset,
    live: parsed.searchParams.get("live") === "true",
    url: parsed,
    forwardPath,
  };
}

function forwardedHeaders(source) {
  const headers = new Headers();
  for (const [name, value] of source) {
    if (FORWARDED_REQUEST_HEADERS.has(name.toLowerCase())) {
      headers.set(name, value);
    }
  }
  return headers;
}

function requireShapeField(shape, field) {
  if (!shape || typeof shape[field] !== "string" || shape[field].length === 0) {
    throw new Error(`trustedShapeHeaders requires ${field}`);
  }
}

function normalizeDecision(decision) {
  if (decision === true) {
    return { allow: true, headers: undefined };
  }
  if (!decision || decision === false || decision.allow === false) {
    return { allow: false, headers: undefined };
  }
  return { allow: true, headers: decision.headers };
}

function applyHeaders(target, source) {
  if (!source) {
    return;
  }
  for (const [name, value] of new Headers(source)) {
    target.set(name, value);
  }
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

function errorResponse(status, error) {
  return new Response(JSON.stringify({ error }), {
    status,
    headers: {
      "content-type": "application/json",
    },
  });
}
