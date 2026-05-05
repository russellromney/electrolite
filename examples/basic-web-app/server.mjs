import { createServer } from "node:http";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  createElectrolite,
  eq,
  shape,
} from "../../packages/electrolite-node/electrolite-node.js";

const root = fileURLToPath(new URL("../..", import.meta.url));
const dir = mkdtempSync(join(tmpdir(), "electrolite-web-"));
const dbPath = join(dir, "app.db");
let nextId = 3;

const electrolite = createElectrolite({
  dbPath,
  liveTimeoutMs: 20_000,
  pollIntervalMs: 1_000,
  shapes: {
    projectTodos: shape({
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      params: ["projectId"],
      where: ({ params }) => eq("project_id", params.projectId),
      scope: ({ params }) => `project:${params.projectId}`,
      authorize: ({ params, context }) => context.projectIds.has(params.projectId),
    }),
  },
});

electrolite.executeBatch(`
  CREATE TABLE todos (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL,
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL DEFAULT 0
  );
`);
electrolite.installTriggers("todos");
electrolite.executeBatch(`
  INSERT INTO todos (id, project_id, title, done) VALUES
    (1, 'launch', 'Write the launch plan', 0),
    (2, 'launch', 'Invite beta users', 0);
`);

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? "/", "http://localhost:3000");

    if (req.method === "GET" && url.pathname === "/") {
      return send(res, 200, "text/html", html);
    }

    if (req.method === "GET" && url.pathname === "/app.js") {
      return send(res, 200, "text/javascript", appJs);
    }

    if (req.method === "GET" && url.pathname === "/electrolite-browser-client.js") {
      const client = readFileSync(join(root, "clients/browser/electrolite.js"), "utf8");
      return send(res, 200, "text/javascript", client);
    }

    if (url.pathname.startsWith("/electrolite/v1/")) {
      const response = await electrolite.handle(toWebRequest(req), {
        projectIds: new Set(["launch"]),
      });
      return sendWebResponse(res, response);
    }

    if (req.method === "POST" && url.pathname === "/api/todos") {
      const body = await readJson(req);
      const title = String(body.title ?? "").trim() || `Todo ${nextId}`;
      const id = nextId;
      electrolite.execute(
        "INSERT INTO todos (id, project_id, title, done) VALUES (?1, 'launch', ?2, 0)",
        [id, title],
      );
      nextId += 1;
      return sendJson(res, 201, { ok: true, id, title });
    }

    const todoMatch = url.pathname.match(/^\/api\/todos\/(\d+)$/);
    if (todoMatch && req.method === "PATCH") {
      const id = Number(todoMatch[1]);
      const body = await readJson(req);
      const title = String(body.title ?? "").trim() || `Renamed todo ${id}`;
      electrolite.execute(
        "UPDATE todos SET title = ?1 WHERE id = ?2",
        [title, id],
      );
      return sendJson(res, 200, { ok: true, id, title });
    }

    if (todoMatch && req.method === "DELETE") {
      const id = Number(todoMatch[1]);
      electrolite.execute("DELETE FROM todos WHERE id = ?1", [id]);
      return sendJson(res, 200, { ok: true, id });
    }

    if (req.method === "POST" && url.pathname === "/api/batch") {
      const firstId = nextId;
      const secondId = nextId + 1;
      nextId += 2;
      electrolite.writeBatch([
        [
          "INSERT INTO todos (id, project_id, title, done) VALUES (?1, 'launch', ?2, 0)",
          [firstId, `Batched todo ${firstId}`],
        ],
        [
          "INSERT INTO todos (id, project_id, title, done) VALUES (?1, 'launch', ?2, 0)",
          [secondId, `Batched todo ${secondId}`],
        ],
      ]);
      return sendJson(res, 201, {
        ok: true,
        inserted: [firstId, secondId],
      });
    }

    return sendJson(res, 404, { error: "not_found" });
  } catch (error) {
    console.error(error);
    return sendJson(res, 500, { error: "internal_server_error" });
  }
});

server.listen(3000, () => {
  console.log("Electrolite basic web app: http://localhost:3000");
  console.log(`SQLite database: ${dbPath}`);
});

process.on("SIGINT", () => {
  server.close(() => {
    rmSync(dir, { recursive: true, force: true });
    process.exit(0);
  });
});

function toWebRequest(req) {
  return new Request(`http://localhost:3000${req.url}`, {
    method: req.method,
    headers: req.headers,
  });
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  if (chunks.length === 0) {
    return {};
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function sendWebResponse(res, response) {
  const body = response.status === 204 ? "" : await response.text();
  response.headers.forEach((value, key) => res.setHeader(key, value));
  res.statusCode = response.status;
  res.end(body);
}

function sendJson(res, status, body) {
  send(res, status, "application/json", JSON.stringify(body));
}

function send(res, status, contentType, body) {
  res.statusCode = status;
  res.setHeader("content-type", contentType);
  res.end(body);
}

const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Electrolite Basic Web App</title>
    <style>
      :root {
        color-scheme: light;
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: #f5f7f8;
        color: #1f2933;
      }
      body {
        margin: 0;
      }
      main {
        width: min(980px, calc(100vw - 32px));
        margin: 48px auto;
      }
      h1 {
        margin: 0 0 8px;
        font-size: 28px;
        letter-spacing: 0;
      }
      p {
        margin: 0 0 24px;
        color: #52606d;
      }
      .columns {
        display: grid;
        grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
        gap: 16px;
        align-items: start;
      }
      section {
        min-height: 320px;
        background: white;
        border: 1px solid #d9e2ec;
        border-radius: 8px;
        padding: 16px;
      }
      h2 {
        margin: 0 0 8px;
        font-size: 16px;
        letter-spacing: 0;
      }
      .hint {
        min-height: 44px;
        margin: 0 0 16px;
        font-size: 14px;
        line-height: 1.45;
      }
      form {
        display: grid;
        gap: 10px;
        margin: 0;
      }
      .button-row {
        display: flex;
        gap: 8px;
      }
      input, button {
        height: 40px;
        border-radius: 6px;
        font: inherit;
      }
      input {
        border: 1px solid #bcccdc;
        padding: 0 12px;
        background: white;
      }
      button {
        border: 0;
        padding: 0 14px;
        background: #0f766e;
        color: white;
        cursor: pointer;
      }
      button.secondary {
        background: #334e68;
      }
      button.danger {
        background: #b42318;
      }
      button.small {
        height: 32px;
        padding: 0 10px;
        font-size: 13px;
      }
      ul {
        list-style: none;
        padding: 0;
        margin: 0;
        display: grid;
        gap: 8px;
      }
      li {
        background: #f8fafc;
        border: 1px solid #d9e2ec;
        border-radius: 6px;
        padding: 12px;
      }
      .todo-id {
        color: #627d98;
        font-size: 12px;
        margin-right: 6px;
      }
      .meta {
        margin-top: 16px;
        font-size: 13px;
        color: #627d98;
      }
      .event-log {
        margin-top: 16px;
        display: grid;
        gap: 8px;
      }
      .writer-list {
        margin-top: 16px;
        display: grid;
        gap: 8px;
      }
      .writer-row {
        display: grid;
        grid-template-columns: auto minmax(0, 1fr) auto auto;
        gap: 8px;
        align-items: center;
        background: #f8fafc;
        border: 1px solid #d9e2ec;
        border-radius: 6px;
        padding: 8px;
      }
      .writer-title {
        min-width: 0;
        height: 32px;
      }
      .event {
        font-size: 13px;
        line-height: 1.4;
        color: #334e68;
        background: #f0f4f8;
        border: 1px solid #d9e2ec;
        border-radius: 6px;
        padding: 10px;
      }
      @media (max-width: 760px) {
        .columns {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <h1>Launch todos</h1>
      <p>Left side writes to SQLite. Right side is a separate Electrolite subscriber.</p>
      <div class="columns">
        <section>
          <h2>Backend writer</h2>
          <p class="hint">Click a button here. The server inserts a todo into SQLite.</p>
          <form id="todo-form">
            <input id="todo-title" placeholder="New todo" autocomplete="off">
	            <div class="button-row">
	              <button>Add typed todo</button>
	              <button class="secondary" id="quick-add" type="button">Add random todo</button>
              <button class="secondary" id="batch-write" type="button">Add 2 as batch</button>
	            </div>
	          </form>
	          <div class="writer-list" id="writer-list"></div>
	          <div class="event-log" id="events"></div>
        </section>
        <section>
          <h2>Live subscriber</h2>
	          <p class="hint">This column listens to <code>projectTodos/launch</code>. New rows show up here through live replay.</p>
	          <ul id="todos"></ul>
	          <div class="meta" id="status">Connecting...</div>
	          <div class="event-log" id="sync-events"></div>
	        </section>
      </div>
    </main>
    <script type="module" src="/app.js"></script>
  </body>
</html>`;

const appJs = `import { ShapeClient } from "/electrolite-browser-client.js";

const list = document.querySelector("#todos");
const form = document.querySelector("#todo-form");
const title = document.querySelector("#todo-title");
const status = document.querySelector("#status");
const events = document.querySelector("#events");
const syncEvents = document.querySelector("#sync-events");
const quickAdd = document.querySelector("#quick-add");
const batchWrite = document.querySelector("#batch-write");
const writerList = document.querySelector("#writer-list");
let writeCount = 1;
let latestRows = [];
const dirtyTitles = new Map();

const todos = new ShapeClient(new URL("/electrolite/v1/projectTodos/launch", window.location.href).href, {
  persist: true,
  multiTab: true,
  retry: { minDelayMs: 100, maxDelayMs: 1000 },
});

todos.subscribe((rows) => {
  latestRows = rows;
  renderWriterRows(rows);
  list.replaceChildren(
    ...rows.map((row) => {
      const item = document.createElement("li");
      const id = document.createElement("span");
      id.className = "todo-id";
      id.textContent = "#" + row.id;
      item.append(id, row.title);
      return item;
    }),
  );
});

todos.subscribeStatus((next) => {
  status.textContent = "Status: " + next.type + " at offset " + next.offset;
  if (next.error) {
    status.textContent += " (" + next.error.message + ")";
  }
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  await addTodo(title.value);
  title.value = "";
  title.focus();
});

quickAdd.addEventListener("click", async () => {
  await addTodo("Button-created todo " + writeCount);
  writeCount += 1;
});

batchWrite.addEventListener("click", async () => {
  const response = await fetch("/api/batch", { method: "POST" });
  const body = await response.json();
  prependEvent("SQLite batch: inserted #" + body.inserted.join(" and #"));
  await catchUp();
});

let pendingSyncMessages = [];
let syncFlushTimer = null;

todos.subscribeEvents((event) => {
  if (event.type === "snapshot") {
    prependSyncEvent("snapshot at offset " + event.offset);
  } else if (event.type === "insert" || event.type === "update" || event.type === "delete") {
    pendingSyncMessages.push(event);
  } else if (event.type === "hydrate") {
    prependSyncEvent("loaded IndexedDB cache at offset " + event.offset + "; validating");
  } else if (event.type === "remote_apply") {
    prependSyncEvent("received from another tab at offset " + event.offset);
  } else if (event.type === "replay") {
    scheduleSyncFlush(event.offset);
  }
});

async function addTodo(nextTitle) {
  const trimmed = nextTitle.trim();
  const response = await fetch("/api/todos", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ title: trimmed }),
  });
  const body = await response.json();
  prependEvent("SQLite insert: " + body.title);
  await catchUp();
}

async function renameTodo(id, title) {
  const response = await fetch("/api/todos/" + id, {
    method: "PATCH",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ title }),
  });
  const body = await response.json();
  dirtyTitles.delete(String(id));
  prependEvent("SQLite update: " + body.title);
  await catchUp();
}

async function deleteTodo(id) {
  await fetch("/api/todos/" + id, { method: "DELETE" });
  dirtyTitles.delete(String(id));
  prependEvent("SQLite delete: todo " + id);
  await catchUp();
}

async function catchUp() {
  if (todos.offset >= 0) {
    await todos.request({ offset: todos.offset });
  }
}

function renderWriterRows(rows) {
  const focusedId = document.activeElement?.dataset?.todoId ?? null;
  const focusedSelectionStart = document.activeElement?.selectionStart ?? null;
  const focusedSelectionEnd = document.activeElement?.selectionEnd ?? null;
  writerList.replaceChildren(
    ...rows.map((row) => {
      const rowId = String(row.id);
      const item = document.createElement("div");
      item.className = "writer-row";

      const label = document.createElement("div");
      label.className = "todo-id";
      label.textContent = "#" + row.id;

      const input = document.createElement("input");
      input.className = "writer-title";
      input.dataset.todoId = rowId;
      input.value = dirtyTitles.get(rowId) ?? row.title;
      input.setAttribute("aria-label", "Todo " + row.id + " title");
      input.addEventListener("input", () => {
        if (input.value === row.title) {
          dirtyTitles.delete(rowId);
        } else {
          dirtyTitles.set(rowId, input.value);
        }
      });

      const rename = document.createElement("button");
      rename.className = "small secondary";
      rename.type = "button";
      rename.textContent = "Save";
      rename.addEventListener("click", () => renameTodo(row.id, input.value));
      input.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          renameTodo(row.id, input.value);
        }
      });

      const remove = document.createElement("button");
      remove.className = "small danger";
      remove.type = "button";
      remove.textContent = "Delete";
      remove.addEventListener("click", () => deleteTodo(row.id));

      item.append(label, input, rename, remove);
      return item;
    }),
  );
  if (focusedId) {
    const input = Array.from(writerList.querySelectorAll("[data-todo-id]"))
      .find((element) => element.dataset.todoId === focusedId);
    input?.focus();
    if (input && focusedSelectionStart !== null && focusedSelectionEnd !== null) {
      input.setSelectionRange(focusedSelectionStart, focusedSelectionEnd);
    }
  }
}

function prependEvent(message) {
  const item = document.createElement("div");
  item.className = "event";
  item.textContent = message;
  events.prepend(item);
}

function prependSyncEvent(message) {
  const item = document.createElement("div");
  item.className = "event";
  item.textContent = message;
  syncEvents.prepend(item);
}

function scheduleSyncFlush(offset) {
  if (syncFlushTimer) {
    clearTimeout(syncFlushTimer);
  }
  syncFlushTimer = setTimeout(() => {
    const messages = pendingSyncMessages;
    pendingSyncMessages = [];
    syncFlushTimer = null;
    if (messages.length === 0) {
      return;
    }
    const batches = new Map();
    for (const event of messages) {
      const batchId = event.message.batch_id ?? "unknown";
      if (!batches.has(batchId)) {
        batches.set(batchId, []);
      }
      batches.get(batchId).push(event);
    }
    for (const [batchId, batchMessages] of [...batches].reverse()) {
      const inserts = batchMessages.filter((event) => event.type === "insert").length;
      const updates = batchMessages.filter((event) => event.type === "update").length;
      const deletes = batchMessages.filter((event) => event.type === "delete").length;
      const parts = [];
      if (inserts) parts.push("+" + inserts + " rows");
      if (updates) parts.push(updates + " updates");
      if (deletes) parts.push("-" + deletes + " rows");
      if (batchMessages.length > 1) {
        prependSyncEvent("Batch applied: " + parts.join(", ") + " at offset " + offset);
        prependSyncEvent("  batch id " + batchId);
      } else {
        prependSyncEvent("Change applied: " + parts.join(", ") + " at offset " + offset);
      }
      for (const event of [...batchMessages].reverse()) {
        prependSyncEvent("  " + event.type + " " + JSON.stringify(event.message.key));
      }
    }
  }, 0);
}

todos.start();
`;
