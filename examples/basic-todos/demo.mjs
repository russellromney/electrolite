import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ShapeClient } from "../../clients/browser/electrolite.js";
import {
  createElectrolite,
  eq,
  shape,
} from "../../packages/electrolite-node/electrolite-node.js";

const dir = mkdtempSync(join(tmpdir(), "electrolite-demo-"));
const dbPath = join(dir, "demo.db");

const electrolite = createElectrolite({
  dbPath,
  connectionPoolSize: 1,
  shapes: {
    projectTodos: shape({
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      params: ["projectId"],
      where: ({ params }) => eq("project_id", params.projectId),
      scope: ({ params }) => `project:${params.projectId}`,
      authorize: ({ params, context }) => {
        return context.projectIds.has(params.projectId);
      },
    }),
  },
});

try {
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
      (2, 'launch', 'Invite beta users', 0),
      (3, 'ops', 'Rotate credentials', 0);
  `);

  const fetch = (url, init) => {
    return electrolite.handle(new Request(url, init), {
      projectIds: new Set(["launch"]),
    });
  };
  const client = new ShapeClient(
    "https://app.example/electrolite/v1/projectTodos/launch",
    { fetch },
  );

  await client.request({ offset: -1 });
  console.log("Initial rows visible to this user:");
  console.table(client.currentRows());

  const live = client.request({ offset: client.offset, live: true });
  electrolite.execute(
    "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
    [4, "launch", "Ship the demo"],
  );
  await live;

  console.log("After a backend write, the browser materializer updates:");
  console.table(client.currentRows());

  const denied = await electrolite.handle(
    new Request("https://app.example/electrolite/v1/projectTodos/ops?offset=-1"),
    { projectIds: new Set(["launch"]) },
  );
  console.log("Unauthorized Shape request status:", denied.status);
} finally {
  rmSync(dir, { recursive: true, force: true });
}
