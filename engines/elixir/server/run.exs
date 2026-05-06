if System.get_env("ELECTROLITE_TEST_SERVER") != "1" do
  IO.puts(
    :stderr,
    "engines/elixir/server/run.exs is a test-only HTTP server with " <>
      "an unauthenticated /_test/exec endpoint. Set " <>
      "ELECTROLITE_TEST_SERVER=1 to launch it."
  )

  System.halt(1)
end

{opts, _, _} =
  OptionParser.parse(System.argv(),
    strict: [port: :integer, db: :string]
  )

port = opts[:port] || raise "--port required"
db_path = opts[:db] || raise "--db required"

{:ok, _pid} =
  Electrolite.start_link(
    db_path: db_path,
    live_timeout_ms: 2_000,
    name: Electrolite.TestServer.engine_name()
  )

pid = Electrolite.TestServer.engine_name()

:ok =
  Electrolite.execute_batch(pid, """
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL,
    title TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS feature_flags (
    id INTEGER PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT 0
  );
  """)

:ok = Electrolite.install_triggers(pid, "todos")
:ok = Electrolite.install_triggers(pid, "feature_flags")

:ok =
  Electrolite.add_shape(
    pid,
    "projectTodos",
    Electrolite.shape_def(
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      params: ["project_id"],
      where: fn ctx -> Electrolite.eq("project_id", ctx.params["project_id"]) end,
      scope: fn ctx -> "project:" <> ctx.params["project_id"] end,
      authorize: fn ctx ->
        case ctx.context do
          %{projects: projects} -> ctx.params["project_id"] in projects
          _ -> false
        end
      end
    )
  )

:ok =
  Electrolite.add_shape(
    pid,
    "highIds",
    Electrolite.shape_def(
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      where: fn _ -> Electrolite.gt("id", 1) end
    )
  )

# Boolean coercion proof: BOOLEAN column + true predicate.
:ok =
  Electrolite.add_shape(
    pid,
    "enabledFlags",
    Electrolite.shape_def(
      table: "feature_flags",
      columns: ["id", "enabled"],
      where: fn _ -> Electrolite.eq("enabled", true) end
    )
  )

# Range-null proof: every engine must reject with 400.
:ok =
  Electrolite.add_shape(
    pid,
    "bogusGt",
    Electrolite.shape_def(
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      where: fn _ -> Electrolite.gt("id", nil) end
    )
  )

{:ok, _} = Electrolite.TestServer.start(port)
IO.puts("electrolite-server listening on #{port}")

# Keep the script running.
Process.sleep(:infinity)
