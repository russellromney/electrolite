defmodule ElectroliteTest do
  use ExUnit.Case, async: false

  defp setup_app(opts \\ []) do
    dir = System.tmp_dir!() |> Path.join("electrolite-#{:rand.uniform(10_000_000)}")
    File.mkdir_p!(dir)
    db_path = Path.join(dir, "app.db")
    {:ok, pid} = Electrolite.start_link([db_path: db_path] ++ opts)

    on_exit(fn ->
      try do
        Process.exit(pid, :normal)
      catch
        :exit, _ -> :ok
      end

      File.rm_rf!(dir)
    end)

    {pid, dir}
  end

  defp seed_todos(pid) do
    :ok =
      Electrolite.execute_batch(pid, """
        CREATE TABLE todos (
          id INTEGER PRIMARY KEY,
          project_id TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO todos (id, project_id, title, done) VALUES
          (1, 'p1', 'ship electrolite', 0),
          (2, 'p2', 'other project', 0),
          (3, 'p1', 'second p1', 0);
      """)

    :ok = Electrolite.install_triggers(pid, "todos")
  end

  defp project_todos_def do
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
  end

  defp build_q(body, live \\ false) do
    base = "offset=#{body["offset"]}"
    base = if live, do: base <> "&live=true", else: base
    base = if body["log_id"], do: base <> "&log_id=#{body["log_id"]}", else: base
    if body["shape_handle"], do: base <> "&shape_handle=#{body["shape_handle"]}", else: base
  end

  test "snapshot returns matching rows ordered by key" do
    {pid, _} = setup_app()
    seed_todos(pid)
    :ok = Electrolite.add_shape(pid, "projectTodos", project_todos_def())

    {200, body} =
      Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", "offset=-1", %{projects: ["p1"]})

    assert body["type"] == "snapshot"
    assert body["key_columns"] == ["id"]
    assert Enum.map(body["rows"], & &1["id"]) == [1, 3]
    assert byte_size(body["shape_handle"]) == 64
  end

  test "eq predicate filters snapshot" do
    {pid, _} = setup_app()
    seed_todos(pid)
    :ok = Electrolite.add_shape(pid, "projectTodos", project_todos_def())
    {200, body} = Electrolite.handle(pid, "/electrolite/v1/projectTodos/p2", "offset=-1", %{projects: ["p2"]})
    assert Enum.map(body["rows"], & &1["id"]) == [2]
  end

  test "in predicate filters snapshot" do
    {pid, _} = setup_app()
    seed_todos(pid)

    :ok =
      Electrolite.add_shape(
        pid,
        "todoSet",
        Electrolite.shape_def(
          table: "todos",
          columns: ["id", "project_id"],
          where: fn _ -> Electrolite.in_list("project_id", ["p1", "p2"]) end
        )
      )

    {200, body} = Electrolite.handle(pid, "/electrolite/v1/todoSet", "offset=-1", %{})
    assert length(body["rows"]) == 3
  end

  test "range predicate filters snapshot" do
    {pid, _} = setup_app()
    seed_todos(pid)

    :ok =
      Electrolite.add_shape(
        pid,
        "highIds",
        Electrolite.shape_def(
          table: "todos",
          columns: ["id", "project_id"],
          where: fn _ -> Electrolite.gt("id", 1) end
        )
      )

    {200, body} = Electrolite.handle(pid, "/electrolite/v1/highIds", "offset=-1", %{})
    assert Enum.map(body["rows"], & &1["id"]) == [2, 3]
  end

  test "and predicate combines conditions" do
    {pid, _} = setup_app()
    seed_todos(pid)

    :ok =
      Electrolite.add_shape(
        pid,
        "p1HighIds",
        Electrolite.shape_def(
          table: "todos",
          columns: ["id", "project_id"],
          where: fn _ ->
            Electrolite.and_pred([
              Electrolite.eq("project_id", "p1"),
              Electrolite.gt("id", 1)
            ])
          end
        )
      )

    {200, body} = Electrolite.handle(pid, "/electrolite/v1/p1HighIds", "offset=-1", %{})
    assert Enum.map(body["rows"], & &1["id"]) == [3]
  end

  test "replay emits insert / update / delete" do
    {pid, _} = setup_app()
    seed_todos(pid)
    :ok = Electrolite.add_shape(pid, "projectTodos", project_todos_def())

    {200, snap} =
      Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", "offset=-1", %{projects: ["p1"]})

    :ok = Electrolite.execute(pid, "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [10, "p1", "new"])
    :ok = Electrolite.execute(pid, "UPDATE todos SET title = ? WHERE id = ?", ["renamed", 10])
    :ok = Electrolite.execute(pid, "DELETE FROM todos WHERE id = ?", [10])

    {200, body} =
      Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", build_q(snap), %{projects: ["p1"]})

    types = Enum.map(body["messages"], & &1["type"])
    assert types == ["insert", "update", "delete"]
  end

  test "write_batch shares batch_id" do
    {pid, _} = setup_app()
    seed_todos(pid)
    :ok = Electrolite.add_shape(pid, "projectTodos", project_todos_def())

    {200, snap} =
      Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", "offset=-1", %{projects: ["p1"]})

    :ok =
      Electrolite.write_batch(pid, [
        {"INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [11, "p1", "a"]},
        {"INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [12, "p1", "b"]}
      ])

    {200, body} =
      Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", build_q(snap), %{projects: ["p1"]})

    [m1, m2] = body["messages"]
    assert m1["batch_id"] == m2["batch_id"]
  end

  test "replay extends past limit to finish a batch" do
    {pid, _} = setup_app()
    seed_todos(pid)

    shape =
      Electrolite.shape(
        table: "todos",
        columns: ["id", "project_id"],
        predicate: Electrolite.eq("project_id", "p1")
      )

    {:ok, snap} = Electrolite.snapshot(pid, shape)

    :ok =
      Electrolite.write_batch(pid, [
        {"INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [21, "p1", "a"]},
        {"INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [22, "p1", "b"]},
        {"INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [23, "p1", "c"]}
      ])

    {:ok, r} = Electrolite.replay(pid, shape, snap.offset, 1)
    assert length(r.messages) == 3
  end

  test "primary-key change replays as delete + insert" do
    {pid, _} = setup_app()

    :ok =
      Electrolite.execute_batch(pid, """
        CREATE TABLE things (thing_id TEXT PRIMARY KEY, owner TEXT NOT NULL);
        INSERT INTO things (thing_id, owner) VALUES ('a', 'alice');
      """)

    :ok = Electrolite.install_triggers(pid, "things")

    :ok =
      Electrolite.add_shape(
        pid,
        "thingsByOwner",
        Electrolite.shape_def(
          table: "things",
          columns: ["thing_id", "owner"],
          params: ["owner"],
          where: fn ctx -> Electrolite.eq("owner", ctx.params["owner"]) end
        )
      )

    {200, snap} = Electrolite.handle(pid, "/electrolite/v1/thingsByOwner/alice", "offset=-1", %{})
    :ok = Electrolite.execute(pid, "UPDATE things SET thing_id = ? WHERE thing_id = ?", ["b", "a"])

    {200, body} =
      Electrolite.handle(pid, "/electrolite/v1/thingsByOwner/alice", build_q(snap), %{})

    [d, i] = body["messages"]
    assert d["type"] == "delete" and d["key"] == %{"thing_id" => "a"}
    assert i["type"] == "insert" and i["key"] == %{"thing_id" => "b"}
  end

  test "unauthorized shape returns 404 shape_not_found" do
    {pid, _} = setup_app()
    seed_todos(pid)
    :ok = Electrolite.add_shape(pid, "projectTodos", project_todos_def())
    {status, body} = Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", "offset=-1", %{projects: ["p2"]})
    assert status == 404
    assert body == %{"error" => "shape_not_found"}
  end

  test "log_id mismatch returns 409" do
    {pid, _} = setup_app()
    seed_todos(pid)
    :ok = Electrolite.add_shape(pid, "projectTodos", project_todos_def())

    {200, snap} =
      Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", "offset=-1", %{projects: ["p1"]})

    q = "offset=#{snap["offset"]}&log_id=00000000000000000000000000000000"
    {status, body} = Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", q, %{projects: ["p1"]})
    assert status == 409
    assert body == %{"error" => "resync_required"}
  end

  test "compact past offset triggers resync" do
    {pid, _} = setup_app()
    seed_todos(pid)
    :ok = Electrolite.add_shape(pid, "projectTodos", project_todos_def())

    {200, snap} =
      Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", "offset=-1", %{projects: ["p1"]})

    :ok = Electrolite.execute(pid, "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [50, "p1", "x"])
    {:ok, stats} = Electrolite.compact(pid, "todos", 0)
    assert stats.retained_offset >= snap["offset"]

    {status, body} = Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", build_q(snap), %{projects: ["p1"]})
    assert status == 409
    assert body == %{"error" => "resync_required"}
  end

  test "install_triggers requires primary key" do
    {pid, _} = setup_app()
    :ok = Electrolite.execute_batch(pid, "CREATE TABLE no_pk (a INTEGER, b INTEGER);")
    assert match?({:error, _}, Electrolite.install_triggers(pid, "no_pk"))
  end

  test "composite primary keys exposed in keys" do
    {pid, _} = setup_app()

    :ok =
      Electrolite.execute_batch(pid, """
        CREATE TABLE memberships (
          org TEXT NOT NULL,
          \"user\" TEXT NOT NULL,
          role TEXT NOT NULL,
          PRIMARY KEY (org, \"user\")
        );
        INSERT INTO memberships (org, \"user\", role) VALUES ('o1', 'u1', 'admin');
      """)

    :ok = Electrolite.install_triggers(pid, "memberships")

    :ok =
      Electrolite.add_shape(
        pid,
        "memberships",
        Electrolite.shape_def(table: "memberships", columns: ["org", "user", "role"])
      )

    {200, snap} = Electrolite.handle(pid, "/electrolite/v1/memberships", "offset=-1", %{})
    assert snap["key_columns"] == ["org", "user"]
    assert snap["rows"] == [%{"org" => "o1", "user" => "u1", "role" => "admin"}]

    :ok = Electrolite.execute(pid, "DELETE FROM memberships WHERE org = ? AND \"user\" = ?", ["o1", "u1"])
    {200, body} = Electrolite.handle(pid, "/electrolite/v1/memberships", build_q(snap), %{})
    [m] = body["messages"]
    assert m["type"] == "delete"
    assert m["key"] == %{"org" => "o1", "user" => "u1"}
  end

  test "wait_for_change wakes when log changes" do
    {pid, _} = setup_app()
    seed_todos(pid)
    :ok = Electrolite.add_shape(pid, "projectTodos", project_todos_def())

    {200, snap} =
      Electrolite.handle(pid, "/electrolite/v1/projectTodos/p1", "offset=-1", %{projects: ["p1"]})

    parent = self()

    spawn_link(fn ->
      result = Electrolite.wait_for_change(pid, 1_000)
      send(parent, {:wait_done, result})
    end)

    Process.sleep(50)

    :ok = Electrolite.execute(pid, "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [60, "p1", "live"])

    assert_receive {:wait_done, :changed}, 2_000

    {:ok, r} =
      Electrolite.replay(
        pid,
        Electrolite.shape(
          table: "todos",
          columns: ["id", "project_id", "title", "done"],
          predicate: Electrolite.eq("project_id", "p1"),
          auth_scope: "project:p1"
        ),
        snap["offset"]
      )

    assert hd(r.messages).type == :insert
    assert hd(r.messages).value["title"] == "live"
  end
end
