defmodule ElectroliteTest do
  use ExUnit.Case, async: false

  setup do
    dir = System.tmp_dir!() |> Path.join("electrolite-#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(dir)
    db_path = Path.join(dir, "app.db")
    {:ok, pid} = Electrolite.start_link(db_path: db_path)

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
          (2, 'p2', 'other project', 0);
      """)

    :ok = Electrolite.install_triggers(pid, "todos")

    on_exit(fn ->
      Process.exit(pid, :normal)
      File.rm_rf!(dir)
    end)

    {:ok, pid: pid}
  end

  defp p1_shape do
    Electrolite.shape(
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      predicate: Electrolite.eq("project_id", "p1")
    )
  end

  test "snapshot filters by predicate", %{pid: pid} do
    {:ok, snap} = Electrolite.snapshot(pid, p1_shape())
    assert snap.key_columns == ["id"]
    assert length(snap.rows) == 1
    assert hd(snap.rows)["title"] == "ship electrolite"
    assert byte_size(snap.shape_handle) == 64
  end

  test "replay covers insert/update/delete and shared batch ids", %{pid: pid} do
    {:ok, snap} = Electrolite.snapshot(pid, p1_shape())

    :ok =
      Electrolite.execute(
        pid,
        "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
        [3, "p1", "from elixir"]
      )

    {:ok, r} = Electrolite.replay(pid, p1_shape(), snap.offset, 100)
    assert length(r.messages) == 1
    msg = hd(r.messages)
    assert msg.type == :insert
    assert msg.value["title"] == "from elixir"

    :ok = Electrolite.execute(pid, "UPDATE todos SET title = ? WHERE id = ?", ["renamed", 3])
    {:ok, r2} = Electrolite.replay(pid, p1_shape(), r.offset, 100)
    assert hd(r2.messages).type == :update
    assert hd(r2.messages).value["title"] == "renamed"

    :ok = Electrolite.execute(pid, "DELETE FROM todos WHERE id = ?", [3])
    {:ok, r3} = Electrolite.replay(pid, p1_shape(), r2.offset, 100)
    assert hd(r3.messages).type == :delete

    :ok =
      Electrolite.write_batch(pid, [
        {"INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [4, "p1", "a"]},
        {"INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [5, "p1", "b"]}
      ])

    {:ok, r4} = Electrolite.replay(pid, p1_shape(), r3.offset, 100)
    assert length(r4.messages) == 2
    [m1, m2] = r4.messages
    assert m1.batch_id == m2.batch_id
  end

  test "wait_for_change wakes when log changes", %{pid: pid} do
    {:ok, snap} = Electrolite.snapshot(pid, p1_shape())
    parent = self()

    spawn_link(fn ->
      result = Electrolite.wait_for_change(pid, 1_000)
      send(parent, {:wait_done, result})
    end)

    Process.sleep(50)

    :ok =
      Electrolite.execute(
        pid,
        "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
        [9, "p1", "live elixir"]
      )

    assert_receive {:wait_done, :changed}, 2_000

    {:ok, r} = Electrolite.replay(pid, p1_shape(), snap.offset, 100)
    assert hd(r.messages).type == :insert
    assert hd(r.messages).value["title"] == "live elixir"
  end
end
