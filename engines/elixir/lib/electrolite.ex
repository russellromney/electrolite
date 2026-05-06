defmodule Electrolite do
  @moduledoc """
  Tiny experimental Electrolite engine for Elixir.

  Mirrors the Python and Node engines: install SQLite triggers, take a
  snapshot of matching rows, replay logical changes, and run explicit
  write batches with a shared `batch_id`. Uses `Exqlite` directly.

  Owns one SQLite connection inside a `GenServer`, so concurrent calls
  serialize cleanly.
  """

  use GenServer

  alias Exqlite.Sqlite3

  defmodule Shape do
    @moduledoc false
    defstruct [:table, :columns, :predicate]
  end

  def shape(opts), do: struct!(Shape, opts)
  def all_pred, do: %{type: :all}
  def eq(column, value), do: %{type: :eq, column: column, value: value}
  def gt(column, value), do: %{type: :gt, column: column, value: value}
  def lt(column, value), do: %{type: :lt, column: column, value: value}
  def gte(column, value), do: %{type: :gte, column: column, value: value}
  def lte(column, value), do: %{type: :lte, column: column, value: value}

  @range_ops %{gt: ">", lt: "<", gte: ">=", lte: "<="}

  # --- public API ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  def stop(pid), do: GenServer.stop(pid)

  def execute(pid, sql, args \\ []), do: GenServer.call(pid, {:execute, sql, args})
  def execute_batch(pid, sql), do: GenServer.call(pid, {:execute_batch, sql})
  def install_triggers(pid, table), do: GenServer.call(pid, {:install_triggers, table})
  def write_batch(pid, statements), do: GenServer.call(pid, {:write_batch, statements})
  def snapshot(pid, shape), do: GenServer.call(pid, {:snapshot, shape})
  def replay(pid, shape, offset, limit \\ 1000), do: GenServer.call(pid, {:replay, shape, offset, limit})
  def log_id(pid), do: GenServer.call(pid, :log_id)

  @doc """
  Block until the log changes or `timeout` ms elapses.
  Returns `:changed` or `:timeout`.
  """
  def wait_for_change(pid, timeout \\ 20_000) do
    GenServer.call(pid, {:subscribe_change}, :infinity)
    receive do
      {:electrolite_change, ^pid} -> :changed
    after
      timeout ->
        GenServer.cast(pid, {:unsubscribe_change, self()})
        :timeout
    end
  end

  # --- GenServer ---

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :db_path)
    {:ok, conn} = Sqlite3.open(path)
    state = %{conn: conn, subscribers: MapSet.new()}
    :ok = bootstrap(conn)
    {:ok, state}
  end

  @impl true
  def handle_call({:execute, sql, args}, _from, state) do
    res = exec(state.conn, sql, args)
    state = notify(state)
    {:reply, res, state}
  end

  def handle_call({:execute_batch, sql}, _from, state) do
    res = Sqlite3.execute(state.conn, sql)
    state = notify(state)
    {:reply, res, state}
  end

  def handle_call({:install_triggers, table}, _from, state) do
    res = do_install_triggers(state.conn, table)
    {:reply, res, state}
  end

  def handle_call({:write_batch, statements}, _from, state) do
    batch_id = random_hex(16)

    res =
      try do
        :ok = Sqlite3.execute(state.conn, "BEGIN IMMEDIATE")

        :ok =
          exec(
            state.conn,
            "INSERT INTO _electrolite_meta (key, value) VALUES ('current_batch_id', ?) " <>
              "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [batch_id]
          )

        Enum.each(statements, fn {sql, args} ->
          :ok = exec(state.conn, sql, args)
        end)

        :ok = exec(state.conn, "DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'", [])
        :ok = Sqlite3.execute(state.conn, "COMMIT")
        :ok
      rescue
        e ->
          _ = Sqlite3.execute(state.conn, "ROLLBACK")
          {:error, e}
      end

    state = if res == :ok, do: notify(state), else: state
    {:reply, res, state}
  end

  def handle_call({:snapshot, %Shape{} = shape}, _from, state) do
    res = do_snapshot(state.conn, shape)
    {:reply, res, state}
  end

  def handle_call({:replay, %Shape{} = shape, offset, limit}, _from, state) do
    res = do_replay(state.conn, shape, offset, limit)
    {:reply, res, state}
  end

  def handle_call(:log_id, _from, state) do
    {:reply, fetch_log_id(state.conn), state}
  end

  def handle_call({:subscribe_change}, {pid, _ref}, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  @impl true
  def handle_cast({:unsubscribe_change, pid}, state) do
    {:noreply, %{state | subscribers: MapSet.delete(state.subscribers, pid)}}
  end

  defp notify(state) do
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:electrolite_change, self()})
    end)
    %{state | subscribers: MapSet.new()}
  end

  # --- helpers ---

  defp bootstrap(conn) do
    :ok =
      Sqlite3.execute(
        conn,
        """
        CREATE TABLE IF NOT EXISTS _electrolite_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS _electrolite_watched_tables (table_name TEXT PRIMARY KEY, pk_columns TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS _electrolite_log (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id TEXT NOT NULL,
          table_name TEXT NOT NULL,
          op TEXT NOT NULL,
          pk_json TEXT NOT NULL,
          old_pk_json TEXT, new_pk_json TEXT,
          old_json TEXT, new_json TEXT,
          created_at INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE INDEX IF NOT EXISTS _electrolite_log_table_seq_idx ON _electrolite_log (table_name, seq);
        """
      )

    case query(conn, "SELECT value FROM _electrolite_meta WHERE key = 'log_id'", []) do
      {:ok, []} ->
        :ok = exec(conn, "INSERT INTO _electrolite_meta (key, value) VALUES ('log_id', ?)", [random_hex(16)])

      _ ->
        :ok
    end
  end

  defp do_install_triggers(conn, table) do
    {:ok, info} = inspect_table(conn, table)

    if info.pk == [] do
      {:error, "table #{table} must have a primary key"}
    else
      :ok =
        exec(
          conn,
          "INSERT INTO _electrolite_watched_tables (table_name, pk_columns) VALUES (?, ?) " <>
            "ON CONFLICT(table_name) DO UPDATE SET pk_columns = excluded.pk_columns",
          [table, Jason.encode!(info.pk)]
        )

      new_row = row_json("NEW", info.columns)
      old_row = row_json("OLD", info.columns)
      new_pk = row_json("NEW", info.pk)
      old_pk = row_json("OLD", info.pk)

      batch_id =
        "COALESCE((SELECT value FROM _electrolite_meta WHERE key = 'current_batch_id'), lower(hex(randomblob(16))))"

      lit = quote_string(table)
      tbl = quote_ident(table)

      sql = """
      DROP TRIGGER IF EXISTS "_electrolite_#{table}_ai";
      DROP TRIGGER IF EXISTS "_electrolite_#{table}_au";
      DROP TRIGGER IF EXISTS "_electrolite_#{table}_ad";
      CREATE TRIGGER "_electrolite_#{table}_ai" AFTER INSERT ON #{tbl} BEGIN
        INSERT INTO _electrolite_log (batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json)
        VALUES (#{batch_id}, #{lit}, 'insert', #{new_pk}, NULL, #{new_pk}, NULL, #{new_row});
      END;
      CREATE TRIGGER "_electrolite_#{table}_au" AFTER UPDATE ON #{tbl} BEGIN
        INSERT INTO _electrolite_log (batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json)
        VALUES (#{batch_id}, #{lit}, 'update', #{new_pk}, #{old_pk}, #{new_pk}, #{old_row}, #{new_row});
      END;
      CREATE TRIGGER "_electrolite_#{table}_ad" AFTER DELETE ON #{tbl} BEGIN
        INSERT INTO _electrolite_log (batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json)
        VALUES (#{batch_id}, #{lit}, 'delete', #{old_pk}, #{old_pk}, NULL, #{old_row}, NULL);
      END;
      """

      Sqlite3.execute(conn, sql)
    end
  end

  defp do_snapshot(conn, %Shape{} = shape) do
    with {:ok, info} <- watched_info(conn, shape.table) do
      {where_sql, args} = compile_predicate(shape.predicate)

      sql =
        "SELECT #{row_json("", shape.columns)} FROM #{quote_ident(shape.table)}" <>
          if(where_sql == "", do: "", else: " WHERE #{where_sql}") <>
          " ORDER BY " <> Enum.map_join(info.pk, ",", &quote_ident/1)

      {:ok, rows} = query(conn, sql, args)
      decoded = Enum.map(rows, fn [json] -> Jason.decode!(json) end)
      {:ok, offset} = high_water(conn)
      {:ok, log_id} = fetch_log_id(conn)

      {:ok,
       %{
         log_id: log_id,
         shape_handle: shape_handle(shape),
         key_columns: info.pk,
         rows: decoded,
         offset: offset
       }}
    end
  end

  defp do_replay(conn, %Shape{} = shape, offset, limit) do
    with {:ok, _info} <- watched_info(conn, shape.table) do
      limit = max(1, limit)
      {:ok, rows} = read_log_page(conn, shape.table, offset, limit)
      latest = if rows == [], do: offset, else: List.last(rows).seq

      {:ok, newer} =
        query(
          conn,
          "SELECT 1 FROM _electrolite_log WHERE table_name = ? AND seq > ? LIMIT 1",
          [shape.table, latest]
        )

      messages =
        rows
        |> Enum.flat_map(&messages_for(shape.predicate, &1))

      {:ok, log_id} = fetch_log_id(conn)

      {:ok,
       %{
         log_id: log_id,
         shape_handle: shape_handle(shape),
         messages: messages,
         offset: latest,
         up_to_date: newer == []
       }}
    end
  end

  defp read_log_page(conn, table, offset, limit) do
    {:ok, page} =
      query(
        conn,
        "SELECT seq, batch_id, op, pk_json, old_pk_json, new_pk_json, old_json, new_json " <>
          "FROM _electrolite_log WHERE table_name = ? AND seq > ? ORDER BY seq LIMIT ?",
        [table, offset, limit]
      )

    rows = Enum.map(page, &parse_log_row/1)

    rows =
      case List.last(rows) do
        nil ->
          rows

        last ->
          {:ok, more} =
            query(
              conn,
              "SELECT seq, batch_id, op, pk_json, old_pk_json, new_pk_json, old_json, new_json " <>
                "FROM _electrolite_log WHERE table_name = ? AND seq > ? AND batch_id = ? ORDER BY seq",
              [table, last.seq, last.batch_id]
            )

          rows ++ Enum.map(more, &parse_log_row/1)
      end

    {:ok, rows}
  end

  defp parse_log_row([seq, batch_id, op, pk, old_pk, new_pk, old_row, new_row]) do
    %{
      seq: seq,
      batch_id: batch_id,
      op: op,
      pk: decode_or_nil(pk),
      old_pk: decode_or_nil(old_pk),
      new_pk: decode_or_nil(new_pk),
      old_row: decode_or_nil(old_row),
      new_row: decode_or_nil(new_row)
    }
  end

  defp decode_or_nil(nil), do: nil
  defp decode_or_nil(""), do: nil
  defp decode_or_nil(json) when is_binary(json), do: Jason.decode!(json)

  defp messages_for(predicate, row) do
    old_match = predicate_matches(predicate, row.old_row)
    new_match = predicate_matches(predicate, row.new_row)
    old_key = row.old_pk || row.pk
    new_key = row.new_pk || row.pk

    cond do
      not old_match and new_match and row.new_row != nil ->
        [build_msg(:insert, row, new_key, row.new_row)]

      old_match and new_match and row.new_row != nil ->
        if old_key == new_key do
          [build_msg(:update, row, new_key, row.new_row)]
        else
          [build_msg(:delete, row, old_key, nil), build_msg(:insert, row, new_key, row.new_row)]
        end

      old_match and not new_match ->
        [build_msg(:delete, row, old_key, nil)]

      true ->
        []
    end
  end

  defp build_msg(kind, row, key, value) do
    base = %{type: kind, batch_id: row.batch_id, key: key, offset: row.seq}
    if value, do: Map.put(base, :value, value), else: base
  end

  defp predicate_matches(_p, nil), do: false
  defp predicate_matches(%{type: :all}, _row), do: true
  defp predicate_matches(%{type: :eq, column: c, value: v}, row), do: Map.get(row, c) == v

  defp predicate_matches(%{type: op, column: c, value: v}, row) when op in [:gt, :lt, :gte, :lte] do
    compare_scalar(Map.get(row, c), v, op)
  end

  defp compare_scalar(nil, _, _), do: false
  defp compare_scalar(_, nil, _), do: false
  defp compare_scalar(left, right, op) when is_number(left) and is_number(right), do: cmp(left, right, op)
  defp compare_scalar(left, right, op) when is_binary(left) and is_binary(right), do: cmp(left, right, op)
  defp compare_scalar(_, _, _), do: false

  defp cmp(a, b, :gt), do: a > b
  defp cmp(a, b, :lt), do: a < b
  defp cmp(a, b, :gte), do: a >= b
  defp cmp(a, b, :lte), do: a <= b

  defp compile_predicate(%{type: :all}), do: {"", []}
  defp compile_predicate(%{type: :eq, column: c, value: nil}), do: {"#{quote_ident(c)} IS NULL", []}
  defp compile_predicate(%{type: :eq, column: c, value: v}), do: {"#{quote_ident(c)} = ?", [v]}

  defp compile_predicate(%{type: op, column: c, value: v}) when op in [:gt, :lt, :gte, :lte] do
    {"#{quote_ident(c)} #{Map.fetch!(@range_ops, op)} ?", [v]}
  end

  defp inspect_table(conn, table) do
    {:ok, rows} = query(conn, "PRAGMA table_info(#{quote_string(table)})", [])

    if rows == [] do
      {:error, "table #{table} does not exist"}
    else
      sorted = Enum.sort_by(rows, fn [cid | _] -> cid end)
      columns = Enum.map(sorted, fn [_, name | _] -> name end)

      pk =
        sorted
        |> Enum.filter(fn row -> Enum.at(row, 5) > 0 end)
        |> Enum.sort_by(fn row -> Enum.at(row, 5) end)
        |> Enum.map(fn [_, name | _] -> name end)

      {:ok, %{columns: columns, pk: pk}}
    end
  end

  defp watched_info(conn, table) do
    with {:ok, info} <- inspect_table(conn, table),
         {:ok, [[pk_json]]} <-
           query(conn, "SELECT pk_columns FROM _electrolite_watched_tables WHERE table_name = ?", [table]) do
      {:ok, %{info | pk: Jason.decode!(pk_json)}}
    else
      {:ok, []} -> {:error, "table #{table} is not watched by Electrolite"}
      other -> other
    end
  end

  defp fetch_log_id(conn) do
    {:ok, [[id]]} = query(conn, "SELECT value FROM _electrolite_meta WHERE key = 'log_id'", [])
    {:ok, id}
  end

  defp high_water(conn) do
    {:ok, [[seq]]} = query(conn, "SELECT COALESCE(MAX(seq), 0) FROM _electrolite_log", [])
    {:ok, seq}
  end

  defp shape_handle(%Shape{} = shape) do
    body =
      %{
        "auth_scope" => "",
        "columns" => Enum.sort(shape.columns),
        "predicate" => predicate_to_json(shape.predicate),
        "schema_version" => 1,
        "table" => shape.table
      }

    canon = canonical_json(body)
    :crypto.hash(:sha256, canon) |> Base.encode16(case: :lower)
  end

  defp predicate_to_json(%{type: :all}), do: %{"type" => "all"}

  defp predicate_to_json(%{type: :eq, column: c, value: v}),
    do: %{"type" => "eq", "column" => c, "value" => v}

  defp predicate_to_json(%{type: op, column: c, value: v}) when op in [:gt, :lt, :gte, :lte],
    do: %{"type" => Atom.to_string(op), "column" => c, "value" => v}

  defp canonical_json(value) when is_map(value) do
    pairs =
      value
      |> Enum.sort_by(fn {k, _} -> k end)
      |> Enum.map_join(",", fn {k, v} -> Jason.encode!(k) <> ":" <> canonical_json(v) end)

    "{" <> pairs <> "}"
  end

  defp canonical_json(value) when is_list(value) do
    "[" <> Enum.map_join(value, ",", &canonical_json/1) <> "]"
  end

  defp canonical_json(value), do: Jason.encode!(value)

  defp row_json(prefix, columns) do
    q = if prefix == "", do: "", else: "#{prefix}."

    parts =
      Enum.map_join(columns, ", ", fn c ->
        "#{quote_string(c)}, #{q}#{quote_ident(c)}"
      end)

    "json_object(#{parts})"
  end

  defp quote_ident(s), do: ~s("#{String.replace(s, "\"", "\"\"")}")
  defp quote_string(s), do: "'#{String.replace(s, "'", "''")}'"

  defp random_hex(n) do
    :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)
  end

  defp exec(conn, sql, args) do
    case query(conn, sql, args) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  defp query(conn, sql, args) do
    {:ok, stmt} = Sqlite3.prepare(conn, sql)

    try do
      :ok = Sqlite3.bind(stmt, args)
      collect_rows(conn, stmt, [])
    after
      :ok = Sqlite3.release(conn, stmt)
    end
  end

  defp collect_rows(conn, stmt, acc) do
    case Sqlite3.step(conn, stmt) do
      :done -> {:ok, Enum.reverse(acc)}
      {:row, row} -> collect_rows(conn, stmt, [row | acc])
      {:error, reason} -> {:error, reason}
    end
  end
end
