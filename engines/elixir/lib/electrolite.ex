defmodule Electrolite do
  @moduledoc """
  Electrolite engine for Elixir. Implements the conformance contract in
  `engines/PROTOCOL.md`: install SQLite triggers, snapshot a shape,
  replay logical changes, share a `batch_id` across `write_batch`,
  detect log-id and retained-offset mismatches as `:resync_required`,
  and serve named shapes through `handle/3`.

  One `GenServer` owns one SQLite connection so concurrent calls
  serialize cleanly. Uses `Exqlite`.
  """

  use GenServer

  alias Exqlite.Sqlite3

  defmodule Shape do
    @moduledoc false
    defstruct table: nil, columns: [], predicate: %{type: :all}, auth_scope: "", schema_version: 1
  end

  defmodule ShapeDef do
    @moduledoc false
    defstruct [
      :table,
      :columns,
      params: [],
      where: nil,
      scope: nil,
      authorize: nil,
      schema_version: 1
    ]
  end

  @range_ops %{gt: ">", lt: "<", gte: ">=", lte: "<="}

  # ---- public helpers ----

  def shape(opts), do: struct!(Shape, opts)
  def shape_def(opts), do: struct!(ShapeDef, opts)
  def all_pred, do: %{type: :all}
  def eq(column, value), do: %{type: :eq, column: column, value: value}
  def gt(column, value), do: %{type: :gt, column: column, value: value}
  def lt(column, value), do: %{type: :lt, column: column, value: value}
  def gte(column, value), do: %{type: :gte, column: column, value: value}
  def lte(column, value), do: %{type: :lte, column: column, value: value}
  def in_list(column, values), do: %{type: :in, column: column, values: values}
  def and_pred(children), do: %{type: :and, predicates: children}
  def or_pred(children), do: %{type: :or, predicates: children}
  def not_pred(child), do: %{type: :not, predicate: child}

  # ---- public API ----

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:name])
  def stop(pid), do: GenServer.stop(pid)

  @doc """
  Mark the engine as stopped and wake every live waiter. In-flight
  live waits return a clean `200 {messages: [], up_to_date: true,
  shutdown: true}` response instead of trying to call back into
  the GenServer. The GenServer stays alive so existing handle/4
  callers can finish their reply; call `stop/1` to terminate it
  when you're done.
  """
  def shutdown(pid) do
    GenServer.cast(pid, :electrolite_shutdown)
  end

  def execute(pid, sql, args \\ []), do: GenServer.call(pid, {:execute, sql, args})
  def execute_batch(pid, sql), do: GenServer.call(pid, {:execute_batch, sql})
  def install_triggers(pid, table), do: GenServer.call(pid, {:install_triggers, table})
  def write_batch(pid, statements), do: GenServer.call(pid, {:write_batch, statements})
  def add_shape(pid, name, def), do: GenServer.call(pid, {:add_shape, name, def})
  def set_replay_limit(pid, limit), do: GenServer.call(pid, {:set_replay_limit, limit})
  def set_prefix(pid, prefix), do: GenServer.call(pid, {:set_prefix, prefix})
  def snapshot(pid, shape), do: GenServer.call(pid, {:snapshot, shape})
  def replay(pid, shape, offset, limit \\ 1000), do: GenServer.call(pid, {:replay, shape, offset, limit})
  def compact(pid, table, keep_last \\ 0), do: GenServer.call(pid, {:compact, table, keep_last})
  def log_id(pid), do: GenServer.call(pid, :log_id)

  @doc """
  Serve an Electrolite path. Returns `{status, body}` where status is
  an HTTP-style integer and body is a map suitable for JSON encoding.

  Live waits are managed in the calling process so the GenServer stays
  free to accept writes that would unblock the wait.
  """
  def handle(pid, path, query \\ "", context \\ %{}) do
    case GenServer.call(pid, {:handle_initial, path, query, context}, :infinity) do
      {:reply, response} ->
        response

      {:wait, shape, offset, timeout_ms} ->
        # The GenServer registered us as a subscriber atomically with
        # this reply, closing the TOCTOU window where a writer could
        # notify before we asked to be subscribed.
        engine_pid = GenServer.whereis(pid) || pid

        outcome =
          receive do
            {:electrolite_change, ^engine_pid} -> :changed
            {:electrolite_shutdown, ^engine_pid} -> :shutdown
          after
            timeout_ms ->
              GenServer.cast(pid, {:unsubscribe_change, self()})
              :timeout
          end

        case outcome do
          :shutdown ->
            current_handle = shape_handle(shape)

            {200,
             %{
               "type" => "replay",
               "log_id" => "",
               "shape_handle" => current_handle,
               "messages" => [],
               "offset" => offset,
               "up_to_date" => true,
               "shutdown" => true
             }}

          _ ->
            GenServer.call(pid, {:replay_for_handle, shape, offset}, :infinity)
        end
    end
  end

  def wait_for_change(pid, timeout \\ 20_000) do
    engine_pid = GenServer.whereis(pid) || pid
    GenServer.call(pid, :subscribe_change, :infinity)

    receive do
      {:electrolite_change, ^engine_pid} -> :changed
    after
      timeout ->
        GenServer.cast(pid, {:unsubscribe_change, self()})
        :timeout
    end
  end

  # ---- GenServer ----

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :db_path)
    {:ok, conn} = Sqlite3.open(path)

    state = %{
      conn: conn,
      subscribers: MapSet.new(),
      monitors: %{},
      stopped: false,
      shapes: %{},
      prefix: Keyword.get(opts, :prefix, "/electrolite/v1"),
      replay_limit: Keyword.get(opts, :replay_limit, 1000),
      live_timeout_ms: Keyword.get(opts, :live_timeout_ms, 20_000)
    }

    :ok = bootstrap(conn)
    {:ok, state}
  end

  @impl true
  def handle_call({:execute, sql, args}, _from, state) do
    res = exec(state.conn, sql, args)
    {:reply, res, notify(state)}
  end

  def handle_call({:execute_batch, sql}, _from, state) do
    res = Sqlite3.execute(state.conn, sql)
    {:reply, res, notify(state)}
  end

  def handle_call({:install_triggers, table}, _from, state) do
    {:reply, do_install_triggers(state.conn, table), state}
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

  def handle_call({:add_shape, name, %ShapeDef{} = def}, _from, state) do
    def = %{def | schema_version: def.schema_version || 1}
    {:reply, :ok, %{state | shapes: Map.put(state.shapes, name, def)}}
  end

  def handle_call({:set_replay_limit, limit}, _from, state),
    do: {:reply, :ok, %{state | replay_limit: max(1, limit)}}

  def handle_call({:set_prefix, prefix}, _from, state),
    do: {:reply, :ok, %{state | prefix: prefix}}

  def handle_call({:snapshot, %Shape{} = shape}, _from, state),
    do: {:reply, do_snapshot(state.conn, shape), state}

  def handle_call({:replay, %Shape{} = shape, offset, limit}, _from, state),
    do: {:reply, do_replay(state.conn, shape, offset, limit), state}

  def handle_call({:compact, table, keep_last}, _from, state),
    do: {:reply, do_compact(state.conn, table, keep_last), state}

  def handle_call(:log_id, _from, state), do: {:reply, fetch_log_id(state.conn), state}

  def handle_call({:handle_initial, path, query, context}, {from_pid, _ref}, state) do
    case do_handle_initial(state, path, query, context) do
      {:wait, shape, offset, timeout_ms} ->
        # Pre-register the caller as a subscriber so a notify that
        # fires after this reply but before the caller's receive is
        # still delivered. Monitor the caller so a dropped Plug
        # worker doesn't leak its subscription past the next notify.
        ref = Process.monitor(from_pid)

        new_state = %{
          state
          | subscribers: MapSet.put(state.subscribers, from_pid),
            monitors: Map.put(state.monitors, from_pid, ref)
        }

        {:reply, {:wait, shape, offset, timeout_ms}, new_state}

      reply ->
        {:reply, reply, state}
    end
  end

  def handle_call({:replay_for_handle, %Shape{} = shape, offset}, _from, state) do
    response =
      case do_replay(state.conn, shape, offset, state.replay_limit) do
        {:ok, body} -> {200, replay_to_body(body)}
        {:error, :resync_required} -> {409, %{"error" => "resync_required"}}
        {:error, _} -> {500, %{"error" => "internal"}}
      end

    {:reply, response, state}
  end

  def handle_call(:subscribe_change, {pid, _ref}, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  @impl true
  def handle_cast({:unsubscribe_change, pid}, state) do
    case Map.get(state.monitors, pid) do
      nil -> :ok
      ref -> Process.demonitor(ref, [:flush])
    end

    {:noreply,
     %{
       state
       | subscribers: MapSet.delete(state.subscribers, pid),
         monitors: Map.delete(state.monitors, pid)
     }}
  end

  def handle_cast(:wake_all_waiters, state) do
    {:noreply, notify(state)}
  end

  def handle_cast(:electrolite_shutdown, state) do
    # Send a distinct shutdown message to every subscriber so they
    # can return a clean response without calling back into the
    # GenServer. Mark state.stopped so subsequent handle_initial
    # calls also short-circuit.
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:electrolite_shutdown, self()})
    end)

    Enum.each(state.monitors, fn {_pid, ref} -> Process.demonitor(ref, [:flush]) end)

    {:noreply,
     %{state | stopped: true, subscribers: MapSet.new(), monitors: %{}}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Clean up subscription for a Plug worker (or any caller) that
    # died before its receive completed.
    {:noreply,
     %{
       state
       | subscribers: MapSet.delete(state.subscribers, pid),
         monitors: Map.delete(state.monitors, pid)
     }}
  end

  defp notify(state) do
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:electrolite_change, self()})
    end)

    Enum.each(state.monitors, fn {_pid, ref} -> Process.demonitor(ref, [:flush]) end)
    %{state | subscribers: MapSet.new(), monitors: %{}}
  end

  # ---- handle dispatch ----

  defp do_handle_initial(state, path, query, context) do
    with {:ok, route} <- parse_route(state.prefix, path, query),
         {:ok, def} <- Map.fetch(state.shapes, route.name) |> wrap_fetch(:not_found),
         {:ok, params} <- bind_params(def, route.params),
         build_ctx <- %{params: params, context: context},
         scope <- if(def.scope, do: def.scope.(build_ctx), else: ""),
         :ok <- run_authorize(def, params, context, scope) do
      predicate = if def.where, do: def.where.(build_ctx), else: all_pred()

      shape = %Shape{
        table: def.table,
        columns: def.columns,
        predicate: predicate,
        auth_scope: scope,
        schema_version: def.schema_version || 1
      }

      current_handle = shape_handle(shape)

      with {:ok, current_log_id} <- fetch_log_id(state.conn),
           :ok <- check_log_id(route, current_log_id),
           :ok <- check_handle(route, current_handle) do
        cond do
          route.offset < 0 ->
            case do_snapshot(state.conn, shape) do
              {:ok, snap} -> {:reply, {200, snapshot_to_body(snap)}}
              {:error, :resync_required} -> {:reply, {409, %{"error" => "resync_required"}}}
              {:error, {:bad_input, msg}} -> {:reply, {400, %{"error" => "bad_request", "detail" => msg}}}
              {:error, _} -> {:reply, {500, %{"error" => "internal"}}}
            end

          true ->
            case do_replay(state.conn, shape, route.offset, state.replay_limit, route.replica) do
              {:ok, body} ->
                if route.live and body.messages == [] and body.up_to_date do
                  # Tell the caller to wait; the GenServer must stay
                  # free to receive writes that would unblock the wait.
                  {:wait, shape, route.offset, state.live_timeout_ms}
                else
                  {:reply, {200, replay_to_body(body)}}
                end

              {:error, :resync_required} ->
                {:reply, {409, %{"error" => "resync_required"}}}

              {:error, {:bad_input, msg}} ->
                {:reply, {400, %{"error" => "bad_request", "detail" => msg}}}

              {:error, _} ->
                {:reply, {500, %{"error" => "internal"}}}
            end
        end
      else
        {:error, :resync_required} -> {:reply, {409, %{"error" => "resync_required"}}}
        {:error, _} -> {:reply, {500, %{"error" => "internal"}}}
      end
    else
      :not_found -> {:reply, {404, %{"error" => "shape_not_found"}}}
      {:error, :not_found} -> {:reply, {404, %{"error" => "shape_not_found"}}}
      {:error, :bad_offset} -> {:reply, {400, %{"error" => "bad_request", "detail" => "offset must be an integer"}}}
      {:error, _} -> {:reply, {500, %{"error" => "internal"}}}
    end
  end

  defp wrap_fetch(:error, tag), do: {:error, tag}
  defp wrap_fetch({:ok, v}, _), do: {:ok, v}

  defp bind_params(%ShapeDef{params: names}, given) do
    if length(names) == length(given) do
      {:ok, names |> Enum.zip(given) |> Map.new()}
    else
      {:error, :not_found}
    end
  end

  defp run_authorize(%ShapeDef{authorize: nil}, _, _, _), do: :ok

  defp run_authorize(%ShapeDef{authorize: fun}, params, context, scope) do
    if fun.(%{params: params, context: context, scope: scope}) do
      :ok
    else
      {:error, :not_found}
    end
  end

  defp check_log_id(%{offset: o}, _) when o < 0, do: :ok
  # On any replay/live request (offset >= 0) the client MUST present its
  # log_id so we can prove it is reading the same log history. A missing
  # log_id is treated as a forced resync rather than silently accepted.
  defp check_log_id(%{log_id: nil}, _), do: {:error, :resync_required}

  defp check_log_id(%{log_id: lid}, current) do
    if lid == current, do: :ok, else: {:error, :resync_required}
  end

  defp check_handle(%{offset: o}, _) when o < 0, do: :ok
  defp check_handle(%{shape_handle: nil}, _), do: :ok

  defp check_handle(%{shape_handle: h}, current) do
    if h == current, do: :ok, else: {:error, :resync_required}
  end

  defp parse_route(prefix, path, query) do
    p = prefix <> "/"

    if String.starts_with?(path, p) do
      rest = String.replace_prefix(path, p, "")
      parts = rest |> String.split("/") |> Enum.reject(&(&1 == ""))

      if parts == [] do
        :not_found
      else
        qp = parse_qs(query)

        case parse_offset(Map.get(qp, "offset")) do
          {:ok, offset} ->
            replica = if Map.get(qp, "replica") == "diff", do: :diff, else: :full

            {:ok,
             %{
               name: hd(parts),
               params: tl(parts),
               offset: offset,
               live: Map.get(qp, "live") == "true",
               log_id: Map.get(qp, "log_id"),
               shape_handle: Map.get(qp, "shape_handle"),
               replica: replica
             }}

          :error ->
            {:error, :bad_offset}
        end
      end
    else
      :not_found
    end
  end

  defp parse_offset(nil), do: {:ok, -1}

  defp parse_offset(value) do
    case Integer.parse(value) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  defp parse_qs(nil), do: %{}

  defp parse_qs(query) do
    query
    |> String.trim_leading("?")
    |> URI.decode_query()
  end

  defp snapshot_to_body(snap) do
    %{
      "type" => "snapshot",
      "log_id" => snap.log_id,
      "shape_handle" => snap.shape_handle,
      "key_columns" => snap.key_columns,
      "rows" => snap.rows,
      "offset" => snap.offset,
      "up_to_date" => true
    }
  end

  defp replay_to_body(body) do
    base = %{
      "type" => "replay",
      "log_id" => body.log_id,
      "shape_handle" => body.shape_handle,
      "messages" => Enum.map(body.messages, &message_to_body/1),
      "offset" => body.offset,
      "up_to_date" => body.up_to_date
    }

    case Map.get(body, :replica) do
      nil -> base
      r -> Map.put(base, "replica", r)
    end
  end

  defp message_to_body(m) do
    base = %{
      "type" => Atom.to_string(m.type),
      "batch_id" => m.batch_id,
      "key" => m.key,
      "offset" => m.offset
    }

    if Map.has_key?(m, :value), do: Map.put(base, "value", m.value), else: base
  end

  # ---- bootstrap / triggers ----

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

    # A crashed write_batch may have left current_batch_id behind;
    # clear it so the next unrelated write does not inherit a dead
    # batch_id.
    :ok = exec(conn, "DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'", [])
    :ok
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

  # ---- snapshot / replay / compact ----

  defp do_snapshot(conn, %Shape{} = shape) do
    with {:ok, info} <- watched_info(conn, shape.table),
         {:ok, np} <- normalize_predicate_tree(info, shape.predicate) do
      shape = %{shape | predicate: np}
      {where_sql, args} = compile_predicate(np)

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

  defp do_replay(conn, %Shape{} = shape, offset, limit, replica \\ :full) do
    with {:ok, info} <- watched_info(conn, shape.table),
         {:ok, np} <- normalize_predicate_tree(info, shape.predicate) do
      shape = %{shape | predicate: np}
      {:ok, retained} = retained_offset(conn, shape.table)

      if offset < retained do
        {:error, :resync_required}
      else
        limit = max(1, limit)
        {:ok, rows} = read_log_page(conn, shape.table, offset, limit)
        latest = if rows == [], do: offset, else: List.last(rows).seq

        {:ok, newer} =
          query(
            conn,
            "SELECT 1 FROM _electrolite_log WHERE table_name = ? AND seq > ? LIMIT 1",
            [shape.table, latest]
          )

        messages = rows |> Enum.flat_map(&messages_for(shape.predicate, &1, replica))
        {:ok, log_id} = fetch_log_id(conn)

        body = %{
          log_id: log_id,
          shape_handle: shape_handle(shape),
          messages: messages,
          offset: latest,
          up_to_date: newer == []
        }

        body = if replica == :diff, do: Map.put(body, :replica, "diff"), else: body
        {:ok, body}
      end
    end
  end

  defp do_compact(conn, table, keep_last) do
    {:ok, candidate_rows} =
      query(
        conn,
        "SELECT seq FROM _electrolite_log WHERE table_name = ? ORDER BY seq DESC LIMIT 1 OFFSET ?",
        [table, max(0, keep_last)]
      )

    # When the table has fewer than keep_last rows there is nothing to
    # drop. Fall back to 0 (keep everything) — NOT the global high-water
    # mark. Using the global MAX(seq) would delete this quiet table's
    # entire log and force its subscribers to resync just because some
    # *other* table advanced the sequence.
    candidate =
      case candidate_rows do
        [[s] | _] -> s
        _ -> 0
      end

    # Never regress the watermark: a prior compaction may have set a
    # higher retained offset, and lowering it would let a client
    # silently skip already-deleted offsets.
    {:ok, existing} = retained_offset(conn, table)
    retained = max(existing, candidate)

    {:ok, _} = query(conn, "DELETE FROM _electrolite_log WHERE table_name = ? AND seq <= ?", [table, retained])
    {:ok, deleted} = Sqlite3.changes(conn)

    :ok =
      exec(
        conn,
        "INSERT INTO _electrolite_meta (key, value) VALUES (?, ?) " <>
          "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        ["retained_offset:" <> table, Integer.to_string(retained)]
      )

    {:ok, %{retained_offset: retained, deleted_rows: deleted}}
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
          # Extend until the trailing batch finishes or until we hit
          # the safety cap. Without the cap, a 10M-row batch would
          # force replay to load every row in one response.
          extension_cap = max(limit, 1) * 10

          {:ok, more} =
            query(
              conn,
              "SELECT seq, batch_id, op, pk_json, old_pk_json, new_pk_json, old_json, new_json " <>
                "FROM _electrolite_log WHERE table_name = ? AND seq > ? AND batch_id = ? ORDER BY seq LIMIT ?",
              [table, last.seq, last.batch_id, extension_cap]
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

  # ---- predicate eval ----

  defp messages_for(predicate, row, replica) do
    old_match = predicate_matches(predicate, row.old_row)
    new_match = predicate_matches(predicate, row.new_row)
    old_key = row.old_pk || row.pk
    new_key = row.new_pk || row.pk

    cond do
      not old_match and new_match and row.new_row != nil ->
        # Predicate-transition INSERT always carries full row.
        [build_msg(:insert, row, new_key, row.new_row)]

      old_match and new_match and row.new_row != nil ->
        if old_key == new_key do
          value =
            case replica do
              :diff when row.old_row != nil ->
                diff_row(row.old_row, row.new_row)

              _ ->
                row.new_row
            end

          if value == %{} do
            []
          else
            [build_msg(:update, row, new_key, value)]
          end
        else
          [build_msg(:delete, row, old_key, nil), build_msg(:insert, row, new_key, row.new_row)]
        end

      old_match and not new_match ->
        [build_msg(:delete, row, old_key, nil)]

      true ->
        []
    end
  end

  defp diff_row(old, new) do
    new
    |> Enum.filter(fn {k, v} -> Map.get(old, k) != v end)
    |> Map.new()
  end

  defp build_msg(kind, row, key, value) do
    base = %{type: kind, batch_id: row.batch_id, key: key, offset: row.seq}
    if value, do: Map.put(base, :value, value), else: base
  end

  @doc """
  Public predicate matcher. Used by the conformance harness to
  compare in-process matching against SQL `WHERE` results.
  """
  def match?(predicate, row) when is_map(row), do: predicate_matches(predicate, row)
  def match?(_, _), do: false

  @doc """
  Parse a JSON-decoded predicate (wire format, with string keys) into
  the engine's internal predicate representation.
  """
  def predicate_from_json(%{"type" => "all"}), do: %{type: :all}

  def predicate_from_json(%{"type" => "eq", "column" => c, "value" => v}),
    do: %{type: :eq, column: c, value: v}

  def predicate_from_json(%{"type" => op, "column" => c, "value" => v})
      when op in ["gt", "lt", "gte", "lte"] do
    %{type: String.to_atom(op), column: c, value: v}
  end

  def predicate_from_json(%{"type" => "in", "column" => c, "values" => vs}),
    do: %{type: :in, column: c, values: vs}

  def predicate_from_json(%{"type" => "and", "predicates" => ps}),
    do: %{type: :and, predicates: Enum.map(ps, &predicate_from_json/1)}

  def predicate_from_json(%{"type" => "or", "predicates" => ps}),
    do: %{type: :or, predicates: Enum.map(ps, &predicate_from_json/1)}

  def predicate_from_json(%{"type" => "not", "predicate" => inner}),
    do: %{type: :not, predicate: predicate_from_json(inner)}

  defp predicate_matches(_p, nil), do: false
  defp predicate_matches(%{type: :all}, _row), do: true
  defp predicate_matches(%{type: :eq, column: c, value: v}, row), do: Map.get(row, c) == v

  defp predicate_matches(%{type: op, column: c, value: v}, row) when op in [:gt, :lt, :gte, :lte] do
    compare_scalar(Map.get(row, c), v, op)
  end

  defp predicate_matches(%{type: :in, column: c, values: vs}, row) do
    val = Map.get(row, c)
    Enum.any?(vs, &(&1 == val))
  end

  defp predicate_matches(%{type: :and, predicates: children}, row) do
    Enum.all?(children, &predicate_matches(&1, row))
  end

  defp predicate_matches(%{type: :or, predicates: children}, row) do
    Enum.any?(children, &predicate_matches(&1, row))
  end

  defp predicate_matches(%{type: :not, predicate: inner}, row) do
    not predicate_matches(inner, row)
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

  # ---- predicate compile ----

  defp compile_predicate(%{type: :all}), do: {"", []}
  defp compile_predicate(%{type: :eq, column: c, value: nil}), do: {"#{quote_ident(c)} IS NULL", []}
  defp compile_predicate(%{type: :eq, column: c, value: v}), do: {"#{quote_ident(c)} = ?", [v]}

  defp compile_predicate(%{type: op, column: _c, value: nil}) when op in [:gt, :lt, :gte, :lte],
    do: {"0", []}

  defp compile_predicate(%{type: op, column: c, value: v}) when op in [:gt, :lt, :gte, :lte] do
    {"#{quote_ident(c)} #{Map.fetch!(@range_ops, op)} ?", [v]}
  end

  defp compile_predicate(%{type: :in, column: c, values: values}) do
    {non_null, has_null} =
      Enum.reduce(values, {[], false}, fn
        nil, {acc, _} -> {acc, true}
        v, {acc, n} -> {[v | acc], n}
      end)

    non_null = Enum.reverse(non_null)
    parts = []
    args = []

    {parts, args} =
      if non_null != [] do
        placeholders = Enum.map_join(non_null, ",", fn _ -> "?" end)
        {parts ++ ["#{quote_ident(c)} IN (#{placeholders})"], args ++ non_null}
      else
        {parts, args}
      end

    parts =
      if has_null do
        parts ++ ["#{quote_ident(c)} IS NULL"]
      else
        parts
      end

    if parts == [] do
      {"0", []}
    else
      {Enum.map_join(parts, " OR ", fn p -> "(#{p})" end), args}
    end
  end

  defp compile_predicate(%{type: :and, predicates: children}) do
    {parts, args} =
      Enum.reduce(children, {[], []}, fn child, {ps, as} ->
        case compile_predicate(child) do
          {"", _} -> {ps, as}
          {w, a} -> {ps ++ ["(#{w})"], as ++ a}
        end
      end)

    {Enum.join(parts, " AND "), args}
  end

  defp compile_predicate(%{type: :or, predicates: []}) do
    # Empty OR is vacuously false → match nothing.
    {"0", []}
  end

  defp compile_predicate(%{type: :or, predicates: children}) do
    {parts, args, match_all} =
      Enum.reduce(children, {[], [], false}, fn child, {ps, as, ma} ->
        case compile_predicate(child) do
          # child is match-all → whole OR is match-all
          {"", _} -> {ps, as, true}
          {w, a} -> {ps ++ ["(#{w})"], as ++ a, ma}
        end
      end)

    cond do
      match_all -> {"", []}
      true -> {Enum.join(parts, " OR "), args}
    end
  end

  defp compile_predicate(%{type: :not, predicate: %{type: :eq, column: c, value: nil}}) do
    # Special case: NOT (col IS NULL) → col IS NOT NULL
    {"#{quote_ident(c)} IS NOT NULL", []}
  end

  defp compile_predicate(%{type: :not, predicate: inner}) do
    case compile_predicate(inner) do
      {"", _} -> {"0", []}
      {w, a} -> {"NOT (#{w})", a}
    end
  end

  # ---- introspection ----

  defp inspect_table(conn, table) do
    {:ok, rows} = query(conn, "PRAGMA table_info(#{quote_string(table)})", [])

    if rows == [] do
      {:error, "table #{table} does not exist"}
    else
      sorted = Enum.sort_by(rows, fn [cid | _] -> cid end)
      columns = Enum.map(sorted, fn [_, name | _] -> name end)

      column_types =
        sorted
        |> Enum.map(fn [_, name, type | _] -> {name, type || ""} end)
        |> Map.new()

      pk =
        sorted
        |> Enum.filter(fn row -> Enum.at(row, 5) > 0 end)
        |> Enum.sort_by(fn row -> Enum.at(row, 5) end)
        |> Enum.map(fn [_, name | _] -> name end)

      {:ok, %{columns: columns, pk: pk, column_types: column_types}}
    end
  end

  defp is_booleanish(decl) when is_binary(decl) do
    String.contains?(String.upcase(decl), "BOOL")
  end

  defp is_booleanish(_), do: false

  defp normalize_value(info, column, value) do
    if column not in info.columns do
      {:error, {:bad_input, "predicate column #{column} does not exist"}}
    else
      col_type = Map.get(info.column_types || %{}, column, "")

      cond do
        is_boolean(value) and is_booleanish(col_type) ->
          {:ok, if(value, do: 1, else: 0)}

        is_boolean(value) ->
          {:error, {:bad_input, "boolean predicates require BOOLEAN columns"}}

        true ->
          {:ok, value}
      end
    end
  end

  defp normalize_predicate_tree(_info, %{type: :all} = p), do: {:ok, p}

  defp normalize_predicate_tree(info, %{type: :eq, column: c, value: v}) do
    with {:ok, nv} <- normalize_value(info, c, v) do
      {:ok, %{type: :eq, column: c, value: nv}}
    end
  end

  defp normalize_predicate_tree(info, %{type: op, column: c, value: v})
       when op in [:gt, :lt, :gte, :lte] do
    cond do
      is_nil(v) ->
        {:error, {:bad_input, "range predicate #{op} requires a non-null value"}}

      true ->
        with {:ok, nv} <- normalize_value(info, c, v) do
          {:ok, %{type: op, column: c, value: nv}}
        end
    end
  end

  defp normalize_predicate_tree(info, %{type: :in, column: c, values: vs}) do
    nvs =
      Enum.reduce_while(vs, {:ok, []}, fn v, {:ok, acc} ->
        case normalize_value(info, c, v) do
          {:ok, nv} -> {:cont, {:ok, [nv | acc]}}
          err -> {:halt, err}
        end
      end)

    case nvs do
      {:ok, vs} -> {:ok, %{type: :in, column: c, values: Enum.reverse(vs)}}
      err -> err
    end
  end

  defp normalize_predicate_tree(info, %{type: :and, predicates: children}) do
    nchildren =
      Enum.reduce_while(children, {:ok, []}, fn c, {:ok, acc} ->
        case normalize_predicate_tree(info, c) do
          {:ok, nc} -> {:cont, {:ok, [nc | acc]}}
          err -> {:halt, err}
        end
      end)

    case nchildren do
      {:ok, cs} -> {:ok, %{type: :and, predicates: Enum.reverse(cs)}}
      err -> err
    end
  end

  defp normalize_predicate_tree(info, %{type: :or, predicates: children}) do
    nchildren =
      Enum.reduce_while(children, {:ok, []}, fn c, {:ok, acc} ->
        case normalize_predicate_tree(info, c) do
          {:ok, nc} -> {:cont, {:ok, [nc | acc]}}
          err -> {:halt, err}
        end
      end)

    case nchildren do
      {:ok, cs} -> {:ok, %{type: :or, predicates: Enum.reverse(cs)}}
      err -> err
    end
  end

  defp normalize_predicate_tree(info, %{type: :not, predicate: inner}) do
    with {:ok, ninner} <- normalize_predicate_tree(info, inner) do
      {:ok, %{type: :not, predicate: ninner}}
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

  defp retained_offset(conn, table) do
    case query(conn, "SELECT value FROM _electrolite_meta WHERE key = ?", ["retained_offset:" <> table]) do
      {:ok, [[v] | _]} -> {:ok, String.to_integer(v)}
      _ -> {:ok, 0}
    end
  end

  # ---- shape handle ----

  defp shape_handle(%Shape{} = shape) do
    body = %{
      "auth_scope" => shape.auth_scope || "",
      "columns" => Enum.sort(shape.columns),
      "predicate" => predicate_to_json(shape.predicate),
      "schema_version" => shape.schema_version || 1,
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

  defp predicate_to_json(%{type: :in, column: c, values: values}) do
    sorted =
      values
      |> Enum.map(&Jason.encode!/1)
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map(&Jason.decode!/1)

    %{"type" => "in", "column" => c, "values" => sorted}
  end

  defp predicate_to_json(%{type: :and, predicates: children}) do
    sorted =
      children
      |> Enum.map(&predicate_to_json/1)
      |> Enum.sort_by(&Jason.encode!/1)

    %{"type" => "and", "predicates" => sorted}
  end

  defp predicate_to_json(%{type: :or, predicates: children}) do
    sorted =
      children
      |> Enum.map(&predicate_to_json/1)
      |> Enum.sort_by(&Jason.encode!/1)

    %{"type" => "or", "predicates" => sorted}
  end

  defp predicate_to_json(%{type: :not, predicate: inner}) do
    %{"type" => "not", "predicate" => predicate_to_json(inner)}
  end

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

  # ---- sql plumbing ----

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

  defp random_hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

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
