defmodule Electrolite.TestServer do
  @moduledoc """
  Tiny test-only HTTP server that exposes the Elixir engine over the
  Electrolite protocol so cross-language client tests can drive a real
  browser ShapeClient against it.

  Run from `engines/elixir/`:

      mix run --no-halt server/run.exs --port 5104 --db /tmp/x/app.db
  """

  use Plug.Router

  @engine :electrolite_test_engine

  plug :match
  plug :dispatch

  def start(port) do
    Plug.Cowboy.http(__MODULE__, [], port: port)
  end

  def engine_name, do: @engine

  get "/electrolite/*_rest" do
    query = conn.query_string

    if String.contains?(get_req_header(conn, "accept") |> Enum.join(","), "text/event-stream") do
      stream_sse(conn, query)
    else
      {status, body} =
        Electrolite.handle(@engine, conn.request_path, query, %{projects: ["p1", "p2"]})

      etag = compute_etag(body, status)
      cache_control = compute_cache_control(query, status)
      if_none_match = get_req_header(conn, "if-none-match") |> List.first()

      conn =
        conn
        |> put_resp_header("access-control-allow-origin", "*")
        |> put_resp_header("vary", "authorization")

      conn = if etag, do: put_resp_header(conn, "etag", etag), else: conn
      conn = if cache_control, do: put_resp_header(conn, "cache-control", cache_control), else: conn

      if etag && if_none_match == etag do
        send_resp(conn, 304, "")
      else
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(status, Jason.encode!(body))
      end
    end
  end

  defp compute_etag(body, 200) when is_map(body) do
    case {body["shape_handle"], body["offset"]} do
      {nil, _} -> nil
      {h, o} -> ~s("#{h}-#{o}")
    end
  end

  defp compute_etag(_, _), do: nil

  defp compute_cache_control(query, 200) do
    qp = URI.decode_query(query)

    cond do
      Map.get(qp, "live") == "true" -> "no-store"
      (Map.get(qp, "offset") || "-1") |> safe_int() |> Kernel.>=(0) -> "public, max-age=31536000, immutable"
      true -> "public, max-age=5"
    end
  end

  defp compute_cache_control(_, _), do: nil

  defp safe_int(s) do
    case Integer.parse(s) do
      {n, ""} -> n
      _ -> -1
    end
  end

  defp stream_sse(conn, query) do
    conn =
      conn
      |> put_resp_content_type("text/event-stream")
      |> put_resp_header("cache-control", "no-cache")
      |> put_resp_header("access-control-allow-origin", "*")
      |> send_chunked(200)

    ctx = %{projects: ["p1", "p2"]}
    {status, body} = Electrolite.handle(@engine, conn.request_path, query, ctx)

    if status != 200 do
      _ = sse_chunk(conn, "error", body)
      conn
    else
      offset_q = URI.decode_query(query) |> Map.get("offset", "-1")
      kind = if offset_q == "-1", do: "snapshot", else: "replay"
      conn = sse_chunk(conn, kind, body) |> elem(1)
      sse_loop(conn, body, ctx)
    end
  end

  defp sse_loop(conn, body, ctx) do
    next_query =
      "offset=#{body["offset"]}&log_id=#{body["log_id"]}&shape_handle=#{body["shape_handle"]}&live=true"

    {status, next} = Electrolite.handle(@engine, conn.request_path, next_query, ctx)

    case status do
      200 ->
        conn =
          if next["messages"] != [] do
            sse_chunk(conn, "replay", next) |> elem(1)
          else
            conn
          end

        case Plug.Conn.chunk(conn, ": ping\n\n") do
          {:ok, conn} ->
            sse_loop(conn, next, ctx)

          {:error, _} ->
            conn
        end

      _ ->
        sse_chunk(conn, "error", next) |> elem(1)
    end
  end

  defp sse_chunk(conn, event, data) do
    payload = "event: #{event}\ndata: #{Jason.encode!(data)}\n\n"

    case Plug.Conn.chunk(conn, payload) do
      {:ok, conn} -> {:ok, conn}
      {:error, _} = err -> {err, conn}
    end
  end

  post "/_test/exec" do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    payload = Jason.decode!(body)
    :ok = Electrolite.execute(@engine, payload["sql"], payload["args"] || [])

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, ~s({"ok":true}))
  end

  post "/_test/write_batch" do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    payload = Jason.decode!(body)

    statements =
      Enum.map(payload["statements"], fn [sql, args] -> {sql, args} end)

    :ok = Electrolite.write_batch(@engine, statements)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, ~s({"ok":true}))
  end

  post "/_test/seed" do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    payload = Jason.decode!(body)
    :ok = Electrolite.execute_batch(@engine, payload["sql"])

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, ~s({"ok":true}))
  end

  post "/_test/match-predicate" do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    payload = Jason.decode!(body)
    predicate = Electrolite.predicate_from_json(payload["predicate"])

    matched =
      payload["rows"]
      |> Enum.filter(&Electrolite.match?(predicate, &1))
      |> Enum.map(& &1["id"])

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{matched_ids: matched}))
  end

  match _ do
    send_resp(conn, 404, ~s({"error":"not_found"}))
  end
end
