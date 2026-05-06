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

    {status, body} =
      Electrolite.handle(@engine, conn.request_path, query, %{projects: ["p1", "p2"]})

    conn
    |> put_resp_content_type("application/json")
    |> put_resp_header("access-control-allow-origin", "*")
    |> send_resp(status, Jason.encode!(body))
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

  match _ do
    send_resp(conn, 404, ~s({"error":"not_found"}))
  end
end
