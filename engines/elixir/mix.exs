defmodule Electrolite.MixProject do
  use Mix.Project

  def project do
    [
      app: :electrolite,
      version: "0.0.1",
      elixir: "~> 1.15",
      start_permanent: false,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:exqlite, "~> 0.23"},
      {:jason, "~> 1.4"},
      {:plug, "~> 1.16"},
      {:plug_cowboy, "~> 2.7"}
    ]
  end
end
