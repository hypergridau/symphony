defmodule SymphonyElixir.RKE2Job.Client do
  @moduledoc "Typed port for exact Kubernetes Job mutations and namespace Pod cleanup readback."

  @type job :: map()
  @type namespace :: String.t()
  @type name :: String.t()
  @type uid :: String.t()

  @callback create_job(namespace(), job(), term()) :: {:ok, job()} | {:error, term()}
  @callback get_job(namespace(), name(), term()) :: {:ok, job()} | {:error, :not_found | term()}
  @callback list_pods(namespace(), term()) :: {:ok, [map()]} | {:error, term()}
  @callback activate_job(namespace(), name(), uid(), String.t(), term()) :: {:ok, job()} | {:error, term()}
  @callback delete_job(namespace(), name(), uid(), term()) :: :ok | {:error, term()}
end
