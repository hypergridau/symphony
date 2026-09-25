defmodule SymphonyElixir.RKE2Job.Client do
  @moduledoc "Typed port for the small Kubernetes Job surface used by the beta provider."

  @type job :: map()
  @type namespace :: String.t()
  @type name :: String.t()
  @type uid :: String.t()

  @callback create_job(namespace(), job(), term()) :: {:ok, job()} | {:error, term()}
  @callback get_job(namespace(), name(), term()) :: {:ok, job()} | {:error, :not_found | term()}
  @callback delete_job(namespace(), name(), uid(), term()) :: :ok | {:error, term()}
end
