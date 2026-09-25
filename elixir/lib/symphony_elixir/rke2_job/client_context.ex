defmodule SymphonyElixir.RKE2Job.ClientContext do
  @moduledoc """
  Fakeable boundary for obtaining host-owned Kubernetes HTTP settings.

  Implementations may resolve short-lived settings for one operation. This port
  never loads credentials itself, and callers must not persist its result.
  """

  @callback client_context(map(), :allocate | :delete, String.t(), term()) ::
              {:ok, term()} | {:error, term()}
end
