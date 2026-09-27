defmodule SymphonyElixir.RKE2Job.PrepareAckGuard do
  @moduledoc """
  Trusted host port that verifies Dahlia durably acknowledged this exact abort
  observation before Symphony removes the suspended Job.
  """

  @callback verify(String.t(), String.t(), map(), map(), term()) ::
              :ok | {:held, term()} | {:error, term()}
end
