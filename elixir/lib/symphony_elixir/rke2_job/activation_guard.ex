defmodule SymphonyElixir.RKE2Job.ActivationGuard do
  @moduledoc """
  Host-owned authorization port immediately before a suspended Job is activated.

  The implementation must revalidate admission and credential readiness for the
  exact signed assignment, allocation and activation key. No production guard is
  provided by this source-only module.
  """

  @callback authorize(map(), map(), String.t(), term()) :: :ok | {:held, term()} | {:error, term()}
end
