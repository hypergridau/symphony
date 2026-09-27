defmodule SymphonyElixir.RKE2Job.AuthSlotLeaseGuard do
  @moduledoc """
  Trusted host port for the durable, exclusive Codex OAuth slot lease.

  The host must reserve the selected slot before Job creation, bind the exact
  server UID before allocation is ready, recheck it before activation, and
  release only after the old Job and Pods are gone and auth cache durability is
  confirmed. No production implementation is supplied by this source module.
  """

  @callback reserve(map(), map(), term()) :: :ok | {:held, term()} | {:error, term()}
  @callback bind_uid(map(), map(), map(), term()) :: :ok | {:held, term()} | {:error, term()}
  @callback authorize(map(), map(), map(), term()) :: :ok | {:held, term()} | {:error, term()}
  @callback release(map(), map(), map(), term()) :: :ok | {:held, term()} | {:error, term()}
end
