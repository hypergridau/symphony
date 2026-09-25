defmodule SymphonyElixir.ManagedExecutor.Journal do
  @moduledoc """
  Durable compare-and-swap port for one assignment lifecycle record.

  A journal implementation must make `compare_and_swap/4` atomic across callers.
  The lifecycle module checkpoints before every operation that can have an
  external effect.
  """

  @callback load(String.t(), term()) :: {:ok, map() | nil} | {:error, term()}
  @callback compare_and_swap(String.t(), non_neg_integer(), map(), term()) :: :ok | {:error, term()}
end
