defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedEpoch do
  @moduledoc "Pins the one observed unsigned epoch-1 failure; it grants no release authority."

  @hashes %{
    "provider-held-readback.json" => "bbbebf8d395bf77e3a28bcc23afbc08591a6225e4d0372c899605a8de6af69fd",
    "issuer-input.json" => "4a83b1230f2f300ed6a12985656e4f5f4d55ab175c2de78a8635c677527e50e4",
    "reviewed-preflight.json" => "3ba3ac39f4c85d194e6a9f91e5a773182ef597a94523887dd337978c910588f4",
    "started.json" => "8b7a2aea630a5e0939e165a82a1ccb1b5be5cfd0bd6f9d4d4363db6803eacbd4",
    "manifest.json" => "005b09ce22e0d3c6571a148f9dfca5e89de9bb7cefc029e0aba707d441ca6b3d"
  }
  @seal "98c5d2b5bd5385a6560b310059c5da1a722a55061ed48a1fbde8cadf6f246ab9"
  @seal_path "/srv/dahlia-runner-state/evidence/hgs740-reconciliation-20261001/failed-epoch-1-seal-v1.json"

  @spec predecessor_binding() :: map()
  def predecessor_binding, do: %{"epoch" => "epoch-1", "evidenceSHA256" => @hashes, "failureSealSHA256" => @seal}

  @spec valid?(map()) :: boolean()
  def valid?(metadata), do: metadata["predecessorEpoch"] == predecessor_binding()

  @spec verify(map(), String.t(), function()) :: :ok | {:error, :invalid_reconciliation_epoch}
  def verify(metadata, directory, read), do: verify_with(metadata, directory, read, @hashes, @seal)

  defp verify_with(metadata, directory, read, hashes, seal) do
    expected = %{"epoch" => "epoch-1", "evidenceSHA256" => hashes, "failureSealSHA256" => seal}

    with true <- metadata["predecessorEpoch"] == expected,
         {:ok, seal_bytes} <- read.(@seal_path, 262_144),
         true <- digest(seal_bytes) == seal,
         true <-
           Enum.all?(hashes, fn {name, hash} ->
             matches_file?(read, Path.join([directory, "reconciliation", "epoch-1", name]), hash)
           end) do
      :ok
    else
      _ -> {:error, :invalid_reconciliation_epoch}
    end
  end

  defp matches_file?(read, path, hash) do
    case read.(path, 1_048_576) do
      {:ok, bytes} -> digest(bytes) == hash
      _ -> false
    end
  end

  if Mix.env() == :test do
    @doc false
    @spec verify_for_test(map(), String.t(), function(), map(), String.t()) :: :ok | {:error, term()}
    def verify_for_test(metadata, directory, read, hashes, seal), do: verify_with(metadata, directory, read, hashes, seal)
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
