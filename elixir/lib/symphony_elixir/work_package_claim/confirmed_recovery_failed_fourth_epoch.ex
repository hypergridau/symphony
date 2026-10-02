defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedFourthEpoch do
  @moduledoc "Pins signed epoch 4, which failed before a local transition or provider release."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedSignedEpoch, as: Third

  @hashes %{
    "candidate.json" => "37077aedd8a6126291781e80946e697444f333ea8ea37aeab3d88827f48a6684",
    "confirmed-root-envelope.json" => "f1bb150c9a3d0770eaee5b8e0994ce91b51e2672fa6ec10a0fe5fcfeebcd1fe9",
    "issued-envelope.json" => "f1bb150c9a3d0770eaee5b8e0994ce91b51e2672fa6ec10a0fe5fcfeebcd1fe9",
    "issuer-input.json" => "90dc5c195d9b30470306fbaba0845b05be511bb4b1a44a3ba7294d6357fb174b",
    "manifest.json" => "b775fed3102b39f623f6d64a2da899788e80b441275f06f4dec74101d4152aac",
    "provider-held-readback.json" => "5a93a3ba802ceb24ef63af435aabc642d131a76c0f86e973dabc86f9af06e9d8",
    "reviewed-preflight.json" => "a6d3f7fa0118ee15eae6967deb27ace59363a404bee7a846a4272e54a0a3d0bf",
    "started.json" => "f957b23692277fede325b426dc229dbbc80b4dcb89c05f9a0e04d635001298d8"
  }
  @base %{
    "candidate.json" => "e27b511837e55faece08bf2758017fef59bb26bdaa907239fa0f79aebf99576c",
    "confirmed-root-envelope.json" => "27d273cac9fce9ec42b0f6dbc84e8fb94e5ee6ca39b3e26f5c1f9c3384ba75e6"
  }
  @seal "11070f20c8918d524bdd0f24d69b69e0de1866aadc88f601002e15c149825fd6"
  @seal_path "/srv/dahlia-runner-state/evidence/hgs740-reconciliation-20261001/failed-signed-epoch-4-seal-v1.json"

  @spec predecessor_binding() :: map()
  def predecessor_binding,
    do: %{"epoch" => "epoch-4", "evidenceSHA256" => @hashes, "baseSignedOutputSHA256" => @base, "failureSealSHA256" => @seal}

  @spec valid?(map()) :: boolean()
  def valid?(metadata), do: metadata["signedPredecessorEpoch4"] == predecessor_binding() and Third.valid?(metadata)

  @spec verify(map(), String.t(), function()) :: :ok | {:error, :invalid_reconciliation_epoch}
  def verify(metadata, directory, read),
    do: verify_with(metadata, directory, read, predecessor_binding(), &Third.verify/3)

  defp verify_with(metadata, directory, read, binding, ancestor) do
    with true <- metadata["signedPredecessorEpoch4"] == binding and Third.valid?(metadata),
         :ok <- ancestor.(metadata, directory, read),
         {:ok, seal_bytes} <- read.(@seal_path, 262_144),
         true <- digest(seal_bytes) == binding["failureSealSHA256"],
         true <- matches_all?(read, Path.join([directory, "reconciliation", "epoch-4"]), binding["evidenceSHA256"]),
         true <- matches_all?(read, directory, binding["baseSignedOutputSHA256"]),
         {:ok, manifest_bytes} <- read.(Path.join([directory, "reconciliation", "epoch-4", "manifest.json"]), 262_144),
         {:ok, manifest} <- Jason.decode(manifest_bytes),
         true <- Evidence.canonical_json(manifest) == manifest_bytes,
         true <- Third.valid?(manifest) do
      :ok
    else
      _ -> {:error, :invalid_reconciliation_epoch}
    end
  rescue
    _ -> {:error, :invalid_reconciliation_epoch}
  end

  @doc "Verifies the pinned failed signature at its recorded issuance, never as current release authority."
  @spec verify_historical_signature(String.t(), function(), function()) :: :ok | {:error, term()}
  def verify_historical_signature(directory, read, read_key),
    do: verify_historical_signature_with(directory, read, read_key, @hashes, &Evidence.verify/3)

  defp verify_historical_signature_with(directory, read, read_key, hashes, verify) do
    epoch = Path.join([directory, "reconciliation", "epoch-4"])

    with {:ok, candidate} <- read.(Path.join(epoch, "candidate.json"), 1_048_576),
         true <- digest(candidate) == hashes["candidate.json"],
         {:ok, observation} <- Jason.decode(candidate),
         true <- Evidence.canonical_json(observation) == candidate,
         {:ok, envelope} <- read.(Path.join(epoch, "issued-envelope.json"), 1_048_576),
         true <- digest(envelope) == hashes["issued-envelope.json"],
         {:ok, wire} <- Jason.decode(envelope),
         {:ok, payload_bytes} <- Base.url_decode64(wire["payload"], padding: false),
         {:ok, payload} <- Jason.decode(payload_bytes),
         {:ok, issued, 0} <- DateTime.from_iso8601(payload["issuedAt"]),
         {:ok, public_key} <- read_key.(),
         {:ok, verified} <-
           verify.(envelope, public_key, %{
             pool: "hypergrid-gitops",
             issue_id: "f77e349e-21d9-4bdf-bad3-ce08b302e7e8",
             generation: 2,
             reservation_id: "workpkgreservation_e19008ccb2764fe79ca68bf500d20a1f",
             assignment_sha256: nil,
             assignment_snapshot_state: "absent",
             nonce: "370369d7-dc2f-441a-ba8d-fa9885a30a4a",
             claim_journal_sha256: observation["claimJournalSHA256"],
             fence_sha256: observation["fenceSHA256"],
             responsibility_graph_sha256: observation["responsibilityGraphSHA256"],
             now_ms: DateTime.to_unix(issued, :millisecond)
           }),
         true <- verified["observation"] == observation do
      :ok
    else
      _ -> {:error, :invalid_reconciliation_epoch}
    end
  rescue
    _ -> {:error, :invalid_reconciliation_epoch}
  end

  if Mix.env() == :test do
    @doc false
    @spec historical_signature_for_test(String.t(), function(), function(), map(), function()) :: :ok | {:error, term()}
    def historical_signature_for_test(directory, read, read_key, hashes, verify),
      do: verify_historical_signature_with(directory, read, read_key, hashes, verify)

    @doc false
    @spec verify_for_test(map(), String.t(), function(), map(), function()) :: :ok | {:error, term()}
    def verify_for_test(metadata, directory, read, binding, ancestor),
      do: verify_with(metadata, directory, read, binding, ancestor)
  end

  defp matches_all?(read, directory, hashes) do
    Enum.all?(hashes, fn {name, hash} ->
      case read.(Path.join(directory, name), 1_048_576) do
        {:ok, bytes} -> digest(bytes) == hash
        _ -> false
      end
    end)
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
