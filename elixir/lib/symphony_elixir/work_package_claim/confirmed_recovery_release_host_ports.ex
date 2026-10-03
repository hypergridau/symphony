defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseHostPorts do
  @moduledoc "Connects the release coordinator to existing native custody, locking, publication and bounded external reads."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryCore, as: Core
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryKubernetes, as: Kubernetes
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseProtocol, as: Protocol
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseTransport, as: Transport
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost, as: Host

  @spec build(struct(), map(), map()) :: map()
  def build(context, enrollment, provider) do
    ports(context, enrollment, %{
      with_lock: fn fun -> Host.with_pool_lock(context, fun) end,
      approval: fn binding, id -> Transport.approval(binding, id, provider) end,
      held: fn binding -> Transport.held(binding, provider) end,
      kubernetes: &Kubernetes.observe_without_assignment_snapshot/2
    })
  end

  if Mix.env() == :test do
    @doc false
    @spec with_test_boundaries(struct(), map(), map()) :: map()
    def with_test_boundaries(context, enrollment, external), do: ports(context, enrollment, external)
  end

  defp ports(context, enrollment, external) do
    snapshot = fn -> Core.release_only_snapshot(context) end

    %{
      with_lock: external.with_lock,
      snapshot: snapshot,
      approval: fn id ->
        with {:ok, saved} <- snapshot.(),
             {:ok, binding} <- Protocol.binding(saved, enrollment.source_heads),
             do: external.approval.(binding, id)
      end,
      now: context.host_ops.now_ms,
      sign: context.host_ops.sign_recovery_payload,
      read: fn name -> Core.release_only_artifact(context, name, nil) end,
      create: fn name, bytes -> publish(context, enrollment, external, name, bytes) end,
      collect: fn binding -> collect(context, enrollment, external, binding) end
    }
  end

  defp publish(context, enrollment, external, name, bytes) do
    guard = fn -> publication_guard(context, enrollment, external, name, bytes) end
    write = context.host_ops.raw_write

    guarded = %{
      context
      | host_ops:
          Map.put(context.host_ops, :raw_write, fn file, data ->
            with :ok <- guard.(), do: write.(file, data)
          end)
    }

    with :ok <- Core.release_only_artifact(guarded, name, bytes), do: guard.()
  end

  defp publication_guard(context, enrollment, external, name, bytes) do
    with {:ok, snapshot} <- Core.release_only_snapshot(context),
         {:ok, binding} <- Protocol.binding(snapshot, enrollment.source_heads),
         {:ok, auth_bytes} <- attempt_bytes(context, name, bytes),
         {:ok, auth} <- Jason.decode(auth_bytes),
         {:ok, decision} <- external.approval.(binding, auth["decisionId"]),
         {:ok, ^snapshot} <- Core.release_only_snapshot(context),
         now = context.host_ops.now_ms.(),
         {:ok, ^auth} <- Protocol.authorize(decision, binding, enrollment.owner_principal, now),
         :ok <- publication_fresh(context, name, bytes, now) do
      :ok
    else
      _ -> {:error, :hgs740_release_publication_held_closed}
    end
  end

  defp attempt_bytes(_context, "release-only-attempt.json", bytes), do: {:ok, bytes}
  defp attempt_bytes(context, _name, _bytes), do: Core.release_only_artifact(context, "release-only-attempt.json", nil)
  defp publication_fresh(_context, "release-only-attempt.json", _bytes, _now), do: :ok

  defp publication_fresh(context, name, bytes, now) do
    value = if name == "release-only-attestation.json", do: {:ok, bytes}, else: Core.release_only_artifact(context, "release-only-attestation.json", nil)

    with {:ok, raw} <- value,
         {:ok, envelope} <- Jason.decode(raw),
         {:ok, payload} <- Base.url_decode64(envelope["payload"], padding: false),
         {:ok, attestation} <- Jason.decode(payload),
         {:ok, observed, 0} <- DateTime.from_iso8601(attestation["observedAt"]),
         {:ok, expires, 0} <- DateTime.from_iso8601(attestation["expiresAt"]),
         observed_ms = DateTime.to_unix(observed, :millisecond),
         expires_ms = DateTime.to_unix(expires, :millisecond),
         true <- observed_ms - 5_000 <= now and now < expires_ms and (expires_ms - observed_ms) in 1..60_000 do
      :ok
    else
      _ -> {:error, :hgs740_release_attestation_expired}
    end
  end

  defp collect(context, enrollment, external, binding) do
    host = context.host_ops

    with :ok <- host.require_paused_gate.(),
         :ok <- host.require_services_quiescent.(),
         :ok <- host.require_mutation_quiescent.(context.runtime, 1001),
         {:ok, snapshot} <- Core.release_only_snapshot(context),
         {:ok, ^binding} <- Protocol.binding(snapshot, enrollment.source_heads),
         {:ok, original} <- Jason.decode(snapshot.observation_bytes),
         {:ok, held} <- external.held.(binding),
         true <- held_valid?(held, binding),
         claim = Map.merge(binding["expected"], %{"assignmentSHA256" => nil, "assignmentSnapshotState" => "absent"}),
         {:ok, kube} <- external.kubernetes.(claim, original["kubernetes"]),
         true <- empty_kubernetes?(kube),
         {:ok, ^snapshot} <- Core.release_only_snapshot(context),
         :ok <- host.require_paused_gate.(),
         :ok <- host.require_services_quiescent.(),
         :ok <- host.require_mutation_quiescent.(context.runtime, 1001) do
      now = host.now_ms.() |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()
      stamp = %{"observedAt" => now}

      {:ok,
       %{
         "host" => Map.merge(stamp, %{"sourceIdentity" => "native-host", "globalPause" => true, "unitsMaskedAndQuiescent" => true, "workerCount" => 0}),
         "custody" =>
           Map.merge(stamp, %{
             "sourceIdentity" => "native-custody",
             "verified" => true,
             "markerSHA256" => binding["markerSHA256"],
             "postimages" => binding["postimages"],
             "sourceHeads" => binding["sourceHeads"]
           }),
         "provider" => %{
           "observedAt" => held["observedAt"],
           "sourceIdentity" => held["sourceIdentity"],
           "expected" => held["expected"],
           "state" => "claimed_held",
           "complete" => true,
           "credentialLeases" => [],
           "oauthSlotLeases" => [],
           "readback" => held
         },
         "jobs" => inventory(kube, "jobs", "confirmingResourceVersion"),
         "pods" => inventory(kube, "pods", "resourceVersion")
       }}
    else
      _ -> {:error, :hgs740_release_collection_held_closed}
    end
  end

  defp held_valid?(h, b) do
    h["sourceIdentity"] == "provider-core:postgres" and h["expected"] == b["expected"] and
      h["assignmentDigest"] == Evidence.tuple_digest(b["expected"]) and
      h["projectionState"] == "active" and h["mutationState"] == "applied" and h["reservationState"] == "claimed" and
      h["executionCapacityState"] == "held" and h["scopeState"] == "held" and no_lease_history?(h)
  end

  defp no_lease_history?(h) do
    h["credentialLeaseInventory"]["complete"] == true and h["credentialLeaseInventory"]["leaseIds"] == [] and
      h["credentialLeaseInventory"]["readbacks"] == [] and h["oauthSlotLeaseInventory"]["complete"] == true and
      h["oauthSlotLeaseInventory"]["leaseCount"] === 0 and h["oauthSlotLeaseInventory"]["leaseIds"] == [] and
      h["oauthSlotLeaseInventory"]["leases"] == []
  end

  defp empty_kubernetes?(k) do
    k["jobs"]["itemCount"] === 0 and k["jobs"]["confirmingItemCount"] === 0 and
      k["pods"]["itemCount"] === 0 and k["jobs"]["claimAbsent"] == true and k["pods"]["claimAbsent"] == true
  end

  defp inventory(kube, name, version),
    do: %{"observedAt" => kube["observedAt"], "sourceIdentity" => "kubernetes:" <> name, "complete" => true, "items" => [], "resourceVersion" => kube[name][version], "readback" => kube[name]}
end
