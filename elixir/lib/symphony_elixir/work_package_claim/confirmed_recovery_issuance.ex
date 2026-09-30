defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuance do
  @moduledoc """
  Root-only HGS-740 issuance coordinator. It rechecks the paused/quiescent host, refreshes
  the complete Kubernetes absence observation, signs the exact validated contract, and
  durably creates the two immutable artifacts consumed by the recovery transaction.
  """

  alias SymphonyElixir.WorkPackageClaim.{
    ConfirmedRecoveryEvidence,
    ConfirmedRecoveryIssuer,
    ConfirmedRecoveryKubernetes,
    ConfirmedRecoveryRootHost
  }

  @spec issue(String.t(), String.t(), String.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def issue(issue_id, pool, workflow_path, nonce, bundle_path)
      when is_binary(issue_id) and is_binary(pool) and is_binary(workflow_path) and is_binary(nonce) and
             is_binary(bundle_path) do
    with {:ok, context} <- ConfirmedRecoveryRootHost.authorize_apply(issue_id, pool, workflow_path, nonce),
         :ok <- ConfirmedRecoveryRootHost.with_pool_lock(context, fn -> issue_locked(context, bundle_path) end) do
      :ok
    else
      {:error, _reason} = error -> error
      _ -> {:error, :hgs740_issuance_held_closed}
    end
  rescue
    _ -> {:error, :hgs740_issuance_held_closed}
  catch
    _, _ -> {:error, :hgs740_issuance_held_closed}
  end

  def issue(_issue_id, _pool, _workflow_path, _nonce, _bundle_path),
    do: {:error, :invalid_hgs740_issuance_request}

  defp issue_locked(context, bundle_path) do
    runtime = context.runtime
    host = context.host_ops

    with :ok <- host.require_mutation_quiescent.(runtime, state_owner_uid(runtime, host)),
         {:ok, bundle_bytes} <- host.read_issuer_bundle.(context.issue_id, bundle_path),
         do: issue_bundle_bytes(context, bundle_bytes, &fresh_kubernetes/2)
  end

  defp issue_bundle_bytes(context, bundle_bytes, observe_kubernetes) do
    runtime = context.runtime
    host = context.host_ops

    with {:ok, bundle} when is_map(bundle) <- Jason.decode(bundle_bytes),
         true <- ConfirmedRecoveryEvidence.canonical_json(bundle) == bundle_bytes,
         :ok <- verify_preimage_hashes(runtime, bundle["observation"], host),
         {:ok, observation} when is_map(observation) <- observe_kubernetes.(bundle["observation"], bundle["assignmentSHA256"]),
         bundle <- Map.put(bundle, "observation", observation),
         now_ms <- host.now_ms.(),
         bindings <- ConfirmedRecoveryIssuer.bindings(bundle, context.pool, context.issue_id, context.nonce, now_ms),
         {:ok, payload, _payload_bytes, envelope_bytes} <-
           ConfirmedRecoveryIssuer.issue(
             ConfirmedRecoveryEvidence.canonical_json(bundle),
             context.pool,
             context.issue_id,
             context.nonce,
             bindings,
             host.sign_recovery_payload,
             host.verify_signed_evidence
           ),
         candidate_bytes <- ConfirmedRecoveryEvidence.canonical_json(payload["observation"]),
         :ok <- host.persist_issuer_outputs.(context.issue_id, candidate_bytes, envelope_bytes) do
      :ok
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  if Mix.env() == :test do
    @doc false
    @spec issue_bundle_with_test_context(
            SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryContext.t(),
            binary(),
            (map(), binary() -> {:ok, map()} | {:error, term()})
          ) :: :ok | {:error, term()}
    def issue_bundle_with_test_context(context, bundle_bytes, observe_kubernetes)
        when is_function(observe_kubernetes, 2),
        do: issue_bundle_bytes(context, bundle_bytes, observe_kubernetes)

    @doc false
    @spec fresh_kubernetes_with_test_observer(map(), binary(), (map(), map() -> term())) ::
            {:ok, map()} | {:error, :kubernetes_observation_unavailable}
    def fresh_kubernetes_with_test_observer(observation, assignment_sha, observe)
        when is_function(observe, 2),
        do: fresh_kubernetes(observation, assignment_sha, observe)
  end

  defp state_owner_uid(runtime, host) do
    case host.lstat.(runtime.journal_path) do
      {:ok, %File.Stat{type: :regular, uid: uid, links: 1}} when uid > 0 -> uid
      _ -> -1
    end
  end

  defp verify_preimage_hashes(runtime, observation, host) when is_map(observation) do
    paths = [
      {runtime.journal_path, observation["claimJournalSHA256"]},
      {runtime.execution_fence_path, observation["fenceSHA256"]},
      {runtime.responsibility_graph_path, observation["responsibilityGraphSHA256"]}
    ]

    results =
      Enum.map(paths, fn {path, expected_digest} ->
        with {:ok, %File.Stat{type: :regular, uid: uid, gid: gid, mode: mode, links: 1}} <- host.lstat.(path),
             true <- uid > 0 and gid >= 0 and Bitwise.band(mode, 0o077) == 0,
             {:ok, bytes} <- host.read.(path),
             true <- digest(bytes) == expected_digest do
          :ok
        else
          _ -> {:error, :local_preimage_changed}
        end
      end)

    if Enum.all?(results, &(&1 == :ok)), do: :ok, else: {:error, :local_preimage_changed}
  end

  defp verify_preimage_hashes(_runtime, _observation, _host), do: {:error, :local_preimage_changed}

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp fresh_kubernetes(observation, assignment_sha) when is_map(observation) and is_binary(assignment_sha) do
    fresh_kubernetes(observation, assignment_sha, &ConfirmedRecoveryKubernetes.observe/2)
  end

  defp fresh_kubernetes(_observation, _assignment_sha), do: {:error, :kubernetes_observation_unavailable}

  defp fresh_kubernetes(observation, assignment_sha, observe)
       when is_map(observation) and is_binary(assignment_sha) and is_function(observe, 2) do
    kube = observation["kubernetes"]
    claim = observation["expected"]

    with %{} = kube <- kube,
         %{} = claim <- claim,
         %{} = cluster <- kube["cluster"],
         {:ok, snapshot} <- observe.(Map.put(claim, "assignmentSHA256", assignment_sha), cluster),
         fresh_kube <- kubernetes_contract(kube, claim, snapshot, assignment_sha),
         observation <- Map.put(observation, "kubernetes", fresh_kube) do
      {:ok, observation}
    else
      _ -> {:error, :kubernetes_observation_unavailable}
    end
  rescue
    _ -> {:error, :kubernetes_observation_unavailable}
  end

  defp fresh_kubernetes(_observation, _assignment_sha, _observe),
    do: {:error, :kubernetes_observation_unavailable}

  defp kubernetes_contract(original, claim, snapshot, assignment_sha) do
    %{
      "observedAt" => snapshot["observedAt"],
      "cluster" => original["cluster"],
      "namespace" => snapshot["namespace"],
      "claim" => %{
        "issueId" => claim["issueId"],
        "generation" => claim["generation"],
        "repositoryRef" => claim["repositoryRef"],
        "reservationId" => claim["reservationId"],
        "assignmentSHA256" => assignment_sha
      },
      "jobs" => %{
        "resourceVersion" => snapshot["jobs"]["confirmingResourceVersion"],
        "sha256" => snapshot["jobs"]["sha256"],
        "complete" => true,
        "itemCount" => snapshot["jobs"]["itemCount"],
        "claimAbsent" => snapshot["jobs"]["claimAbsent"]
      },
      "pods" => %{
        "resourceVersion" => snapshot["pods"]["resourceVersion"],
        "sha256" => snapshot["pods"]["sha256"],
        "complete" => true,
        "itemCount" => snapshot["pods"]["itemCount"],
        "claimAbsent" => snapshot["pods"]["claimAbsent"]
      }
    }
  end
end
