defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuance do
  @moduledoc """
  Root-only HGS-740 issuance coordinator. It rechecks the paused/quiescent host, refreshes
  the complete Kubernetes absence observation, signs the exact validated contract, and
  durably creates the two immutable artifacts consumed by the recovery transaction.
  """

  alias SymphonyElixir.ManagedAssignmentBundle

  alias SymphonyElixir.WorkPackageClaim.{
    ConfirmedRecoveryEvidence,
    ConfirmedRecoveryIssuer,
    ConfirmedRecoveryKubernetes,
    ConfirmedRecoveryRootHost,
    Journal
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
         :ok <- verify_absent_snapshot_precondition(runtime, bundle, host),
         {:ok, observation} when is_map(observation) <- observe_kubernetes.(bundle["observation"], bundle["assignmentSHA256"]),
         bundle <- Map.put(bundle, "observation", observation),
         :ok <- verify_absent_snapshot_precondition(runtime, bundle, host),
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
            (map(), binary() | nil -> {:ok, map()} | {:error, term()})
          ) :: :ok | {:error, term()}
    def issue_bundle_with_test_context(context, bundle_bytes, observe_kubernetes)
        when is_function(observe_kubernetes, 2),
        do: issue_bundle_bytes(context, bundle_bytes, observe_kubernetes)

    @doc false
    @spec fresh_kubernetes_with_test_observer(map(), binary() | nil, (map(), map() -> term())) ::
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

  defp verify_absent_snapshot_precondition(runtime, %{"assignmentSnapshotState" => "absent"} = bundle, host) do
    observation = bundle["observation"]
    expected = if is_map(observation), do: observation["expected"], else: nil

    with true <- is_nil(bundle["assignmentSHA256"]),
         true <- is_map(observation) and is_map(expected),
         true <- observation["dispatchPhase"] == "confirmed" and observation["localGenerationMax"] == 2,
         true <- is_binary(expected["issueId"]),
         key <- Journal.reservation_key(expected["issueId"], expected["managedProjectProfileId"], expected["repositoryRef"], 2),
         {:ok, journal_bytes} <- host.read.(runtime.journal_path),
         true <- digest(journal_bytes) == observation["claimJournalSHA256"],
         :absent <- Journal.assignment_snapshot_state(journal_bytes, key),
         {:ok, journal} <- Journal.decode_bytes(journal_bytes),
         reservation when is_map(reservation) <- journal.reservations[key],
         true <- reservation_matches_expected?(reservation, expected),
         %{dispatch: %{phase: "confirmed", allocation_id: nil}} <- reservation,
         true <- map_size(Map.get(reservation, :failed_worker_turns, %{})) == 0,
         true <- map_size(Map.get(reservation, :cleanup_receipts, %{})) == 0 do
      :ok
    else
      _ -> {:error, :local_preimage_changed}
    end
  rescue
    _ -> {:error, :local_preimage_changed}
  end

  defp verify_absent_snapshot_precondition(runtime, %{"assignmentSHA256" => assignment_sha} = bundle, host)
       when is_binary(assignment_sha) do
    observation = bundle["observation"]
    expected = if is_map(observation), do: observation["expected"], else: nil

    with true <- is_map(observation) and is_map(expected),
         true <- is_binary(expected["issueId"]),
         key <- Journal.reservation_key(expected["issueId"], expected["managedProjectProfileId"], expected["repositoryRef"], 2),
         {:ok, journal_bytes} <- host.read.(runtime.journal_path),
         true <- digest(journal_bytes) == observation["claimJournalSHA256"],
         :present <- Journal.assignment_snapshot_state(journal_bytes, key),
         {:ok, journal} <- Journal.decode_bytes(journal_bytes),
         reservation when is_map(reservation) <- journal.reservations[key],
         true <- reservation_matches_expected?(reservation, expected),
         snapshot when is_binary(snapshot) <- Map.get(reservation, :assignment_snapshot),
         {:ok, assignment} <- ManagedAssignmentBundle.from_snapshot(snapshot),
         true <- assignment_matches_reservation?(assignment, reservation),
         true <- assignment.sha256 == assignment_sha do
      :ok
    else
      _ -> {:error, :local_preimage_changed}
    end
  rescue
    _ -> {:error, :local_preimage_changed}
  end

  defp verify_absent_snapshot_precondition(_runtime, _bundle, _host),
    do: {:error, :local_preimage_changed}

  defp reservation_matches_expected?(reservation, expected) do
    expected_claim = %{
      "projectionId" => reservation.projection_id,
      "reservationId" => reservation.reservation_id,
      "workspaceId" => Map.get(reservation, :workspace_id),
      "companyId" => Map.get(reservation, :company_id),
      "issueId" => reservation.issue_id,
      "runnerId" => reservation.runner_id,
      "managedProjectProfileId" => reservation.managed_project_profile_id,
      "repositoryRef" => reservation.repository_ref,
      "scopeKeys" => Enum.sort(reservation.scope_keys),
      "generation" => reservation.generation,
      "sessionId" => reservation.session_id,
      "processId" => reservation.process_id,
      "responsibleDelegationId" => reservation.responsible_delegation_id,
      "executionFenceToken" => reservation.execution_fence_token,
      "runtimeLeaseId" => reservation.runtime_lease_id,
      "nonceHash" => digest(reservation.reservation_nonce)
    }

    expected_claim == expected
  rescue
    _ -> false
  end

  defp assignment_matches_reservation?(assignment, reservation) do
    lease = assignment.lease

    lease.issue_id == reservation.issue_id and
      lease.repository == reservation.repository_ref and
      lease.generation == reservation.generation and
      lease.session_id == reservation.session_id and
      lease.process_id == reservation.process_id
  rescue
    _ -> false
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp fresh_kubernetes(observation, assignment_sha) when is_map(observation) and is_binary(assignment_sha) do
    fresh_kubernetes(observation, assignment_sha, &ConfirmedRecoveryKubernetes.observe/2)
  end

  defp fresh_kubernetes(observation, nil) when is_map(observation) do
    fresh_kubernetes(observation, nil, &ConfirmedRecoveryKubernetes.observe_without_assignment_snapshot/2)
  end

  defp fresh_kubernetes(_observation, _assignment_sha), do: {:error, :kubernetes_observation_unavailable}

  defp fresh_kubernetes(observation, assignment_sha, observe)
       when is_map(observation) and is_binary(assignment_sha) and is_function(observe, 2) do
    fresh_kubernetes_with_observer(observation, assignment_sha, observe, :present)
  end

  defp fresh_kubernetes(observation, nil, observe) when is_map(observation) and is_function(observe, 2) do
    fresh_kubernetes_with_observer(observation, nil, observe, :absent)
  end

  defp fresh_kubernetes(_observation, _assignment_sha, _observe),
    do: {:error, :kubernetes_observation_unavailable}

  defp fresh_kubernetes_with_observer(observation, assignment_sha, observe, snapshot_state) do
    kube = observation["kubernetes"]
    claim = observation["expected"]

    with %{} = kube <- kube,
         %{} = claim <- claim,
         %{} = cluster <- kube["cluster"],
         kube_claim <- Map.put(claim, "assignmentSHA256", assignment_sha),
         kube_claim <- if(snapshot_state == :absent, do: Map.put(kube_claim, "assignmentSnapshotState", "absent"), else: kube_claim),
         {:ok, snapshot} <- observe.(kube_claim, cluster),
         fresh_kube <- kubernetes_contract(kube, claim, snapshot, assignment_sha),
         observation <- Map.put(observation, "kubernetes", fresh_kube) do
      {:ok, observation}
    else
      _ -> {:error, :kubernetes_observation_unavailable}
    end
  rescue
    _ -> {:error, :kubernetes_observation_unavailable}
  end

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
