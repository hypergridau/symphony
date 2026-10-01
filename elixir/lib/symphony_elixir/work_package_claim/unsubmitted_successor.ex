defmodule SymphonyElixir.WorkPackageClaim.UnsubmittedSuccessor do
  @moduledoc """
  Prepares the narrowly scoped retirement of a signed, never-submitted authority
  pair before its distinct signed successor can be admitted.

  This transition consumes a root-owned, signature-verified manifest entry with
  a fresh root-attested provider, Kubernetes, process, and workspace observation.
  It also checks the local journal, execution fence, graph, and workspace directly.
  The caller must persist the returned graph first and the fence second; replay
  recognizes that single permitted intermediate from the exact retained receipt.
  """

  alias SymphonyElixir.{ExecutionFence, ExecutionSupervisor, ResponsibilityGraph}
  alias SymphonyElixir.WorkPackageClaim.{Journal, Unsubmitted}

  @grant_fields ~w(id parent_delegation_id role actor_id scope authority budget expires_at_ms expected_deliverable expected_evidence return_to_parent)a
  @observation_times ~w(provider_observed_at_ms kubernetes_observed_at_ms process_observed_at_ms workspace_observed_at_ms)
  @observation_refs ~w(provider_evidence_ref kubernetes_jobs_evidence_ref kubernetes_pods_evidence_ref process_evidence_ref workspace_evidence_ref)
  @max_observation_age_ms 60_000
  @clock_skew_ms 5_000

  @doc "Builds fail-closed graph and fence candidates for one signed successor entry."
  @spec prepare(map(), map(), map(), map(), non_neg_integer()) ::
          {:ok, map(), map(), :retired | :already_retired} | {:error, term()}
  def prepare(runtime, fence, graph, entry, now_ms)
      when is_map(runtime) and is_map(fence) and is_map(graph) and is_map(entry) and
             is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_snapshots(fence, graph),
         {:ok, context} <- candidate_context(runtime, fence, graph, entry, now_ms),
         {:ok, next_graph, graph_result} <- retire_graph_candidate(context, graph, entry, now_ms),
         {:ok, next_fence, fence_result} <- retire_fence_candidate(context, fence, entry, now_ms),
         :ok <- consistent_retirement_results(graph_result, fence_result) do
      {:ok, next_fence, next_graph, retirement_result(graph_result, fence_result)}
    else
      {:error, _reason} = error -> error
    end
  rescue
    _error -> {:error, :unsubmitted_successor_not_proven}
  catch
    _kind, _reason -> {:error, :unsubmitted_successor_not_proven}
  end

  def prepare(_runtime, _fence, _graph, _entry, _now_ms), do: {:error, :unsubmitted_successor_not_proven}

  defp validate_snapshots(fence, graph) do
    case ExecutionFence.validate(fence) do
      :ok -> ResponsibilityGraph.validate(graph)
      error -> error
    end
  end

  defp candidate_context(runtime, fence, graph, entry, now_ms) do
    with {:ok, context} <- predecessor_context(runtime, fence, entry),
         :ok <- no_submission_evidence(runtime, entry, context.execution),
         :ok <- exact_successor_identity(runtime, context.manifest, entry, context.prior, context.execution, graph),
         {:ok, receipt, replay?} <-
           graph_receipt(graph, entry, context.prior, context.observation, context.manifest, now_ms),
         :ok <-
           validate_observation(
             context.observation,
             entry,
             context.prior,
             context.worker,
             context.execution,
             receipt["prepared_at_ms"]
           ) do
      {:ok, Map.merge(context, %{receipt: receipt, replay?: replay?})}
    end
  end

  defp predecessor_context(runtime, fence, entry) do
    with {:ok, manifest, prior, observation} <- signed_successor_from_entry(runtime, entry),
         {:ok, execution} <- current_execution(fence, entry.issue_id, prior.generation),
         {:ok, worker} <- untouched_worker(execution) do
      {:ok, %{manifest: manifest, prior: prior, observation: observation, execution: execution, worker: worker}}
    end
  end

  defp no_submission_evidence(runtime, entry, execution) do
    case journal_proves_absence(runtime, entry.issue_id) do
      :ok -> workspace_proves_absence(execution)
      error -> error
    end
  end

  defp retire_graph_candidate(context, graph, entry, now_ms) do
    retire_graph(
      graph,
      entry,
      context.prior,
      context.worker,
      context.receipt,
      now_ms,
      context.replay?
    )
  end

  defp retire_fence_candidate(context, fence, entry, now_ms) do
    retire_fence(fence, context.execution, context.worker, entry, context.receipt, now_ms)
  end

  defp consistent_retirement_results(graph_result, fence_result) do
    if graph_result == fence_result or graph_result == :retired or fence_result == :retired,
      do: :ok,
      else: {:error, :unsubmitted_successor_not_proven}
  end

  defp retirement_result(:already_retired, :already_retired), do: :already_retired
  defp retirement_result(_graph_result, _fence_result), do: :retired

  defp signed_successor_from_entry(
         %{
           managed_project_profile_id: profile,
           managed_delegations:
             %{
               schema_version: 2,
               source_sha256: manifest_sha,
               signer_key_sha256: signer
             } = manifest
         },
         %{issue_id: issue_id, identifier: identifier, responsible: %{scope: %{repository: repository}}} = entry
       )
       when is_binary(profile) and is_binary(manifest_sha) and is_binary(signer) do
    with true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, manifest_sha),
         true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, signer),
         true <- manifest.repository_ref == repository,
         true <- manifest.managed_project_profile_id == profile,
         selected when selected == entry <-
           Enum.find(manifest.entries, &(&1.issue_id == issue_id and &1.identifier == identifier)),
         prior when is_map(prior) <- Map.get(selected, :prior_unsubmitted_authority),
         observation when is_map(observation) <- Map.get(selected, :unsubmitted_observation) do
      {:ok, manifest, prior, observation}
    else
      _ -> {:error, :signed_unsubmitted_successor_missing}
    end
  end

  defp signed_successor_from_entry(_fence, _entry), do: {:error, :signed_unsubmitted_successor_missing}

  defp current_execution(fence, issue_id, generation) do
    case Map.get(fence.executions, issue_id) do
      %{issue_id: ^issue_id, generation: ^generation} = execution -> {:ok, execution}
      _ -> {:error, :prior_execution_missing_or_changed}
    end
  end

  defp untouched_worker(%{leases: leases, ownership: ownership, termination_unconfirmed: false} = execution)
       when is_map(leases) and ownership in [:reconciled, :unknown] do
    case Map.values(leases) do
      [%{role: :worker} = worker] ->
        if untouched_worker_identity?(worker, execution),
          do: {:ok, worker},
          else: {:error, :prior_worker_was_observed_or_spawned}

      _ ->
        {:error, :prior_worker_identity_conflict}
    end
  end

  defp untouched_worker(_execution), do: {:error, :prior_execution_not_unsubmitted}

  defp untouched_worker_identity?(worker, execution) do
    expected_identity? =
      worker.issue_id == execution.issue_id and worker.repository == execution.repository and
        worker.generation == execution.generation and is_binary(worker.session_id) and is_binary(worker.process_id)

    never_observed? =
      worker.head == "unobserved" and worker.last_heartbeat_at == 0 and
        is_nil(Map.get(worker, :supervisor_identity)) and not Map.get(worker, :termination_required, false)

    expected_identity? and never_observed?
  end

  defp journal_proves_absence(%{journal_path: path}, issue_id) when is_binary(path) do
    path |> Journal.load() |> journal_proves_issue_absent(issue_id)
  end

  defp journal_proves_absence(_runtime, _issue_id), do: {:error, :claim_journal_unavailable}

  defp journal_proves_issue_absent({:ok, journal}, issue_id) do
    if Enum.any?(journal.reservations, &reservation_for_issue?(&1, issue_id)),
      do: {:error, :prior_claim_journal_row_exists},
      else: :ok
  end

  defp journal_proves_issue_absent(_load_result, _issue_id), do: {:error, :claim_journal_unavailable}

  defp reservation_for_issue?({_key, reservation}, issue_id), do: reservation.issue_id == issue_id

  defp workspace_proves_absence(%{worker_host: nil, worktree: path}) when is_binary(path) do
    if Path.type(path) == :absolute and Path.expand(path) == path and not String.starts_with?(path, ["//", "\\\\"]) and
         File.lstat(path) == {:error, :enoent} and plain_directory_ancestors?(Path.dirname(path)) do
      :ok
    else
      {:error, :workspace_not_proven_absent}
    end
  end

  defp workspace_proves_absence(_execution), do: {:error, :workspace_not_proven_absent}

  defp exact_successor_identity(runtime, manifest, entry, prior, execution, graph) do
    old_accountable = Map.get(graph.delegations, prior.accountable_id)
    old_responsible = Map.get(graph.delegations, prior.responsible_id)
    new_ids = [entry.accountable.id, entry.responsible.id]

    with true <- prior.issue_id == entry.issue_id and prior.generation == execution.generation,
         true <- prior.repository_ref == execution.repository and prior.repository_ref == manifest.repository_ref,
         true <- prior.managed_project_profile_id == runtime.managed_project_profile_id,
         true <- entry.responsible.scope.issue_id == entry.issue_id and entry.responsible.scope.repository == execution.repository,
         true <- entry.responsible.scope.work_package_id == get_in(entry, [:unsubmitted_observation, "provider_projection_id"]),
         true <- old_authorities_match?(old_accountable, old_responsible, prior, entry, execution),
         true <- successor_graph_state?(graph, entry, prior, new_ids, execution),
         true <- old_scope_work_package?(old_accountable, old_responsible, entry.identifier),
         true <- normalized_grant(old_accountable, :accountable) == normalized_grant(entry.accountable, :accountable),
         true <- normalized_grant(old_responsible, :responsible) == normalized_grant(entry.responsible, :responsible) do
      :ok
    else
      _ -> {:error, :successor_authority_or_repository_mismatch}
    end
  end

  defp old_authorities_match?(accountable, responsible, prior, entry, execution)
       when is_map(accountable) and is_map(responsible) do
    retained_pair_matches?(accountable, responsible, prior) and
      retained_responsibility_matches?(accountable, responsible, prior, execution) and
      successor_pair_is_distinct?(accountable, responsible, entry)
  end

  defp old_authorities_match?(_accountable, _responsible, _prior, _entry, _execution), do: false

  defp retained_pair_matches?(accountable, responsible, prior) do
    grant_digest(accountable) == prior.accountable_digest and
      grant_digest(responsible) == prior.responsible_digest and prior_pair_state?(accountable, responsible)
  end

  defp retained_responsibility_matches?(accountable, responsible, prior, execution) do
    accountable.role == :accountable and is_nil(accountable.parent_delegation_id) and
      responsible.role == :responsible and responsible.parent_delegation_id == accountable.id and
      accountable.id == prior.accountable_id and responsible.id == prior.responsible_id and
      responsible.scope.issue_id == execution.issue_id and responsible.scope.repository == execution.repository
  end

  defp successor_pair_is_distinct?(accountable, responsible, entry) do
    entry.accountable.id != accountable.id and entry.responsible.id != responsible.id and
      entry.responsible.parent_delegation_id == entry.accountable.id
  end

  defp successor_graph_state?(graph, entry, prior, [accountable_id, responsible_id], execution) do
    case {Map.get(graph.delegations, accountable_id), Map.get(graph.delegations, responsible_id)} do
      {nil, nil} ->
        only_prior_pair_for_issue?(graph, entry.issue_id, prior.accountable_id, prior.responsible_id)

      {%{} = accountable, %{} = responsible} when execution.status == :retired and execution.cleanup == :cleaned ->
        bound_successor_pair?(graph, entry, prior, accountable, responsible)

      _ ->
        false
    end
  end

  defp bound_successor_pair?(graph, entry, prior, accountable, responsible) do
    next_generation = prior.generation + 1
    expected_session = "worker:#{entry.issue_id}:#{next_generation}"

    expected_lease = %{
      issue_id: entry.issue_id,
      repository: prior.repository_ref,
      generation: next_generation,
      session_id: expected_session,
      process_id: expected_session
    }

    issue_delegation_ids =
      graph.delegations
      |> Enum.filter(fn {_id, delegation} ->
        delegation.scope.issue_id == entry.issue_id and delegation.role in [:accountable, :responsible]
      end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    issue_delegation_ids == Enum.sort([prior.accountable_id, prior.responsible_id, entry.accountable.id, entry.responsible.id]) and
      grant_digest(accountable) == grant_digest(entry.accountable) and
      grant_digest(responsible) == grant_digest(entry.responsible) and
      successor_pair_runtime_state?(accountable, responsible, expected_lease) and
      accountable.role == :accountable and is_nil(accountable.parent_delegation_id) and
      responsible.role == :responsible and responsible.parent_delegation_id == accountable.id
  end

  defp successor_pair_runtime_state?(
         %{status: :active, runtime_lease: nil},
         %{status: :active, runtime_lease: lease},
         lease
       ),
       do: true

  defp successor_pair_runtime_state?(
         %{status: :blocked, blocked_on: :restart_reconciliation, runtime_lease: nil},
         %{status: :blocked, blocked_on: :restart_reconciliation, runtime_lease: lease},
         lease
       ),
       do: true

  defp successor_pair_runtime_state?(_accountable, _responsible, _expected_lease), do: false

  defp prior_pair_state?(%{status: :active}, %{status: :active}), do: true

  defp prior_pair_state?(
         %{status: :blocked, blocked_on: :restart_reconciliation},
         %{status: :blocked, blocked_on: :restart_reconciliation}
       ),
       do: true

  defp prior_pair_state?(
         %{status: :revoked, terminal_reason: reason, terminal_evidence: receipt},
         %{status: :revoked, terminal_reason: reason, terminal_evidence: receipt}
       )
       when reason in [:unsubmitted_successor, "unsubmitted_successor"] and is_map(receipt),
       do: true

  defp prior_pair_state?(_accountable, _responsible), do: false

  defp old_scope_work_package?(
         %{scope: %{work_package_id: "linear:" <> identifier}},
         %{scope: %{work_package_id: "linear:" <> identifier}},
         identifier
       ),
       do: true

  defp old_scope_work_package?(_accountable, _responsible, _identifier), do: false

  defp normalized_grant(grant, role) do
    grant
    |> Map.take(@grant_fields)
    |> Map.delete(:id)
    |> Map.delete(:expires_at_ms)
    |> maybe_drop_parent(role)
    |> Map.update!(:scope, &Map.delete(&1, :work_package_id))
  end

  defp maybe_drop_parent(grant, :responsible), do: Map.delete(grant, :parent_delegation_id)
  defp maybe_drop_parent(grant, _role), do: grant

  defp only_prior_pair_for_issue?(graph, issue_id, accountable_id, responsible_id) do
    graph.delegations
    |> Enum.filter(fn {_id, delegation} ->
      delegation.scope.issue_id == issue_id and delegation.role in [:accountable, :responsible]
    end)
    |> Enum.map(&elem(&1, 0))
    |> Enum.sort() == Enum.sort([accountable_id, responsible_id])
  end

  defp graph_receipt(graph, entry, prior, observation, manifest, now_ms) do
    account = Map.get(graph.delegations, prior.accountable_id)
    responsible = Map.get(graph.delegations, prior.responsible_id)

    case {account, responsible} do
      {%{status: status_a} = accountable, %{status: status_r} = responsible}
      when status_a in [:active, :blocked] and status_r in [:active, :blocked] ->
        new_graph_receipt(accountable, responsible, entry, prior, observation, manifest, now_ms)

      {
        %{status: :revoked, terminal_reason: reason_a, terminal_evidence: receipt},
        %{status: :revoked, terminal_reason: reason_r, terminal_evidence: receipt}
      }
      when reason_a in [:unsubmitted_successor, "unsubmitted_successor"] and
             reason_r in [:unsubmitted_successor, "unsubmitted_successor"] ->
        prepared_at = Map.get(receipt, "prepared_at_ms")

        with true <- is_integer(prepared_at) and prepared_at >= 0,
             :ok <- validate_observation_static(observation, entry, prior),
             :ok <- fresh_observation(observation, prepared_at),
             expected <- build_receipt(entry, prior, observation, manifest, prepared_at),
             true <- receipt == expected do
          {:ok, receipt, true}
        else
          _ -> {:error, :persisted_successor_receipt_conflict}
        end

      _ ->
        {:error, :prior_authority_pair_state_conflict}
    end
  end

  defp new_graph_receipt(accountable, responsible, entry, prior, observation, manifest, now_ms) do
    with true <- prior_pair_state?(accountable, responsible),
         :ok <- validate_observation_static(observation, entry, prior),
         :ok <- fresh_observation(observation, now_ms),
         receipt <- build_receipt(entry, prior, observation, manifest, now_ms) do
      {:ok, receipt, false}
    else
      _ -> {:error, :prior_authority_pair_state_conflict}
    end
  end

  defp validate_observation(observation, entry, prior, worker, execution, prepared_at) do
    expected_unit = ExecutionSupervisor.unit_name(entry.issue_id, execution.generation, worker.session_id)

    with :ok <- validate_observation_static(observation, entry, prior),
         :ok <- fresh_observation(observation, prepared_at),
         true <- observation["process_unit"] == expected_unit do
      :ok
    else
      _ -> {:error, :unsubmitted_observation_identity_or_freshness_invalid}
    end
  end

  defp validate_observation_static(observation, entry, prior) do
    predicates = [
      observation_identity?(observation, entry, prior),
      valid_sha256?(observation["workflow_sha256"]),
      provider_is_unclaimed?(observation),
      kubernetes_is_empty?(observation),
      process_is_absent?(observation),
      observation_workspace_is_absent?(observation),
      observation_evidence_is_complete?(observation)
    ]

    if Enum.all?(predicates, & &1) do
      :ok
    else
      {:error, :unsubmitted_observation_incomplete_or_contradictory}
    end
  end

  defp observation_identity?(observation, entry, prior) do
    observation["issue_id"] == entry.issue_id and observation["generation"] == prior.generation and
      observation["repository_ref"] == prior.repository_ref and
      observation["managed_project_profile_id"] == prior.managed_project_profile_id and
      observation["provider_projection_id"] == entry.responsible.scope.work_package_id
  end

  defp provider_is_unclaimed?(observation) do
    observation["provider_reservation_state"] == "reserved" and is_nil(observation["provider_claimed_at"]) and
      is_nil(observation["provider_claim_generation"]) and is_nil(observation["provider_execution_fence_token"])
  end

  defp kubernetes_is_empty?(observation) do
    observation["kubernetes_namespace"] == "frigga" and observation["kubernetes_job_issue_matches"] == 0 and
      observation["kubernetes_pod_issue_matches"] == 0 and
      valid_resource_version?(observation["kubernetes_jobs_resource_version"]) and
      valid_resource_version?(observation["kubernetes_pods_resource_version"])
  end

  defp process_is_absent?(observation) do
    observation["process_load_state"] == "not-found" and
      observation["process_active_state"] in ["inactive", "unknown"] and
      is_nil(observation["process_control_group"]) and observation["process_main_pid"] in [nil, 0] and
      observation["process_count"] == 0 and is_binary(observation["process_unit"])
  end

  defp observation_workspace_is_absent?(observation), do: observation["workspace_absent"] == true

  defp observation_evidence_is_complete?(observation) do
    valid_refs? = Enum.all?(@observation_refs, &valid_sha256?(observation[&1]))
    valid_times? = Enum.all?(@observation_times, &valid_timestamp?(observation[&1]))
    valid_refs? and valid_times?
  end

  defp valid_timestamp?(timestamp), do: is_integer(timestamp) and timestamp >= 0

  defp fresh_observation(observation, now_ms) do
    timestamps = Enum.map(@observation_times, &observation[&1])

    if Enum.all?(timestamps, &timestamp_fresh?(&1, now_ms)),
      do: :ok,
      else: {:error, :unsubmitted_observation_stale}
  end

  defp timestamp_fresh?(timestamp, now_ms) do
    timestamp <= now_ms + @clock_skew_ms and now_ms - timestamp <= @max_observation_age_ms
  end

  defp build_receipt(entry, prior, observation, manifest, prepared_at_ms) do
    observation_sha = digest(observation)
    accountable_sha = grant_digest(entry.accountable)
    responsible_sha = grant_digest(entry.responsible)

    evidence_ref =
      digest({
        manifest.source_sha256,
        manifest.signer_key_sha256,
        observation_sha,
        prior.accountable_digest,
        prior.responsible_digest,
        entry.accountable.id,
        entry.responsible.id,
        accountable_sha,
        responsible_sha
      })

    %{
      "type" => "unsubmitted_successor",
      "issue_id" => entry.issue_id,
      "generation" => prior.generation,
      "repository_ref" => prior.repository_ref,
      "managed_project_profile_id" => prior.managed_project_profile_id,
      "prior_accountable_id" => prior.accountable_id,
      "prior_responsible_id" => prior.responsible_id,
      "prior_accountable_digest" => prior.accountable_digest,
      "prior_responsible_digest" => prior.responsible_digest,
      "successor_accountable_id" => entry.accountable.id,
      "successor_responsible_id" => entry.responsible.id,
      "successor_accountable_digest" => accountable_sha,
      "successor_responsible_digest" => responsible_sha,
      "manifest_sha256" => manifest.source_sha256,
      "signer_key_sha256" => manifest.signer_key_sha256,
      "observation_sha256" => observation_sha,
      "evidence_ref" => evidence_ref,
      "prepared_at_ms" => prepared_at_ms,
      "observation" => observation
    }
  end

  defp retire_graph(graph, _entry, prior, _worker, receipt, now_ms, true) do
    case ResponsibilityGraph.retire_unsubmitted_successor_pair(
           graph,
           prior.accountable_id,
           prior.responsible_id,
           receipt,
           now_ms
         ) do
      {:ok, graph, :already_retired} -> {:ok, graph, :already_retired}
      _ -> {:error, :persisted_successor_graph_conflict}
    end
  end

  defp retire_graph(graph, _entry, prior, worker, receipt, now_ms, false) do
    execution_lease = Map.take(worker, [:issue_id, :repository, :generation, :session_id, :process_id])

    if expired_restart_blocked_pair?(graph, prior, now_ms) do
      retire_expired_restart_blocked_pair(graph, prior, execution_lease, receipt, now_ms)
    else
      retire_reconciled_pair(graph, prior, execution_lease, receipt, now_ms)
    end
  end

  defp retire_expired_restart_blocked_pair(graph, prior, execution_lease, receipt, now_ms) do
    case ResponsibilityGraph.retire_expired_restart_blocked_successor_pair(
           graph,
           prior.accountable_id,
           prior.responsible_id,
           execution_lease,
           receipt,
           now_ms
         ) do
      {:ok, retired_graph, :retired} -> {:ok, retired_graph, :retired}
      _ -> {:error, :expired_restart_blocked_pair_retirement_failed}
    end
  end

  defp retire_reconciled_pair(graph, prior, execution_lease, receipt, now_ms) do
    with {:ok, reconciled_graph} <- reconcile_restart_blocked_pair(graph, prior, execution_lease, now_ms),
         {:ok, released_graph, _result} <-
           ResponsibilityGraph.release_runtime_lease(reconciled_graph, prior.responsible_id, execution_lease, now_ms),
         {:ok, retired_graph, :retired} <-
           ResponsibilityGraph.retire_unsubmitted_successor_pair(
             released_graph,
             prior.accountable_id,
             prior.responsible_id,
             receipt,
             now_ms
           ) do
      {:ok, retired_graph, :retired}
    else
      _ -> {:error, :prior_responsibility_release_failed}
    end
  end

  defp expired_restart_blocked_pair?(graph, prior, now_ms) do
    accountable = graph.delegations[prior.accountable_id]
    responsible = graph.delegations[prior.responsible_id]

    prior_pair_state?(accountable, responsible) and accountable.status == :blocked and
      accountable.expires_at_ms <= now_ms and responsible.expires_at_ms <= now_ms
  end

  defp reconcile_restart_blocked_pair(graph, prior, execution_lease, now_ms) do
    responsible = graph.delegations[prior.responsible_id]
    accountable = graph.delegations[prior.accountable_id]

    if prior_pair_state?(accountable, responsible) and accountable.status == :blocked do
      Unsubmitted.reconcile_restart_blocked_pair(
        graph,
        prior.accountable_id,
        prior.responsible_id,
        execution_lease,
        now_ms
      )
    else
      {:ok, graph}
    end
  end

  defp retire_fence(fence, execution, worker, entry, receipt, now_ms) do
    token = %{issue_id: entry.issue_id, generation: execution.generation}
    evidence = fence_evidence(entry, execution, worker, receipt)

    with {:ok, released_fence} <- release_fence_if_active(fence, execution, worker, token),
         {:ok, retired_fence, result} <- ExecutionFence.retire_unsubmitted(released_fence, token, evidence, now_ms) do
      {:ok, retired_fence, result}
    else
      _ -> {:error, :prior_execution_retirement_failed}
    end
  end

  defp release_fence_if_active(fence, %{status: :active}, worker, token),
    do: ExecutionFence.release_unsubmitted_claim(fence, token, worker.session_id)

  defp release_fence_if_active(fence, %{status: :retired}, _worker, _token), do: {:ok, fence}
  defp release_fence_if_active(_fence, _execution, _worker, _token), do: {:error, :prior_execution_state_conflict}

  defp fence_evidence(entry, execution, worker, receipt) do
    %{
      active_process: :absent,
      evidence_ref: receipt["evidence_ref"],
      generation: execution.generation,
      issue_id: execution.issue_id,
      linear_state: worker.linear_state,
      local_claim: :absent,
      provider_claim: :absent,
      provider_projection_id: entry.responsible.scope.work_package_id,
      workspace: :absent,
      type: "unsubmitted_successor",
      repository_ref: receipt["repository_ref"],
      managed_project_profile_id: receipt["managed_project_profile_id"],
      prior_accountable_id: receipt["prior_accountable_id"],
      prior_responsible_id: receipt["prior_responsible_id"],
      prior_accountable_digest: receipt["prior_accountable_digest"],
      prior_responsible_digest: receipt["prior_responsible_digest"],
      successor_accountable_id: receipt["successor_accountable_id"],
      successor_responsible_id: receipt["successor_responsible_id"],
      successor_accountable_digest: receipt["successor_accountable_digest"],
      successor_responsible_digest: receipt["successor_responsible_digest"],
      manifest_sha256: receipt["manifest_sha256"],
      signer_key_sha256: receipt["signer_key_sha256"],
      observation_sha256: receipt["observation_sha256"]
    }
  end

  defp plain_directory_ancestors?(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory}} ->
        parent = Path.dirname(path)
        parent == path or plain_directory_ancestors?(parent)

      _ ->
        false
    end
  end

  defp grant_digest(grant) do
    grant
    |> Map.take(@grant_fields)
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp digest(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp valid_sha256?(value) when is_binary(value), do: Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp valid_sha256?(_value), do: false
  defp valid_resource_version?(value), do: is_binary(value) and byte_size(value) in 1..128
end
