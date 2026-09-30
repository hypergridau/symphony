defmodule SymphonyElixir.ManagedResponsibility do
  @moduledoc """
  Validates static operator authorization and proposes one exact responsibility pair.
  Intents remain inert until the normal orchestrator admits a fresh native issue.
  """

  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.ResponsibilityGraph.Persistence
  alias SymphonyElixir.WorkPackageClaim.Unsubmitted

  @routing [:pool_key, :repository_ref, :managed_project_profile_id]
  @payload_keys ~w(schema_version pool_key repository_ref managed_project_profile_id authority_ref entries)
  @entry_keys ~w(issue_id identifier owner_id accountable responsible)
  @v2_entry_keys @entry_keys ++ ["assignment_context"]
  @prior_unsubmitted_authority_keys ~w(issue_id generation repository_ref managed_project_profile_id accountable_id responsible_id accountable_digest responsible_digest)
  @unsubmitted_observation_keys ~w(issue_id generation repository_ref managed_project_profile_id journal_path execution_fence_path responsibility_graph_path provider_projection_id provider_reservation_state provider_claimed_at provider_claim_generation provider_execution_fence_token provider_observed_at_ms provider_evidence_ref kubernetes_namespace kubernetes_job_issue_matches kubernetes_pod_issue_matches kubernetes_jobs_resource_version kubernetes_pods_resource_version kubernetes_jobs_evidence_ref kubernetes_pods_evidence_ref kubernetes_observed_at_ms process_unit process_load_state process_active_state process_control_group process_main_pid process_count process_observed_at_ms process_evidence_ref workspace_absent workspace_observed_at_ms workspace_evidence_ref)
  @observation_timestamp_keys ~w(provider_observed_at_ms kubernetes_observed_at_ms process_observed_at_ms workspace_observed_at_ms)
  @observation_digest_keys ~w(provider_evidence_ref kubernetes_jobs_evidence_ref kubernetes_pods_evidence_ref process_evidence_ref workspace_evidence_ref)
  @base_ref "refs/remotes/origin/main"
  @supported_platforms ["linux-x86_64"]
  @scope_ids [:company_id, :objective_id, :initiative_id, :project_id, :work_package_id, :issue_id, :repository]
  @uuid ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z/
  @identifier ~r/\A[A-Z][A-Z0-9_]*-[0-9]+\z/

  @spec decode(term(), term(), term()) :: {:ok, map()} | {:error, term()}
  def decode(payload, context, now_ms) when is_map(payload) and is_map(context) and is_integer(now_ms) and now_ms >= 0 do
    version = payload["schema_version"]

    with true <- exact_keys?(payload, @payload_keys) and version in [1, 2],
         true <- Enum.all?(@routing, &(present?(context[&1]) and payload[Atom.to_string(&1)] == context[&1])),
         true <- present?(payload["authority_ref"]),
         true <- present?(context[:runner_id]),
         entries when is_list(entries) and length(entries) in 0..20 <- payload["entries"],
         {:ok, decoded} <- decode_entries(entries, context, now_ms, version),
         true <- unique?(decoded) do
      manifest = Map.new(@routing, &{&1, context[&1]})
      {:ok, Map.merge(manifest, %{schema_version: version, authority_ref: payload["authority_ref"], entries: decoded})}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_managed_delegation_manifest}
    end
  end

  def decode(_payload, _context, _now_ms), do: {:error, :invalid_managed_delegation_manifest}

  @spec admit(map(), map() | nil, map(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def admit(graph, manifest, issue, now_ms), do: admit(graph, manifest, issue, now_ms, nil)

  @spec admit(map(), map() | nil, map(), non_neg_integer(), map() | nil) :: {:ok, map()} | {:error, term()}
  def admit(graph, nil, _issue, _now_ms, _recovery), do: {:ok, graph}

  def admit(
        graph,
        %{schema_version: version, entries: entries, repository_ref: repository},
        issue,
        now_ms,
        recovery
      )
      when version in [1, 2] and is_map(graph) and is_list(entries) and is_map(issue) and
             is_integer(now_ms) and now_ms >= 0 do
    with :ok <- ResponsibilityGraph.validate(graph),
         entry when is_map(entry) <- Enum.find(entries, &(&1.issue_id == issue.id and &1.identifier == issue.identifier)),
         :ok <- validate_assignment_context(version, entry, issue, repository),
         true <- entry.owner_id == issue.assignee_id,
         true <- entry.accountable.expires_at_ms > now_ms and entry.responsible.expires_at_ms > now_ms,
         false <- repository_busy?(graph, entry.responsible.id, repository, recovery, now_ms) do
      ensure_pair(graph, entry, now_ms)
    else
      {:error, _reason} = error -> error
      _ -> {:error, :managed_delegation_not_admissible}
    end
  end

  def admit(_graph, _manifest, _issue, _now_ms, _recovery), do: {:error, :invalid_managed_delegation_input}

  @doc false
  @spec assignment_context(map(), map(), String.t(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def assignment_context(
        %{schema_version: 2, entries: entries, repository_ref: repository},
        issue,
        delegation_id,
        now_ms
      )
      when is_list(entries) and is_map(issue) and is_binary(delegation_id) and is_integer(now_ms) and now_ms >= 0 do
    issue_id = Map.get(issue, :id)

    case Enum.find(entries, fn
           %{issue_id: ^issue_id, identifier: identifier, responsible: %{id: ^delegation_id}} ->
             identifier == Map.get(issue, :identifier)

           _ ->
             false
         end) do
      %{
        assignment_context: context,
        owner_id: owner_id,
        accountable: %{expires_at_ms: accountable_expires},
        responsible: %{expires_at_ms: responsible_expires}
      } = entry
      when is_map(context) ->
        case validate_assignment_authority(owner_id, issue, accountable_expires, responsible_expires, now_ms) do
          :ok -> validate_and_return_assignment_context(entry, context, issue, repository)
          {:error, _reason} = error -> error
        end

      _ ->
        {:error, :managed_assignment_context_missing}
    end
  end

  def assignment_context(_manifest, _issue, _delegation_id, _now_ms),
    do: {:error, :managed_assignment_context_missing}

  defp validate_assignment_authority(owner_id, issue, accountable_expires, responsible_expires, now_ms) do
    cond do
      owner_id != Map.get(issue, :assignee_id) -> {:error, :managed_assignment_owner_drift}
      accountable_expires <= now_ms or responsible_expires <= now_ms -> {:error, :managed_delegation_expired}
      true -> :ok
    end
  end

  defp validate_and_return_assignment_context(entry, context, issue, repository) do
    case validate_assignment_context(2, entry, issue, repository) do
      :ok -> {:ok, context}
      {:error, _reason} = error -> error
    end
  end

  defp decode_entries(entries, context, now_ms, version) do
    Enum.reduce_while(entries, {:ok, []}, fn raw, {:ok, acc} ->
      case decode_entry(raw, context, now_ms, version) do
        {:ok, entry} -> {:cont, {:ok, acc ++ [entry]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp decode_entry(raw, context, now_ms, version) when is_map(raw) do
    with true <- entry_keys?(raw, version),
         true <- is_binary(raw["issue_id"]) and Regex.match?(@uuid, raw["issue_id"]),
         true <- is_binary(raw["identifier"]) and Regex.match?(@identifier, raw["identifier"]),
         true <- present?(raw["owner_id"]),
         {:ok, accountable} <- Persistence.decode_delegation_input(raw["accountable"]),
         {:ok, responsible} <- Persistence.decode_delegation_input(raw["responsible"]),
         true <- accountable.role == :accountable and is_nil(accountable.parent_delegation_id),
         true <- accountable.actor_id == raw["owner_id"],
         true <- responsible.role == :responsible and responsible.parent_delegation_id == accountable.id,
         true <- responsible.actor_id == context.runner_id,
         true <- responsible.budget.max_children == 0,
         true <- accountable.scope == responsible.scope,
         true <- bounded_scope?(responsible.scope, raw["issue_id"], context.repository_ref),
         true <- Enum.all?([accountable, responsible], &repository_authority?/1),
         {:ok, first, _} <- ResponsibilityGraph.delegate(ResponsibilityGraph.new(), accountable, now_ms),
         {:ok, _validated, _} <- ResponsibilityGraph.delegate(first, responsible, now_ms),
         {:ok, assignment_context} <- decode_assignment_context(raw, version),
         {:ok, prior_authority, observation} <-
           decode_unsubmitted_successor(raw, version, context, raw["issue_id"], accountable, responsible) do
      entry = %{
        issue_id: raw["issue_id"],
        identifier: raw["identifier"],
        owner_id: raw["owner_id"],
        accountable: accountable,
        responsible: responsible,
        assignment_context: assignment_context
      }

      entry =
        entry
        |> maybe_put(:prior_authority_revocation_ref, raw["prior_authority_revocation_ref"])
        |> maybe_put(:prior_unsubmitted_authority, prior_authority)
        |> maybe_put(:unsubmitted_observation, observation)

      {:ok, entry}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_managed_delegation_entry}
    end
  end

  defp decode_entry(_raw, _repository, _now_ms, _version), do: {:error, :invalid_managed_delegation_entry}

  defp entry_keys?(raw, 1) do
    exact_keys?(raw, @entry_keys) or
      (exact_keys?(raw, @entry_keys ++ ["prior_authority_revocation_ref"]) and valid_revocation_ref?(raw["prior_authority_revocation_ref"]))
  end

  defp entry_keys?(raw, 2) do
    exact_keys?(raw, @v2_entry_keys) or
      (exact_keys?(raw, @v2_entry_keys ++ ["prior_authority_revocation_ref"]) and valid_revocation_ref?(raw["prior_authority_revocation_ref"])) or
      exact_keys?(raw, @v2_entry_keys ++ ["prior_unsubmitted_authority", "unsubmitted_observation"])
  end

  defp decode_unsubmitted_successor(raw, 2, context, issue_id, accountable, responsible) do
    case {Map.fetch(raw, "prior_unsubmitted_authority"), Map.fetch(raw, "unsubmitted_observation")} do
      {:error, :error} ->
        {:ok, nil, nil}

      {{:ok, prior}, {:ok, observation}} ->
        with {:ok, decoded_prior} <- decode_prior_unsubmitted_authority(prior, context, issue_id, responsible),
             true <- decoded_prior.accountable_id not in [accountable.id, responsible.id],
             true <- decoded_prior.responsible_id not in [accountable.id, responsible.id],
             {:ok, decoded_observation} <-
               decode_unsubmitted_observation(observation, context, issue_id, decoded_prior, responsible) do
          {:ok, decoded_prior, decoded_observation}
        else
          _ -> {:error, :invalid_unsubmitted_successor_authority}
        end

      _ ->
        {:error, :invalid_unsubmitted_successor_authority}
    end
  end

  defp decode_unsubmitted_successor(_raw, 1, _context, _issue_id, _accountable, _responsible), do: {:ok, nil, nil}

  defp decode_prior_unsubmitted_authority(raw, context, issue_id, responsible) when is_map(raw) do
    with true <- exact_keys?(raw, @prior_unsubmitted_authority_keys),
         true <- raw["issue_id"] == issue_id,
         true <- is_integer(raw["generation"]) and raw["generation"] > 0,
         true <- raw["repository_ref"] == context.repository_ref,
         true <- raw["managed_project_profile_id"] == context.managed_project_profile_id,
         true <- present?(raw["accountable_id"]) and present?(raw["responsible_id"]),
         true <- raw["accountable_id"] != raw["responsible_id"],
         true <- raw["accountable_id"] != responsible.id and raw["responsible_id"] != responsible.id,
         true <- valid_sha256?(raw["accountable_digest"]),
         true <- valid_sha256?(raw["responsible_digest"]) do
      {:ok,
       %{
         issue_id: issue_id,
         generation: raw["generation"],
         repository_ref: raw["repository_ref"],
         managed_project_profile_id: raw["managed_project_profile_id"],
         accountable_id: raw["accountable_id"],
         responsible_id: raw["responsible_id"],
         accountable_digest: raw["accountable_digest"],
         responsible_digest: raw["responsible_digest"]
       }}
    else
      _ -> {:error, :invalid_unsubmitted_successor_authority}
    end
  end

  defp decode_prior_unsubmitted_authority(_raw, _context, _issue_id, _responsible),
    do: {:error, :invalid_unsubmitted_successor_authority}

  defp decode_unsubmitted_observation(raw, context, issue_id, prior, responsible) when is_map(raw) do
    with true <- exact_keys?(raw, @unsubmitted_observation_keys),
         true <- raw["issue_id"] == issue_id and raw["issue_id"] == prior.issue_id,
         true <- raw["generation"] == prior.generation,
         true <- raw["repository_ref"] == context.repository_ref and raw["repository_ref"] == prior.repository_ref,
         true <- raw["managed_project_profile_id"] == context.managed_project_profile_id,
         true <- valid_unsubmitted_state_paths?(raw, context.pool_key),
         true <- raw["provider_projection_id"] == responsible.scope.work_package_id,
         true <- raw["provider_reservation_state"] == "reserved",
         true <- is_nil(raw["provider_claimed_at"]) and is_nil(raw["provider_claim_generation"]),
         true <- is_nil(raw["provider_execution_fence_token"]),
         true <- raw["kubernetes_namespace"] == "frigga",
         true <- raw["kubernetes_job_issue_matches"] == 0 and raw["kubernetes_pod_issue_matches"] == 0,
         true <- valid_resource_version?(raw["kubernetes_jobs_resource_version"]),
         true <- valid_resource_version?(raw["kubernetes_pods_resource_version"]),
         true <- raw["process_load_state"] == "not-found",
         true <- raw["process_active_state"] in ["inactive", "unknown"],
         true <- is_nil(raw["process_control_group"]) and raw["process_main_pid"] in [nil, 0] and raw["process_count"] == 0,
         true <- raw["workspace_absent"] == true,
         true <- Enum.all?(@observation_timestamp_keys, &(is_integer(raw[&1]) and raw[&1] >= 0)),
         true <- Enum.all?(@observation_digest_keys, &valid_sha256?(raw[&1])),
         true <- present?(raw["process_unit"]) do
      {:ok, raw}
    else
      _ -> {:error, :invalid_unsubmitted_successor_observation}
    end
  end

  defp decode_unsubmitted_observation(_raw, _context, _issue_id, _prior, _responsible),
    do: {:error, :invalid_unsubmitted_successor_observation}

  defp valid_sha256?(value) when is_binary(value), do: Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp valid_sha256?(_value), do: false

  defp valid_resource_version?(value), do: is_binary(value) and byte_size(value) in 1..128

  defp valid_unsubmitted_state_paths?(observation, pool_key) when is_binary(pool_key) do
    pool_pattern = ~r/\A[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}\z/
    base = "/srv/dahlia-runner-state"

    expected = %{
      "journal_path" => Path.join([base, "run", "pools", pool_key, "work-package.json"]),
      "execution_fence_path" => Path.join([base, "workspaces", "pools", pool_key, ".symphony", "execution-fence.json"]),
      "responsibility_graph_path" => Path.join([base, "workspaces", "pools", pool_key, ".symphony", "responsibility-graph.json"])
    }

    Regex.match?(pool_pattern, pool_key) and
      Enum.all?(expected, fn {key, path} ->
        observation[key] == path
      end)
  end

  defp valid_unsubmitted_state_paths?(_observation, _pool_key), do: false

  defp decode_assignment_context(_raw, 1), do: {:ok, nil}

  defp decode_assignment_context(raw, 2) do
    context = raw["assignment_context"]

    with true <- is_map(context) and exact_keys?(context, ~w(objective base_ref environment placement target_environment)),
         objective when is_map(objective) <- context["objective"],
         true <- exact_keys?(objective, ~w(id content)),
         true <- valid_assignment_text?(objective["id"]) and valid_assignment_text?(objective["content"]),
         true <- context["base_ref"] == @base_ref,
         environment when is_map(environment) <- context["environment"],
         true <- exact_keys?(environment, ~w(platform classification constraints)),
         true <- environment["platform"] in @supported_platforms,
         true <- environment["classification"] == "repository",
         {:ok, placement, target_environment} <- decode_placement(context["placement"], context["target_environment"]),
         constraints when is_list(constraints) and constraints != [] <- environment["constraints"],
         true <- Enum.all?(constraints, &valid_assignment_text?/1),
         true <- length(constraints) <= 32 do
      {:ok,
       %{
         objective_id: objective["id"],
         objective_content: objective["content"],
         base_ref: context["base_ref"],
         platform: environment["platform"],
         environment_classification: environment["classification"],
         environment_constraints: Enum.uniq(constraints) |> Enum.sort(),
         placement: placement,
         target_environment: target_environment
       }}
    else
      _ -> {:error, :invalid_managed_assignment_context}
    end
  end

  defp validate_assignment_context(1, _entry, _issue, _repository),
    do: {:error, :managed_assignment_context_missing}

  defp validate_assignment_context(
         2,
         %{
           assignment_context: context,
           responsible: %{scope: %{objective_id: scope_objective_id, repository: scope_repository}}
         },
         issue,
         repository
       )
       when is_map(context) do
    expected_content = objective_content(issue)

    if Map.get(context, :objective_id) == scope_objective_id and
         Map.get(context, :objective_content) == expected_content and Map.get(context, :base_ref) == @base_ref and
         Map.get(context, :environment_classification) == "repository" and
         valid_placement?(Map.get(context, :placement), Map.get(context, :target_environment)) and
         scope_repository == repository do
      :ok
    else
      {:error, :managed_assignment_context_drift}
    end
  end

  defp validate_assignment_context(_version, _entry, _issue, _repository),
    do: {:error, :managed_assignment_context_missing}

  defp objective_content(%{title: title, description: description}) when is_binary(title) do
    case description do
      value when is_binary(value) ->
        if String.trim(value) == "", do: title, else: title <> "\n\n" <> value

      _ ->
        title
    end
  end

  defp objective_content(_issue), do: nil

  defp valid_assignment_text?(value) when is_binary(value) do
    byte_size(value) in 1..8_192 and String.valid?(value) and String.trim(value) != "" and
      Enum.all?(:binary.bin_to_list(value), &(&1 in [9, 10, 13] or (&1 > 31 and &1 != 127)))
  end

  defp valid_assignment_text?(_value), do: false

  defp decode_placement("internal_beta", "rke2"), do: {:ok, :internal_beta, :rke2}
  defp decode_placement("hosted_production", "lke"), do: {:ok, :hosted_production, :lke}
  defp decode_placement(_placement, _target_environment), do: {:error, :invalid_managed_assignment_context}

  defp valid_placement?(:internal_beta, :rke2), do: true
  defp valid_placement?(:hosted_production, :lke), do: true
  defp valid_placement?(_placement, _target_environment), do: false

  defp valid_revocation_ref?(ref) when is_binary(ref), do: Regex.match?(~r/\Asha256:[0-9a-f]{64}\z/, ref)
  defp valid_revocation_ref?(_ref), do: false

  defp repository_authority?(delegation) do
    delegation.authority.class == :routine_engineering and delegation.authority.environments == ["repository"]
  end

  defp bounded_scope?(scope, issue_id, repository) do
    Enum.all?(@scope_ids, &(present?(scope[&1]) and not String.contains?(scope[&1], "*"))) and
      scope.issue_id == issue_id and scope.repository == repository and scope.environments == ["repository"] and
      scope.paths != [] and Enum.all?(scope.paths, &safe_path?/1)
  end

  defp ensure_pair(graph, entry, now_ms) do
    account = entry.accountable
    responsible = entry.responsible

    case {graph.delegations[account.id], graph.delegations[responsible.id]} do
      {nil, nil} ->
        with {:ok, first, _} <- ResponsibilityGraph.delegate(graph, account, now_ms),
             {:ok, second, _} <- ResponsibilityGraph.delegate(first, responsible, now_ms) do
          {:ok, second}
        end

      {%{status: :active} = existing_account, %{status: :active} = existing_responsible} ->
        if immutable_match?(existing_account, account) and immutable_match?(existing_responsible, responsible) do
          {:ok, graph}
        else
          {:error, :managed_delegation_changed}
        end

      _ ->
        {:error, :managed_delegation_pair_not_active}
    end
  end

  defp immutable_match?(existing, attrs), do: Map.take(existing, Map.keys(attrs)) == attrs

  defp repository_busy?(graph, selected_id, repository, recovery, now_ms) do
    Enum.any?(graph.delegations, fn {id, delegation} ->
      id != selected_id and delegation.role == :responsible and
        delegation.status in [:active, :blocked] and delegation.scope.repository == repository and
        not unsubmitted_delegation?(graph, delegation, recovery, now_ms)
    end)
  end

  defp unsubmitted_delegation?(graph, %{runtime_lease: nil} = delegation, %{runtime: runtime, fence: fence}, now_ms)
       when is_map(runtime) and is_map(fence) do
    with %{entries: entries} <- runtime[:managed_delegations],
         entry when is_map(entry) <- Enum.find(entries, &(&1.issue_id == delegation.scope.issue_id)),
         true <- entry.responsible.id == delegation.id,
         execution when is_map(execution) <- fence.executions[delegation.scope.issue_id] do
      Unsubmitted.released_without_workspace?(runtime, fence, graph, execution, now_ms)
    else
      _ -> false
    end
  end

  defp unsubmitted_delegation?(_graph, _delegation, _recovery, _now_ms), do: false

  defp unique?(entries) do
    ids = Enum.flat_map(entries, &[&1.accountable.id, &1.responsible.id])
    issues = Enum.map(entries, & &1.issue_id)
    identifiers = Enum.map(entries, & &1.identifier)
    Enum.all?([ids, issues, identifiers], &(&1 == Enum.uniq(&1)))
  end

  defp safe_path?("."), do: true

  defp safe_path?(path) when is_binary(path) do
    present?(path) and not String.contains?(path, ["\\", ":"]) and
      Enum.all?(String.split(path, "/"), &(&1 not in ["", ".", ".."]))
  end

  defp safe_path?(_path), do: false

  defp exact_keys?(map, keys), do: MapSet.new(Map.keys(map)) == MapSet.new(keys)
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
  defp present?(value), do: is_binary(value) and String.trim(value) != ""
end
