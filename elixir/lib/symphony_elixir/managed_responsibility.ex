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

  def admit(graph, %{schema_version: version, entries: entries, repository_ref: repository}, issue, now_ms, recovery)
      when version in [1, 2] and is_map(graph) and is_list(entries) and is_map(issue) and is_integer(now_ms) and now_ms >= 0 do
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
  @spec assignment_context(map(), map(), String.t()) :: {:ok, map()} | {:error, term()}
  def assignment_context(%{schema_version: 2, entries: entries, repository_ref: repository}, issue, delegation_id)
      when is_list(entries) and is_map(issue) and is_binary(delegation_id) do
    issue_id = Map.get(issue, :id)

    case Enum.find(entries, fn
           %{issue_id: ^issue_id, responsible: %{id: ^delegation_id}} -> true
           _ -> false
         end) do
      %{assignment_context: context} = entry when is_map(context) ->
        case validate_assignment_context(2, entry, issue, repository) do
          :ok -> {:ok, context}
          {:error, _reason} = error -> error
        end

      _ ->
        {:error, :managed_assignment_context_missing}
    end
  end

  def assignment_context(_manifest, _issue, _delegation_id), do: {:error, :managed_assignment_context_missing}

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
         {:ok, assignment_context} <- decode_assignment_context(raw, version) do
      entry = %{
        issue_id: raw["issue_id"],
        identifier: raw["identifier"],
        owner_id: raw["owner_id"],
        accountable: accountable,
        responsible: responsible,
        assignment_context: assignment_context
      }

      if Map.has_key?(raw, "prior_authority_revocation_ref"),
        do: {:ok, Map.put(entry, :prior_authority_revocation_ref, raw["prior_authority_revocation_ref"])},
        else: {:ok, entry}
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
      (exact_keys?(raw, @v2_entry_keys ++ ["prior_authority_revocation_ref"]) and valid_revocation_ref?(raw["prior_authority_revocation_ref"]))
  end

  defp decode_assignment_context(_raw, 1), do: {:ok, nil}

  defp decode_assignment_context(raw, 2) do
    context = raw["assignment_context"]

    with true <- is_map(context) and exact_keys?(context, ~w(objective base_ref environment)),
         objective when is_map(objective) <- context["objective"],
         true <- exact_keys?(objective, ~w(id content)),
         true <- valid_assignment_text?(objective["id"]) and valid_assignment_text?(objective["content"]),
         true <- context["base_ref"] == @base_ref,
         environment when is_map(environment) <- context["environment"],
         true <- exact_keys?(environment, ~w(platform classification constraints)),
         true <- environment["platform"] in @supported_platforms,
         true <- environment["classification"] == "repository",
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
         environment_constraints: Enum.uniq(constraints) |> Enum.sort()
       }}
    else
      _ -> {:error, :invalid_managed_assignment_context}
    end
  end

  defp validate_assignment_context(1, _entry, _issue, _repository),
    do: {:error, :managed_assignment_context_missing}

  defp validate_assignment_context(
         2,
         %{assignment_context: context, responsible: %{scope: %{objective_id: scope_objective_id, repository: scope_repository}}},
         issue,
         repository
       )
       when is_map(context) do
    expected_content = objective_content(issue)

    if Map.get(context, :objective_id) == scope_objective_id and
         Map.get(context, :objective_content) == expected_content and Map.get(context, :base_ref) == @base_ref and
         Map.get(context, :environment_classification) == "repository" and
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
  defp present?(value), do: is_binary(value) and String.trim(value) != ""
end
