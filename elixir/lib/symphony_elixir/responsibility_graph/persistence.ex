defmodule SymphonyElixir.ResponsibilityGraph.Persistence do
  @moduledoc """
  Durable JSON storage for the responsibility graph.

  Only the typed delegation contract is persisted. The runtime lease is a
  reference to HGS-294 ownership; this module never creates or reconstructs a
  second process/session registry.
  """

  alias SymphonyElixir.ResponsibilityGraph

  @schema_version 1
  @roles [:accountable, :responsible, :reviewer, :consulted, :observer]
  @statuses [:active, :blocked, :handed_off, :revoked, :failed, :completed, :expired]
  @enforcements [:manual, :enforced]
  @actions [:read, :observe, :delegate, :reconcile, :edit, :commit, :push, :state_mutation, :cleanup, :review, :report]
  @efforts [:none, :minimal, :low, :medium, :high, :xhigh, :max, :ultra]
  @budget_modes [:finite, :progress_scoped]
  @progress_models ["gpt-5.6-luna", "gpt-6-luna"]
  @classes [:routine_engineering, :coordination, :read_only, :exception]
  @scope_identifiers [:company_id, :objective_id, :initiative_id, :project_id, :work_package_id, :issue_id, :repository]
  @scope_collections [:paths, :modules, :environments, :actions]

  @type load_result :: {:ok, ResponsibilityGraph.state()} | :missing | {:error, term()}

  @doc "Decodes operator-issued delegation input without accepting runtime history or leases."
  @spec decode_delegation_input(map()) :: {:ok, map()} | {:error, term()}
  def decode_delegation_input(payload) when is_map(payload) do
    fields = ~w(id parent_delegation_id role actor_id scope authority budget expires_at_ms expected_deliverable expected_evidence return_to_parent)

    with true <- input_keys?(payload, fields),
         true <- input_keys?(payload["scope"], Enum.map(@scope_identifiers ++ @scope_collections, &Atom.to_string/1)),
         true <- input_keys?(payload["authority"], ~w(class capabilities environments)),
         true <- input_keys?(payload["budget"], ~w(model effort mode max_tokens max_children)),
         true <- input_keys?(payload["return_to_parent"], ~w(owner_id contract)),
         {:ok, id} <- required_string(payload, "id"),
         {:ok, parent_id} <- optional_string(payload, "parent_delegation_id"),
         {:ok, role} <- decode_atom(Map.get(payload, "role"), @roles),
         {:ok, actor_id} <- required_string(payload, "actor_id"),
         {:ok, scope} <- decode_scope(Map.get(payload, "scope")),
         {:ok, authority} <- decode_authority(Map.get(payload, "authority")),
         {:ok, budget} <- decode_budget(Map.get(payload, "budget")),
         {:ok, expiry} <- required_integer(payload, "expires_at_ms"),
         {:ok, deliverable} <- required_string(payload, "expected_deliverable"),
         {:ok, evidence} <- required_string(payload, "expected_evidence"),
         {:ok, return_contract} <- decode_return_contract(Map.get(payload, "return_to_parent")) do
      {:ok,
       %{
         id: id,
         parent_delegation_id: parent_id,
         role: role,
         actor_id: actor_id,
         scope: scope,
         authority: authority,
         budget: budget,
         expires_at_ms: expiry,
         expected_deliverable: deliverable,
         expected_evidence: evidence,
         return_to_parent: return_contract
       }}
    else
      false -> {:error, :unexpected_delegation_input_field}
      {:error, _reason} = error -> error
    end
  end

  def decode_delegation_input(_payload), do: {:error, :invalid_delegation_input}

  defp input_keys?(payload, fields) when is_map(payload), do: Map.keys(payload) -- fields == []
  defp input_keys?(_payload, _fields), do: false

  @doc "Decodes exact authenticated graph bytes without reading or writing a filesystem path."
  @spec decode_bytes(binary()) :: {:ok, ResponsibilityGraph.state()} | {:error, term()}
  def decode_bytes(contents) when is_binary(contents) do
    with {:ok, payload} <- Jason.decode(contents),
         {:ok, state} <- decode_state(payload),
         :ok <- ResponsibilityGraph.validate(state) do
      {:ok, state}
    else
      {:error, reason} -> {:error, {:invalid_snapshot, reason}}
      other -> {:error, {:invalid_snapshot, other}}
    end
  end

  def decode_bytes(_contents), do: {:error, {:invalid_snapshot, :invalid_snapshot_bytes}}

  @spec load(Path.t()) :: load_result()
  def load(path) when is_binary(path) do
    case File.read(path) do
      {:ok, contents} ->
        decode_bytes(contents)

      {:error, :enoent} ->
        :missing

      {:error, reason} ->
        {:error, {:read_failed, reason}}
    end
  end

  @spec save(Path.t(), ResponsibilityGraph.state()) :: :ok | {:error, term()}
  def save(path, state) when is_binary(path) do
    with :ok <- ResponsibilityGraph.validate(state),
         {:ok, encoded} <- encode_state(state),
         :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- atomic_write(path, encoded) do
      :ok
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end

  defp encode_state(state) do
    with :ok <- validate_budget_modes(state) do
      Jason.encode(%{
        "schema_version" => @schema_version,
        "enforcement" => Atom.to_string(Map.get(state, :enforcement, :manual)),
        "delegations" => Map.new(state.delegations, fn {id, delegation} -> {id, encode_delegation(delegation)} end),
        "events" => Enum.map(state.events, &encode_value/1)
      })
    end
  end

  defp encode_delegation(delegation) do
    %{
      "id" => delegation.id,
      "parent_delegation_id" => delegation.parent_delegation_id,
      "role" => Atom.to_string(delegation.role),
      "actor_id" => delegation.actor_id,
      "scope" => encode_scope(delegation.scope),
      "authority" => encode_authority(delegation.authority),
      "budget" => encode_budget(delegation.budget),
      "runtime_lease" => encode_runtime_lease(delegation.runtime_lease),
      "status" => Atom.to_string(delegation.status),
      "accepted_at_ms" => delegation.accepted_at_ms,
      "last_heartbeat_at" => delegation.last_heartbeat_at,
      "expires_at_ms" => delegation.expires_at_ms,
      "expected_deliverable" => delegation.expected_deliverable,
      "expected_evidence" => delegation.expected_evidence,
      "return_to_parent" => encode_value(delegation.return_to_parent),
      "blocked_on" => encode_reason(delegation.blocked_on),
      "terminal_reason" => encode_reason(delegation.terminal_reason),
      "terminal_evidence" => encode_value(delegation.terminal_evidence),
      "metadata" => encode_value(Map.get(delegation, :metadata)),
      "reviewer_delegation_id" => Map.get(delegation, :reviewer_delegation_id),
      "finding" => Map.get(delegation, :finding)
    }
  end

  defp encode_scope(scope) do
    Map.new(@scope_identifiers ++ @scope_collections, fn key ->
      value = Map.get(scope, key)
      {Atom.to_string(key), if(key in @scope_identifiers, do: encode_identifier(value), else: encode_collection(key, value))}
    end)
  end

  defp encode_authority(authority) do
    %{
      "class" => Atom.to_string(authority.class),
      "capabilities" => Enum.map(authority.capabilities, &Atom.to_string/1),
      "environments" => authority.environments
    }
  end

  defp encode_budget(budget) do
    encoded = %{
      "model" => budget.model,
      "effort" => Atom.to_string(budget.effort),
      "max_tokens" => budget.max_tokens,
      "max_children" => budget.max_children
    }

    case Map.fetch(budget, :mode) do
      {:ok, mode} -> Map.put(encoded, "mode", Atom.to_string(mode))
      :error -> encoded
    end
  end

  defp encode_runtime_lease(nil), do: nil
  defp encode_runtime_lease(lease), do: encode_value(lease)

  defp encode_identifier(:any), do: "*"
  defp encode_identifier(value), do: value

  defp encode_collection(:actions, values), do: Enum.map(values, &Atom.to_string/1)
  defp encode_collection(_key, values), do: values

  defp encode_reason(nil), do: nil
  defp encode_reason(value) when is_atom(value), do: Atom.to_string(value)
  defp encode_reason(value) when is_binary(value), do: value
  defp encode_reason(value), do: inspect(value, limit: :infinity)

  defp encode_value(nil), do: nil
  defp encode_value(value) when is_binary(value) or is_number(value) or is_boolean(value), do: value
  defp encode_value(value) when is_atom(value), do: Atom.to_string(value)
  defp encode_value(value) when is_list(value), do: Enum.map(value, &encode_value/1)

  defp encode_value(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {if(is_atom(key), do: Atom.to_string(key), else: to_string(key)), encode_value(nested)} end)
  end

  defp encode_value(value), do: inspect(value, limit: :infinity)

  defp decode_state(%{"schema_version" => @schema_version, "delegations" => delegations, "events" => events} = payload)
       when is_map(delegations) and is_list(events) do
    with {:ok, enforcement} <- decode_atom(Map.get(payload, "enforcement", "manual"), @enforcements),
         {:ok, decoded_delegations} <- decode_map(delegations, &decode_delegation/1) do
      {:ok,
       %{
         schema_version: @schema_version,
         enforcement: enforcement,
         delegations: decoded_delegations,
         events: events
       }}
    end
  end

  defp decode_state(_payload), do: {:error, :unsupported_schema}

  defp decode_delegation(payload) when is_map(payload) do
    with {:ok, id} <- required_string(payload, "id"),
         {:ok, parent_delegation_id} <- optional_string(payload, "parent_delegation_id"),
         {:ok, role} <- decode_atom(Map.get(payload, "role"), @roles),
         {:ok, actor_id} <- required_string(payload, "actor_id"),
         {:ok, scope} <- decode_scope(Map.get(payload, "scope")),
         {:ok, authority} <- decode_authority(Map.get(payload, "authority")),
         {:ok, budget} <- decode_budget(Map.get(payload, "budget")),
         {:ok, runtime_lease} <- decode_runtime_lease(Map.get(payload, "runtime_lease")),
         {:ok, status} <- decode_atom(Map.get(payload, "status"), @statuses),
         {:ok, accepted_at_ms} <- required_integer(payload, "accepted_at_ms"),
         {:ok, last_heartbeat_at} <- required_integer(payload, "last_heartbeat_at"),
         {:ok, expires_at_ms} <- required_integer(payload, "expires_at_ms"),
         {:ok, expected_deliverable} <- required_string(payload, "expected_deliverable"),
         {:ok, expected_evidence} <- required_string(payload, "expected_evidence"),
         {:ok, return_to_parent} <- decode_return_contract(Map.get(payload, "return_to_parent")) do
      {:ok,
       %{
         id: id,
         parent_delegation_id: parent_delegation_id,
         role: role,
         actor_id: actor_id,
         scope: scope,
         authority: authority,
         budget: budget,
         runtime_lease: runtime_lease,
         status: status,
         accepted_at_ms: accepted_at_ms,
         last_heartbeat_at: last_heartbeat_at,
         expires_at_ms: expires_at_ms,
         expected_deliverable: expected_deliverable,
         expected_evidence: expected_evidence,
         return_to_parent: return_to_parent,
         blocked_on: decode_blocked_on(Map.get(payload, "blocked_on")),
         terminal_reason: Map.get(payload, "terminal_reason"),
         terminal_evidence: Map.get(payload, "terminal_evidence"),
         metadata: Map.get(payload, "metadata"),
         reviewer_delegation_id: Map.get(payload, "reviewer_delegation_id"),
         finding: Map.get(payload, "finding")
       }}
    end
  end

  defp decode_delegation(_payload), do: {:error, :invalid_delegation}

  defp decode_scope(payload) when is_map(payload) do
    with {:ok, identifiers} <- decode_scope_values(payload, @scope_identifiers),
         {:ok, collections} <- decode_scope_collections(payload, @scope_collections) do
      {:ok, Map.merge(identifiers, collections)}
    end
  end

  defp decode_scope(_payload), do: {:error, :invalid_scope}

  defp decode_scope_values(payload, keys) do
    Enum.reduce_while(keys, {:ok, %{}}, fn key, {:ok, acc} ->
      case Map.fetch(payload, Atom.to_string(key)) do
        {:ok, "*"} -> {:cont, {:ok, Map.put(acc, key, :any)}}
        {:ok, value} when is_binary(value) and value != "" -> {:cont, {:ok, Map.put(acc, key, value)}}
        _ -> {:halt, {:error, :invalid_scope}}
      end
    end)
  end

  defp decode_scope_collections(payload, keys) do
    Enum.reduce_while(keys, {:ok, %{}}, fn key, {:ok, acc} ->
      case Map.fetch(payload, Atom.to_string(key)) do
        {:ok, values} when is_list(values) ->
          case decode_collection(key, values) do
            {:ok, decoded} -> {:cont, {:ok, Map.put(acc, key, decoded)}}
            error -> {:halt, error}
          end

        _ ->
          {:halt, {:error, :invalid_scope}}
      end
    end)
  end

  defp decode_collection(:actions, values), do: decode_atoms(values, @actions)

  defp decode_collection(_key, values) do
    if Enum.all?(values, &(is_binary(&1) and &1 != "")), do: {:ok, values}, else: {:error, :invalid_scope}
  end

  defp decode_authority(%{"class" => class, "capabilities" => capabilities, "environments" => environments}) do
    with {:ok, class} <- decode_atom(class, @classes),
         {:ok, capabilities} <- decode_atoms(capabilities, @actions),
         true <- is_list(environments) and Enum.all?(environments, &(is_binary(&1) and &1 != "")) do
      {:ok, %{class: class, capabilities: capabilities, environments: environments}}
    else
      false -> {:error, :invalid_authority}
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_authority}
    end
  end

  defp decode_authority(_payload), do: {:error, :invalid_authority}

  defp decode_budget(%{"model" => model, "effort" => effort, "max_tokens" => max_tokens, "max_children" => max_children} = payload) do
    with {:ok, effort} <- decode_atom(effort, @efforts),
         {:ok, mode, explicit_mode?} <- decode_budget_mode(payload),
         true <- is_binary(model) and model != "" and valid_budget_limit?(mode, max_tokens) and is_integer(max_children) and max_children >= 0,
         true <- valid_budget_model?(mode, model) do
      budget = %{model: model, effort: effort, max_tokens: max_tokens, max_children: max_children}
      {:ok, if(explicit_mode?, do: Map.put(budget, :mode, mode), else: budget)}
    else
      false -> {:error, :invalid_budget}
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_budget}
    end
  end

  defp decode_budget(_payload), do: {:error, :invalid_budget}

  defp decode_budget_mode(payload) do
    case Map.fetch(payload, "mode") do
      :error ->
        {:ok, :finite, false}

      {:ok, mode} ->
        case decode_atom(mode, @budget_modes) do
          {:ok, decoded} -> {:ok, decoded, true}
          {:error, _reason} = error -> error
        end
    end
  end

  defp validate_budget_modes(%{delegations: delegations}) when is_map(delegations) do
    if Enum.all?(delegations, fn {_id, delegation} -> valid_encoded_budget?(Map.get(delegation, :budget)) end),
      do: :ok,
      else: {:error, :invalid_budget}
  end

  defp validate_budget_modes(_state), do: {:error, :invalid_budget}

  defp valid_encoded_budget?(budget) when is_map(budget) do
    case Map.fetch(budget, :mode) do
      :error -> valid_budget_limit?(:finite, Map.get(budget, :max_tokens))
      {:ok, mode} when mode in @budget_modes ->
        valid_budget_model?(mode, Map.get(budget, :model)) and
          valid_budget_limit?(mode, Map.get(budget, :max_tokens))
      _ -> false
    end
  end

  defp valid_encoded_budget?(_budget), do: false

  defp valid_budget_limit?(:progress_scoped, nil), do: true
  defp valid_budget_limit?(:finite, maximum), do: is_integer(maximum) and maximum > 0
  defp valid_budget_limit?(_mode, _maximum), do: false

  defp valid_budget_model?(:progress_scoped, model) when model in @progress_models, do: true
  defp valid_budget_model?(:progress_scoped, _model), do: false
  defp valid_budget_model?(:finite, _model), do: true

  defp decode_runtime_lease(nil), do: {:ok, nil}

  defp decode_runtime_lease(payload) when is_map(payload) do
    required = ["issue_id", "repository", "generation", "session_id", "process_id"]

    if Enum.all?(required, &Map.has_key?(payload, &1)) and is_binary(payload["issue_id"]) and
         is_binary(payload["repository"]) and is_integer(payload["generation"]) and
         is_binary(payload["session_id"]) and is_binary(payload["process_id"]) do
      {:ok,
       %{
         issue_id: payload["issue_id"],
         repository: payload["repository"],
         generation: payload["generation"],
         session_id: payload["session_id"],
         process_id: payload["process_id"]
       }}
    else
      {:error, :invalid_runtime_lease}
    end
  end

  defp decode_runtime_lease(_payload), do: {:error, :invalid_runtime_lease}

  defp decode_return_contract(%{"owner_id" => owner_id, "contract" => contract})
       when is_binary(owner_id) and is_binary(contract) and owner_id != "" and contract != "" do
    {:ok, %{owner_id: owner_id, contract: contract}}
  end

  defp decode_return_contract(_payload), do: {:error, :invalid_return_contract}

  defp decode_blocked_on("restart_reconciliation"), do: :restart_reconciliation
  defp decode_blocked_on(value), do: value

  defp decode_map(payload, decoder) do
    Enum.reduce_while(payload, {:ok, %{}}, fn
      {key, value}, {:ok, acc} when is_binary(key) ->
        case decoder.(value) do
          {:ok, decoded} -> {:cont, {:ok, Map.put(acc, key, decoded)}}
          {:error, _reason} = error -> {:halt, error}
        end

      _entry, _acc ->
        {:halt, {:error, :invalid_map_key}}
    end)
  end

  defp decode_atoms(values, allowed) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case decode_atom(value, allowed) do
        {:ok, atom} -> {:cont, {:ok, [atom | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp decode_atoms(_values, _allowed), do: {:error, :invalid_atom_list}

  defp decode_atom(value, allowed) when is_binary(value) do
    case Enum.find(allowed, &(Atom.to_string(&1) == value)) do
      nil -> {:error, {:invalid_atom, value}}
      atom -> {:ok, atom}
    end
  end

  defp decode_atom(_value, _allowed), do: {:error, :invalid_atom}

  defp required_string(payload, key) do
    case Map.get(payload, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_or_invalid, key}}
    end
  end

  defp optional_string(payload, key) do
    case Map.get(payload, key) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:invalid, key}}
    end
  end

  defp required_integer(payload, key) do
    case Map.get(payload, key) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, {:missing_or_invalid, key}}
    end
  end

  defp atomic_write(path, contents) do
    temporary_path = "#{path}.tmp-#{System.unique_integer([:positive])}"

    result =
      with :ok <- write_synced(temporary_path, contents),
           :ok <- replace_file(temporary_path, path) do
        :ok
      end

    if result != :ok, do: File.rm(temporary_path)
    result
  end

  defp write_synced(path, contents) do
    case :file.open(String.to_charlist(path), [:write, :binary, :raw, :sync]) do
      {:ok, handle} ->
        try do
          :file.write(handle, contents)
        after
          :file.close(handle)
        end

      {:error, reason} ->
        {:error, {:write_failed, reason}}
    end
  end

  defp replace_file(temporary_path, path) do
    case File.rename(temporary_path, path) do
      :ok ->
        :ok

      {:error, :eexist} ->
        with :ok <- File.rm(path), :ok <- File.rename(temporary_path, path), do: :ok

      {:error, reason} ->
        {:error, {:rename_failed, reason}}
    end
  end
end
