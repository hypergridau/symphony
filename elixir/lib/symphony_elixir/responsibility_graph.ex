defmodule SymphonyElixir.ResponsibilityGraph do
  @moduledoc """
  Pure, versioned responsibility and delegation contract for the management
  hierarchy above issue execution.

  A delegation is an authority record, not a process registry. Runtime
  identity is held as a reference to one Symphony execution-fence lease, so
  mutation authorization can require both organisational authority and the
  HGS-294 generation/session fence.
  """

  alias SymphonyElixir.ExecutionFence

  @schema_version 1
  @roles [:accountable, :responsible, :reviewer, :consulted, :observer]
  @statuses [:active, :blocked, :handed_off, :revoked, :failed, :completed, :expired]
  @mutable_actions [:edit, :commit, :push, :state_mutation, :cleanup]
  @actions [:read, :observe, :delegate, :reconcile, :edit, :commit, :push, :state_mutation, :cleanup, :review, :report]
  @authority_classes [:routine_engineering, :coordination, :read_only, :exception]
  @efforts [:none, :minimal, :low, :medium, :high, :xhigh, :max, :ultra]
  @progress_models ["gpt-5.6-luna", "gpt-6-luna"]
  @scope_identifiers [:company_id, :objective_id, :initiative_id, :project_id, :work_package_id, :issue_id, :repository]
  @scope_collections [:paths, :modules, :environments, :actions]

  @type state :: %{
          schema_version: 1,
          enforcement: :manual | :enforced,
          delegations: %{optional(String.t()) => map()},
          events: [map()]
        }

  @doc "Creates an empty responsibility graph."
  @spec new() :: state()
  def new, do: %{schema_version: @schema_version, enforcement: :manual, delegations: %{}, events: []}

  @doc "Returns whether worker admission must consume responsibility state."
  @spec enforced?(state()) :: boolean()
  def enforced?(%{enforcement: :enforced}), do: true
  def enforced?(_state), do: false

  @doc "Activates machine-enforced responsibility admission after the graph is proven."
  @spec activate(state(), non_neg_integer()) :: {:ok, state(), :activated | :already_activated} | {:error, term()}
  def activate(state, now_ms) when is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state) do
      if enforced?(state) do
        {:ok, state, :already_activated}
      else
        {:ok,
         state
         |> Map.put(:enforcement, :enforced)
         |> append_event(:enforcement_activated, nil, now_ms, %{}), :activated}
      end
    end
  end

  def activate(_state, _now_ms), do: {:error, :invalid_activation}

  @doc "Validates graph structure and all active authority invariants."
  @spec validate(state()) :: :ok | {:error, :invalid_state}
  def validate(state), do: validate_state(state)

  @doc "Returns a deterministic, read-only management projection."
  @spec snapshot(state()) :: map() | {:error, :invalid_state}
  def snapshot(state) do
    case validate_state(state) do
      :ok ->
        delegations =
          state.delegations
          |> Map.values()
          |> Enum.sort_by(& &1.id)
          |> Enum.map(&sanitize_delegation/1)

        edges =
          delegations
          |> Enum.filter(&is_binary(&1.parent_delegation_id))
          |> Enum.group_by(& &1.parent_delegation_id, & &1.id)
          |> Map.new(fn {parent_id, children} -> {parent_id, Enum.sort(children)} end)

        %{
          schema_version: @schema_version,
          enforcement: Map.get(state, :enforcement, :manual),
          delegations: delegations,
          edges: edges,
          events: state.events
        }

      {:error, :invalid_state} = error ->
        error
    end
  end

  @doc "Creates one explicit delegation after enforcing parent and overlap rules."
  @spec delegate(state(), map(), non_neg_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def delegate(state, attrs, now_ms) do
    with :ok <- validate_state(state),
         :ok <- validate_delegation_attrs(attrs, now_ms),
         :ok <- delegation_id_available(state, attrs.id),
         :ok <- parent_constraints(state, attrs),
         :ok <- accountability_available(state, attrs),
         :ok <- mutable_scope_available(state, attrs) do
      delegation =
        Map.merge(
          %{
            id: attrs.id,
            parent_delegation_id: Map.get(attrs, :parent_delegation_id),
            role: attrs.role,
            actor_id: attrs.actor_id,
            scope: attrs.scope,
            authority: attrs.authority,
            budget: attrs.budget,
            runtime_lease: Map.get(attrs, :runtime_lease),
            status: :active,
            accepted_at_ms: now_ms,
            last_heartbeat_at: now_ms,
            expires_at_ms: attrs.expires_at_ms,
            expected_deliverable: attrs.expected_deliverable,
            expected_evidence: attrs.expected_evidence,
            return_to_parent: attrs.return_to_parent,
            blocked_on: nil,
            terminal_reason: nil,
            terminal_evidence: nil,
            metadata: nil,
            reviewer_delegation_id: nil,
            finding: nil
          },
          Map.take(attrs, [:metadata, :reviewer_delegation_id, :finding])
        )

      next_state =
        state
        |> put_in([:delegations, delegation.id], delegation)
        |> append_event(:delegated, delegation.id, now_ms, %{role: delegation.role})

      {:ok, next_state, delegation}
    end
  end

  @doc "Renews an active delegation heartbeat."
  @spec heartbeat(state(), String.t(), non_neg_integer()) ::
          {:ok, state()} | {:error, term()}
  def heartbeat(state, delegation_id, now_ms)
      when is_binary(delegation_id) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state),
         {:ok, delegation} <- fetch_delegation(state, delegation_id),
         :ok <- active_delegation(delegation),
         :ok <- not_expired(delegation, now_ms) do
      updated = %{delegation | last_heartbeat_at: now_ms}

      {:ok,
       state
       |> put_in([:delegations, delegation_id], updated)
       |> append_event(:heartbeat, delegation_id, now_ms, %{})}
    end
  end

  def heartbeat(_state, _delegation_id, _now_ms), do: {:error, :invalid_heartbeat}

  @doc "Finds the unique active responsible delegation for an issue scope."
  @spec admission_delegation(state(), String.t(), String.t() | nil, String.t()) ::
          {:ok, map()} | {:error, term()}
  def admission_delegation(state, issue_id, identifier, repository)
      when is_binary(issue_id) and is_binary(repository) do
    with :ok <- validate_state(state) do
      issue_keys = Enum.filter([issue_id, identifier], &present_string?/1)

      candidates =
        state.delegations
        |> Map.values()
        |> Enum.filter(fn delegation ->
          delegation.status == :active and delegation.role == :responsible and
            delegation.scope.repository == repository and
            delegation.scope.issue_id in issue_keys
        end)
        |> Enum.sort_by(& &1.id)

      case candidates do
        [delegation] -> {:ok, delegation}
        [] -> {:error, :responsible_delegation_missing}
        delegations -> {:error, {:ambiguous_responsible_delegation, Enum.map(delegations, & &1.id)}}
      end
    end
  end

  def admission_delegation(_state, _issue_id, _identifier, _repository),
    do: {:error, :invalid_admission_scope}

  @doc "Binds one HGS-294 runtime lease to an active responsible delegation."
  @spec bind_runtime_lease(state(), String.t(), map(), non_neg_integer()) ::
          {:ok, state()} | {:error, term()}
  def bind_runtime_lease(state, delegation_id, runtime_lease, now_ms)
      when is_binary(delegation_id) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state),
         {:ok, delegation} <- fetch_delegation(state, delegation_id),
         :ok <- active_delegation(delegation),
         true <- delegation.role == :responsible,
         :ok <- validate_runtime_lease(runtime_lease),
         :ok <- runtime_lease_matches_scope?(delegation, runtime_lease),
         :ok <- runtime_lease_available?(delegation, runtime_lease) do
      updated = %{delegation | runtime_lease: runtime_lease, last_heartbeat_at: now_ms}

      {:ok,
       state
       |> put_in([:delegations, delegation_id], updated)
       |> append_event(:runtime_lease_bound, delegation_id, now_ms, %{})}
    else
      false -> {:error, :delegation_not_responsible}
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_runtime_lease}
    end
  end

  def bind_runtime_lease(_state, _delegation_id, _runtime_lease, _now_ms),
    do: {:error, :invalid_runtime_lease_binding}

  @doc "Releases an active delegation's runtime lease after a worker exits."
  @spec release_runtime_lease(state(), String.t(), map(), non_neg_integer()) ::
          {:ok, state(), :released | :already_released} | {:error, term()}
  def release_runtime_lease(state, delegation_id, runtime_lease, now_ms)
      when is_binary(delegation_id) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state),
         {:ok, delegation} <- fetch_delegation(state, delegation_id),
         :ok <- active_delegation(delegation) do
      case delegation.runtime_lease do
        nil ->
          {:ok, state, :already_released}

        ^runtime_lease ->
          release_bound_runtime_lease(state, delegation_id, delegation, now_ms)

        _other ->
          {:error, :runtime_lease_conflict}
      end
    end
  end

  def release_runtime_lease(_state, _delegation_id, _runtime_lease, _now_ms),
    do: {:error, :invalid_runtime_lease_release}

  @doc "Retires an expired delegation's exact lease with caller-verified never-submitted evidence."
  @spec retire_expired_unsubmitted(state(), String.t(), map() | nil, map(), non_neg_integer()) ::
          {:ok, state()} | {:error, term()}
  def retire_expired_unsubmitted(state, id, lease, evidence, now_ms) do
    with :ok <- validate_state(state),
         {:ok, delegation} <- fetch_delegation(state, id),
         true <- delegation.expires_at_ms <= now_ms,
         true <- evidence["type"] == "expired_never_submitted",
         :ok <- reconcile_clock_is_monotonic(delegation, now_ms) do
      cond do
        delegation.status == :expired and delegation.runtime_lease == nil and delegation.terminal_evidence == evidence ->
          {:ok, state}

        delegation.runtime_lease == lease and expired_unsubmitted_releasable?(delegation) ->
          persist_expired_unsubmitted(state, id, delegation, evidence, now_ms)

        true ->
          {:error, :expired_unsubmitted_authority_changed}
      end
    else
      _ -> {:error, :expired_unsubmitted_authority_changed}
    end
  end

  defp expired_unsubmitted_releasable?(%{status: :blocked, blocked_on: :restart_reconciliation}), do: true
  defp expired_unsubmitted_releasable?(%{status: :expired, terminal_reason: reason}) when reason in [:lease_expired, "lease_expired"], do: true
  defp expired_unsubmitted_releasable?(_delegation), do: false

  defp persist_expired_unsubmitted(state, id, delegation, evidence, now_ms) do
    updated =
      delegation
      |> Map.put(:status, :expired)
      |> Map.put(:runtime_lease, nil)
      |> Map.put(:terminal_reason, :expired_never_submitted)
      |> Map.put(:terminal_evidence, evidence)

    next =
      state
      |> put_in([:delegations, id], updated)
      |> append_event(:expired_never_submitted, id, now_ms, evidence)

    with :ok <- validate_state(next), do: {:ok, next}
  end

  defp release_bound_runtime_lease(state, delegation_id, delegation, now_ms) do
    with :ok <- reconcile_clock_is_monotonic(delegation, now_ms),
         updated <- %{delegation | runtime_lease: nil},
         next_state <-
           state
           |> put_in([:delegations, delegation_id], updated)
           |> append_event(:runtime_lease_released, delegation_id, now_ms, %{}),
         :ok <- validate_state(next_state) do
      {:ok, next_state, :released}
    end
  end

  @doc "Expires active or restart-blocked authority without releasing runtime leases."
  @spec reconcile(state(), non_neg_integer()) :: {:ok, state(), map()} | {:error, term()}
  def reconcile(state, now_ms) when is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state) do
      expired_ids =
        state.delegations
        |> Enum.filter(fn {_id, delegation} ->
          expirable_for_reconcile?(delegation) and delegation.expires_at_ms <= now_ms
        end)
        |> Enum.map(&elem(&1, 0))
        |> Enum.sort()

      next_state =
        Enum.reduce(expired_ids, state, fn delegation_id, acc ->
          delegation = acc.delegations[delegation_id]

          acc
          |> put_in([:delegations, delegation_id], %{delegation | status: :expired, terminal_reason: :lease_expired})
          |> append_event(:expired, delegation_id, now_ms, %{})
        end)

      with :ok <- validate_state(next_state) do
        {:ok, next_state, %{expired: expired_ids}}
      end
    end
  end

  def reconcile(_state, _now_ms), do: {:error, :invalid_reconciliation}

  @doc "Re-opens unexpired restart-blocked authority with its exact persisted runtime lease."
  @spec reconcile_delegation(state(), String.t(), map() | nil, non_neg_integer()) ::
          {:ok, state()} | {:error, term()}
  def reconcile_delegation(state, delegation_id, runtime_lease, now_ms)
      when is_binary(delegation_id) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state),
         {:ok, delegation} <- fetch_delegation(state, delegation_id),
         :ok <- blocked_for_restart(delegation),
         :ok <- not_expired(delegation, now_ms),
         :ok <- reconcile_clock_is_monotonic(delegation, now_ms),
         :ok <- parent_available_for_reconcile(state, delegation, now_ms),
         :ok <- validate_reconciled_runtime_lease(delegation, runtime_lease),
         :ok <- runtime_lease_matches_scope?(delegation, runtime_lease),
         :ok <- persisted_runtime_lease_matches?(delegation, runtime_lease) do
      updated = %{
        delegation
        | runtime_lease: runtime_lease,
          status: :active,
          blocked_on: nil,
          last_heartbeat_at: now_ms
      }

      next_state =
        state
        |> put_in([:delegations, delegation_id], updated)
        |> append_event(:reconciled, delegation_id, now_ms, %{})

      with :ok <- validate_state(next_state) do
        {:ok, next_state}
      end
    end
  end

  def reconcile_delegation(_state, _delegation_id, _runtime_lease, _now_ms),
    do: {:error, :invalid_reconciliation}

  @doc "Marks a delegation and all active descendants revoked."
  @spec revoke(state(), String.t(), term(), non_neg_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def revoke(state, delegation_id, reason, now_ms),
    do: terminalize(state, delegation_id, :revoked, reason, now_ms)

  @doc "Hands responsibility back to the parent and fences active descendants."
  @spec handoff(state(), String.t(), term(), non_neg_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def handoff(state, delegation_id, reason, now_ms),
    do: terminalize(state, delegation_id, :handed_off, reason, now_ms)

  @doc "Records a bounded failure and fences active descendants."
  @spec fail(state(), String.t(), term(), non_neg_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def fail(state, delegation_id, reason, now_ms),
    do: terminalize(state, delegation_id, :failed, reason, now_ms)

  @doc "Completes a delegation and fences any still-active descendants."
  @spec complete(state(), String.t(), term(), non_neg_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def complete(state, delegation_id, evidence, now_ms),
    do: terminalize(state, delegation_id, :completed, :completed, now_ms, evidence)

  @doc "Blocks a delegation and prevents active descendants from continuing."
  @spec block(state(), String.t(), term(), non_neg_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def block(state, delegation_id, reason, now_ms) do
    with :ok <- validate_state(state),
         {:ok, delegation} <- fetch_delegation(state, delegation_id),
         :ok <- active_delegation(delegation) do
      descendants = active_descendant_ids(state, delegation_id)
      affected = [delegation_id | descendants]

      next_state =
        Enum.reduce(affected, state, fn id, acc ->
          current = acc.delegations[id]

          updated =
            if id == delegation_id do
              %{current | status: :blocked, blocked_on: reason}
            else
              %{current | status: :revoked, terminal_reason: {:ancestor_blocked, delegation_id}}
            end

          acc
          |> put_in([:delegations, id], updated)
          |> append_event(if(id == delegation_id, do: :blocked, else: :revoked), id, now_ms, %{ancestor: delegation_id})
        end)

      {:ok, next_state, impact(next_state, affected)}
    end
  end

  @doc "Creates a reviewer finding proposal without granting the reviewer mutation authority."
  @spec propose_remediation(state(), String.t(), map(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()}
  def propose_remediation(state, reviewer_id, attrs, now_ms) when is_map(attrs) do
    with :ok <- validate_state(state),
         {:ok, reviewer} <- fetch_delegation(state, reviewer_id),
         :ok <- active_delegation(reviewer),
         :ok <- reviewer_role(reviewer),
         parent_id when is_binary(parent_id) <- reviewer.parent_delegation_id,
         {:ok, parent} <- fetch_delegation(state, parent_id),
         :ok <- manager_role(parent),
         :ok <- validate_remediation_attrs(attrs, now_ms),
         :ok <- scope_subset?(attrs.scope, parent.scope),
         :ok <- authority_subset?(attrs.authority, parent.authority),
         :ok <- budget_subset?(attrs.budget, parent.budget) do
      {:ok,
       Map.merge(attrs, %{
         parent_delegation_id: parent_id,
         role: :responsible,
         reviewer_delegation_id: reviewer_id,
         finding: attrs.finding
       })}
    else
      nil -> {:error, :reviewer_missing_manager}
      {:error, _reason} = error -> error
      _ -> {:error, :reviewer_missing_manager}
    end
  end

  def propose_remediation(_state, _reviewer_id, _attrs, _now_ms),
    do: {:error, :invalid_remediation}

  @doc "Accepts a reviewer proposal through the normal governed delegation path."
  @spec delegate_remediation(state(), String.t(), map(), non_neg_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def delegate_remediation(state, reviewer_id, attrs, now_ms) do
    with {:ok, proposal} <- propose_remediation(state, reviewer_id, attrs, now_ms) do
      delegate(state, proposal, now_ms)
    end
  end

  @doc "Authorizes one graph action, including parent child-scope protection."
  @spec authorize(state(), String.t(), atom()) :: {:ok, map()} | {:error, term()}
  def authorize(state, delegation_id, action) when action in @actions do
    with :ok <- validate_state(state),
         {:ok, delegation} <- fetch_delegation(state, delegation_id),
         :ok <- active_delegation(delegation),
         :ok <- capability_allowed(delegation, action),
         :ok <- role_allowed(delegation, action),
         :ok <- parent_scope_available(state, delegation, action) do
      {:ok, %{delegation_id: delegation.id, role: delegation.role, action: action, scope: delegation.scope}}
    end
  end

  def authorize(_state, _delegation_id, _action), do: {:error, :unsupported_action}

  @doc "Authorizes a mutable action against both the graph and HGS-294 lease."
  @spec authorize_with_execution_fence(state(), String.t(), atom(), ExecutionFence.state()) ::
          {:ok, map()} | {:error, term()}
  def authorize_with_execution_fence(state, delegation_id, action, fence_state) do
    with {:ok, graph_authorization} <- authorize(state, delegation_id, action),
         {:ok, delegation} <- fetch_delegation(state, delegation_id),
         :ok <- runtime_authority_required(delegation, action),
         {:ok, lease} <- active_runtime_lease(fence_state, delegation),
         {:ok, fence_authorization} <-
           ExecutionFence.authorize(fence_state, execution_token(lease), fence_action(action)) do
      {:ok, Map.merge(graph_authorization, %{execution_fence: fence_authorization, runtime_lease: lease})}
    end
  end

  @doc "Revokes a delegation tree and fences each referenced HGS-294 generation."
  @spec revoke_and_fence(state(), ExecutionFence.state(), String.t(), term(), map(), non_neg_integer()) ::
          {:ok, state(), ExecutionFence.state(), map()} | {:error, term()}
  def revoke_and_fence(state, fence_state, delegation_id, reason, terminal_attrs, now_ms) do
    with {:ok, next_graph, impact} <- revoke(state, delegation_id, reason, now_ms),
         {:ok, next_fence} <- fence_runtime_leases(fence_state, impact.execution_leases, terminal_attrs, now_ms) do
      {:ok, next_graph, next_fence, impact}
    end
  end

  @doc "Blocks live delegations after restart unless they are unbound responsible delegations awaiting first admission."
  @spec mark_unreconciled_after_restart(state()) :: {:ok, state()} | {:error, :invalid_state}
  def mark_unreconciled_after_restart(state) do
    with :ok <- validate_state(state) do
      {delegations, events} =
        Enum.reduce(state.delegations, {state.delegations, state.events}, fn {id, delegation}, {acc, event_acc} ->
          if delegation.status == :active and
               (delegation.role != :responsible or not is_nil(delegation.runtime_lease)) do
            updated = %{delegation | status: :blocked, blocked_on: :restart_reconciliation}
            {Map.put(acc, id, updated), [%{type: :restart_blocked, delegation_id: id} | event_acc]}
          else
            {acc, event_acc}
          end
        end)

      {:ok, %{state | delegations: delegations, events: events}}
    end
  end

  defp terminalize(state, delegation_id, status, reason, now_ms, evidence \\ nil) do
    with :ok <- validate_state(state),
         {:ok, delegation} <- fetch_delegation(state, delegation_id),
         :ok <- active_delegation(delegation) do
      descendants = active_descendant_ids(state, delegation_id)

      next_state =
        Enum.reduce([delegation_id | descendants], state, fn id, acc ->
          current = acc.delegations[id]

          updated =
            if id == delegation_id do
              %{current | status: status, terminal_reason: reason, terminal_evidence: evidence}
            else
              %{current | status: :revoked, terminal_reason: {:ancestor_terminal, delegation_id}}
            end

          acc
          |> put_in([:delegations, id], updated)
          |> append_event(if(id == delegation_id, do: status, else: :revoked), id, now_ms, %{ancestor: delegation_id})
        end)

      {:ok, next_state, impact(next_state, [delegation_id | descendants])}
    end
  end

  defp fence_runtime_leases(fence_state, leases, terminal_attrs, now_ms) do
    leases
    |> Enum.uniq_by(&{&1.issue_id, &1.generation})
    |> Enum.reduce_while({:ok, fence_state}, fn lease, {:ok, current_fence} ->
      case ExecutionFence.fence(current_fence, execution_token(lease), terminal_attrs, now_ms) do
        {:ok, next_fence, _result} -> {:cont, {:ok, next_fence}}
        {:error, reason} -> {:halt, {:error, {:execution_fence_failed, lease.session_id, reason}}}
      end
    end)
  end

  defp impact(state, ids) do
    leases =
      ids
      |> Enum.map(fn id -> state.delegations[id].runtime_lease end)
      |> Enum.reject(&is_nil/1)

    %{delegation_ids: ids, execution_leases: leases}
  end

  defp validate_state(%{schema_version: @schema_version, delegations: delegations, events: events} = state)
       when is_map(delegations) and is_list(events) do
    if Map.get(state, :enforcement, :manual) in [:manual, :enforced] and
         Enum.all?(delegations, fn {id, delegation} -> valid_delegation?(id, delegation) end) and
         Enum.all?(events, &is_map/1) and
         valid_parent_links?(delegations) and
         valid_scope_invariants?(delegations) do
      :ok
    else
      {:error, :invalid_state}
    end
  end

  defp validate_state(_state), do: {:error, :invalid_state}

  defp valid_delegation?(id, delegation) when is_binary(id) and is_map(delegation) do
    Map.get(delegation, :id) == id and
      Map.get(delegation, :role) in @roles and
      Map.get(delegation, :status) in @statuses and
      present_string?(Map.get(delegation, :actor_id)) and
      valid_scope?(Map.get(delegation, :scope)) and
      valid_authority?(Map.get(delegation, :authority)) and
      valid_budget?(Map.get(delegation, :budget)) and
      valid_runtime_lease_for_role?(Map.get(delegation, :role), Map.get(delegation, :runtime_lease)) and
      valid_clock?(delegation) and
      present_string?(Map.get(delegation, :expected_deliverable)) and
      present_string?(Map.get(delegation, :expected_evidence)) and
      valid_return_contract?(Map.get(delegation, :return_to_parent))
  end

  defp valid_delegation?(_id, _delegation), do: false

  defp valid_parent_links?(delegations) do
    Enum.all?(delegations, fn {_id, delegation} ->
      case delegation.parent_delegation_id do
        nil ->
          true

        parent_id when is_binary(parent_id) ->
          parent_id != delegation.id and Map.has_key?(delegations, parent_id) and acyclic?(delegations, delegation.id, MapSet.new())

        _ ->
          false
      end
    end) and
      Enum.all?(delegations, fn {_id, delegation} ->
        case Map.get(delegations, delegation.parent_delegation_id) do
          nil ->
            is_nil(delegation.parent_delegation_id)

          parent ->
            manager_role(parent) == :ok and scope_subset?(delegation.scope, parent.scope) == :ok and
              authority_subset?(delegation.authority, parent.authority) == :ok and
              budget_subset?(delegation.budget, parent.budget) == :ok
        end
      end)
  end

  defp acyclic?(delegations, id, seen) do
    if MapSet.member?(seen, id) do
      false
    else
      case Map.get(delegations, id) do
        nil -> true
        %{parent_delegation_id: nil} -> true
        %{parent_delegation_id: parent_id} -> acyclic?(delegations, parent_id, MapSet.put(seen, id))
      end
    end
  end

  defp valid_scope_invariants?(delegations) do
    active = Enum.filter(delegations, fn {_id, delegation} -> delegation.status == :active end)

    accountable_ok? =
      active
      |> Enum.filter(&(elem(&1, 1).role == :accountable))
      |> Enum.group_by(fn {_id, delegation} -> scope_key(delegation.scope) end)
      |> Enum.all?(fn {_scope, entries} -> length(entries) == 1 end)

    mutable_ok? =
      for {left_id, left} <- active,
          {right_id, right} <- active,
          left_id < right_id,
          left.role == :responsible,
          right.role == :responsible,
          scopes_overlap?(left.scope, right.scope),
          not ancestor_pair?(delegations, left_id, right_id),
          do: false

    accountable_ok? and mutable_ok? == []
  end

  defp validate_delegation_attrs(attrs, now_ms) when is_map(attrs) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- required_strings(attrs, [:id, :actor_id, :expected_deliverable, :expected_evidence]),
         true <- Map.get(attrs, :role) in @roles,
         :ok <- valid_scope_input(Map.get(attrs, :scope)),
         :ok <- valid_authority_input(Map.get(attrs, :authority)),
         :ok <- valid_budget_input(Map.get(attrs, :budget)),
         :ok <- valid_return_contract(Map.get(attrs, :return_to_parent)),
         :ok <- validate_expiry(Map.get(attrs, :expires_at_ms), now_ms),
         :ok <- validate_runtime_lease_for_role?(Map.get(attrs, :role), Map.get(attrs, :runtime_lease)) do
      :ok
    else
      false -> {:error, :invalid_delegation}
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_delegation}
    end
  end

  defp validate_delegation_attrs(_attrs, _now_ms), do: {:error, :invalid_delegation}

  defp validate_remediation_attrs(attrs, now_ms) do
    with :ok <- required_strings(attrs, [:finding]),
         :ok <- validate_delegation_attrs(Map.put(attrs, :role, :responsible), now_ms) do
      :ok
    end
  end

  defp delegation_id_available(state, id) do
    if Map.has_key?(state.delegations, id), do: {:error, :delegation_exists}, else: :ok
  end

  defp parent_constraints(_state, %{parent_delegation_id: nil}), do: :ok

  defp parent_constraints(state, attrs) do
    with {:ok, parent} <- fetch_delegation(state, attrs.parent_delegation_id),
         :ok <- manager_role(parent),
         :ok <- active_delegation(parent),
         :ok <- scope_subset?(attrs.scope, parent.scope),
         :ok <- authority_subset?(attrs.authority, parent.authority),
         :ok <- budget_subset?(attrs.budget, parent.budget),
         :ok <- child_capacity_available(state, parent) do
      :ok
    end
  end

  defp accountability_available(state, %{role: :accountable, scope: scope}) do
    if Enum.any?(state.delegations, fn {_id, delegation} ->
         delegation.status == :active and delegation.role == :accountable and delegation.scope == scope
       end) do
      {:error, :accountable_owner_exists}
    else
      :ok
    end
  end

  defp accountability_available(_state, _attrs), do: :ok

  defp mutable_scope_available(state, %{role: :responsible} = attrs) do
    conflict =
      Enum.find_value(state.delegations, fn {id, delegation} ->
        if delegation.status == :active and delegation.role == :responsible and
             scopes_overlap?(attrs.scope, delegation.scope) and
             not ancestor_pair_with_new?(state, attrs, id) do
          id
        end
      end)

    if conflict, do: {:error, {:overlapping_mutable_scope, conflict}}, else: :ok
  end

  defp mutable_scope_available(_state, _attrs), do: :ok

  defp ancestor_pair_with_new?(state, %{parent_delegation_id: parent_id}, existing_id) do
    is_binary(parent_id) and (parent_id == existing_id or is_descendant?(state, parent_id, existing_id))
  end

  defp ancestor_pair_with_new?(_state, _attrs, _existing_id), do: false

  defp child_capacity_available(state, parent) do
    children =
      Enum.count(state.delegations, fn {_id, delegation} ->
        delegation.parent_delegation_id == parent.id and delegation.status == :active
      end)

    if children < parent.budget.max_children, do: :ok, else: {:error, :child_budget_exhausted}
  end

  defp active_descendant_ids(state, ancestor_id) do
    state.delegations
    |> Enum.filter(fn {id, delegation} ->
      id != ancestor_id and delegation.status == :active and is_descendant?(state, id, ancestor_id)
    end)
    |> Enum.map(&elem(&1, 0))
    |> Enum.sort()
  end

  defp is_descendant?(state, candidate_id, ancestor_id) do
    case Map.get(state.delegations, candidate_id) do
      nil -> false
      %{parent_delegation_id: ^ancestor_id} -> true
      %{parent_delegation_id: nil} -> false
      %{parent_delegation_id: parent_id} -> is_descendant?(state, parent_id, ancestor_id)
    end
  end

  defp ancestor_pair?(delegations, left_id, right_id) do
    ancestor_id?(delegations, left_id, right_id) or ancestor_id?(delegations, right_id, left_id)
  end

  defp ancestor_id?(delegations, candidate, possible_ancestor) do
    case Map.get(delegations, candidate) do
      %{parent_delegation_id: ^possible_ancestor} -> true
      %{parent_delegation_id: nil} -> false
      %{parent_delegation_id: parent_id} -> ancestor_id?(delegations, parent_id, possible_ancestor)
      _ -> false
    end
  end

  defp parent_scope_available(state, delegation, action) do
    if action in @mutable_actions and delegation.role == :responsible do
      case Enum.find(state.delegations, fn {id, child} ->
             id != delegation.id and child.status == :active and child.role == :responsible and
               is_descendant?(state, id, delegation.id) and scopes_overlap?(delegation.scope, child.scope)
           end) do
        nil -> :ok
        {child_id, _child} -> {:error, {:child_scope_owned, child_id}}
      end
    else
      :ok
    end
  end

  defp active_runtime_lease(fence_state, delegation) do
    with :ok <- ExecutionFence.validate(fence_state),
         lease when is_map(lease) <- delegation.runtime_lease,
         {:ok, execution} <- current_execution(fence_state, lease),
         %{status: :active, role: :worker} = registered <- Map.get(execution.leases, lease.session_id) do
      if registered.process_id == lease.process_id do
        {:ok, registered}
      else
        {:error, :runtime_process_conflict}
      end
    else
      nil -> {:error, :runtime_lease_missing}
      {:error, _reason} = error -> error
      _ -> {:error, :runtime_lease_not_active}
    end
  end

  defp current_execution(fence_state, %{issue_id: issue_id, generation: generation}) do
    case Map.get(fence_state.executions, issue_id) do
      %{generation: ^generation} = execution -> {:ok, execution}
      nil -> {:error, :unknown_execution}
      _ -> {:error, :stale_generation}
    end
  end

  defp execution_token(%{issue_id: issue_id, generation: generation}),
    do: %{issue_id: issue_id, generation: generation}

  defp fence_action(:edit), do: :state_mutation
  defp fence_action(:cleanup), do: :state_mutation
  defp fence_action(action), do: action

  defp runtime_authority_required(delegation, action) do
    if action in @mutable_actions and is_nil(delegation.runtime_lease) do
      {:error, :runtime_lease_required}
    else
      :ok
    end
  end

  defp capability_allowed(delegation, action) do
    if action in delegation.authority.capabilities do
      :ok
    else
      {:error, {:capability_not_granted, action}}
    end
  end

  defp role_allowed(%{role: :responsible}, action), do: if(action in @mutable_actions or action in [:read, :report], do: :ok, else: {:error, :responsible_action_denied})
  defp role_allowed(%{role: :accountable}, action), do: if(action in [:read, :observe, :delegate, :reconcile, :report], do: :ok, else: {:error, :accountable_read_only})
  defp role_allowed(%{role: :reviewer}, action), do: if(action in [:read, :observe, :review, :report], do: :ok, else: {:error, :reviewer_read_only})
  defp role_allowed(%{role: role}, action) when role in [:consulted, :observer], do: if(action in [:read, :observe, :report], do: :ok, else: {:error, :advisory_read_only})

  defp manager_role(%{role: role}) when role in [:accountable, :responsible], do: :ok
  defp manager_role(_delegation), do: {:error, :parent_not_manager}

  defp reviewer_role(%{role: :reviewer}), do: :ok
  defp reviewer_role(_delegation), do: {:error, :not_reviewer}

  defp active_delegation(%{status: :active}), do: :ok
  defp active_delegation(%{status: status}), do: {:error, {:delegation_not_active, status}}

  defp blocked_for_restart(%{status: :blocked, blocked_on: :restart_reconciliation}), do: :ok
  defp blocked_for_restart(_delegation), do: {:error, :delegation_not_restart_blocked}

  defp expirable_for_reconcile?(%{status: :active}), do: true
  defp expirable_for_reconcile?(%{status: :blocked, blocked_on: :restart_reconciliation}), do: true
  defp expirable_for_reconcile?(_delegation), do: false

  defp reconcile_clock_is_monotonic(%{last_heartbeat_at: last_heartbeat_at}, now_ms) when now_ms < last_heartbeat_at,
    do: {:error, :delegation_clock_regression}

  defp reconcile_clock_is_monotonic(_delegation, _now_ms), do: :ok

  defp parent_available_for_reconcile(_state, %{parent_delegation_id: nil}, _now_ms), do: :ok

  defp parent_available_for_reconcile(state, %{parent_delegation_id: parent_id}, now_ms) do
    case Map.get(state.delegations, parent_id) do
      %{status: :active} = parent -> not_expired(parent, now_ms)
      _ -> {:error, :parent_not_reconciled}
    end
  end

  defp not_expired(%{expires_at_ms: expires_at_ms}, now_ms) when expires_at_ms > now_ms, do: :ok
  defp not_expired(_delegation, _now_ms), do: {:error, :delegation_expired}

  defp fetch_delegation(state, id) do
    case Map.get(state.delegations, id) do
      nil -> {:error, :unknown_delegation}
      delegation -> {:ok, delegation}
    end
  end

  defp append_event(state, type, delegation_id, now_ms, details) do
    %{state | events: [%{type: type, delegation_id: delegation_id, at_ms: now_ms, details: details} | state.events]}
  end

  defp valid_scope_input(scope) when is_map(scope) do
    if Enum.all?(@scope_identifiers, &identifier_value?(Map.get(scope, &1))) and
         Enum.all?(@scope_collections, &collection_value?(Map.get(scope, &1), &1)) do
      :ok
    else
      {:error, :invalid_scope}
    end
  end

  defp valid_scope_input(_scope), do: {:error, :invalid_scope}

  defp valid_scope?(scope), do: valid_scope_input(scope) == :ok

  defp identifier_value?(value), do: value == :any or present_string?(value)

  defp collection_value?(value, :actions), do: is_list(value) and Enum.all?(value, &(&1 in @actions)) and value == Enum.uniq(value)
  defp collection_value?(value, _key), do: is_list(value) and Enum.all?(value, &present_string?/1) and value == Enum.uniq(value)

  defp valid_authority_input(%{class: class, capabilities: capabilities, environments: environments}) do
    if class in @authority_classes and is_list(capabilities) and capabilities != [] and
         Enum.all?(capabilities, &(&1 in @actions)) and capabilities == Enum.uniq(capabilities) and
         is_list(environments) and Enum.all?(environments, &present_string?/1) and environments == Enum.uniq(environments) do
      :ok
    else
      {:error, :invalid_authority}
    end
  end

  defp valid_authority_input(_authority), do: {:error, :invalid_authority}
  defp valid_authority?(authority), do: valid_authority_input(authority) == :ok

  defp valid_budget_input(%{model: model, effort: effort, max_tokens: max_tokens, max_children: max_children} = budget) do
    if present_string?(model) and effort in @efforts and valid_token_limit?(Map.get(budget, :mode), model, max_tokens) and
         is_integer(max_children) and max_children >= 0 do
      :ok
    else
      {:error, :invalid_budget}
    end
  end

  defp valid_budget_input(_budget), do: {:error, :invalid_budget}
  defp valid_budget?(budget), do: valid_budget_input(budget) == :ok

  defp valid_token_limit?(:progress_scoped, model, nil) when model in @progress_models, do: true
  defp valid_token_limit?(mode, _model, maximum) when mode in [nil, :finite], do: is_integer(maximum) and maximum > 0
  defp valid_token_limit?(_mode, _model, _maximum), do: false

  defp valid_runtime_lease_for_role?(:responsible, nil), do: true
  defp valid_runtime_lease_for_role?(:responsible, lease), do: valid_runtime_lease(lease) == :ok
  defp valid_runtime_lease_for_role?(:reviewer, lease), do: valid_runtime_lease(lease) == :ok
  defp valid_runtime_lease_for_role?(_role, nil), do: true
  defp valid_runtime_lease_for_role?(_role, lease), do: valid_runtime_lease(lease) == :ok

  defp validate_runtime_lease_for_role?(:responsible, nil), do: :ok
  defp validate_runtime_lease_for_role?(:responsible, lease), do: validate_runtime_lease(lease)
  defp validate_runtime_lease_for_role?(:reviewer, lease), do: validate_runtime_lease(lease)
  defp validate_runtime_lease_for_role?(_role, nil), do: :ok
  defp validate_runtime_lease_for_role?(_role, lease), do: validate_runtime_lease(lease)

  defp validate_runtime_lease(%{issue_id: issue_id, repository: repository, generation: generation, session_id: session_id, process_id: process_id}) do
    if present_string?(issue_id) and present_string?(repository) and is_integer(generation) and generation > 0 and present_string?(session_id) and present_string?(process_id),
      do: :ok,
      else: {:error, :invalid_runtime_lease}
  end

  defp validate_runtime_lease(_lease), do: {:error, :invalid_runtime_lease}

  defp valid_runtime_lease(lease), do: validate_runtime_lease(lease)

  defp runtime_lease_available?(%{runtime_lease: nil}, _runtime_lease), do: :ok
  defp runtime_lease_available?(%{runtime_lease: runtime_lease}, runtime_lease), do: :ok
  defp runtime_lease_available?(_delegation, _runtime_lease), do: {:error, :runtime_lease_conflict}

  defp validate_reconciled_runtime_lease(%{role: role}, nil) when role not in [:responsible, :reviewer], do: :ok
  defp validate_reconciled_runtime_lease(_delegation, lease), do: validate_runtime_lease(lease)

  defp persisted_runtime_lease_matches?(%{runtime_lease: lease}, lease), do: :ok
  defp persisted_runtime_lease_matches?(_delegation, _lease), do: {:error, :runtime_lease_conflict}

  defp runtime_lease_matches_scope?(delegation, lease) do
    if is_nil(lease) or (lease.issue_id == delegation.scope.issue_id and lease.repository == delegation.scope.repository) do
      :ok
    else
      {:error, :runtime_scope_mismatch}
    end
  end

  defp valid_clock?(delegation) do
    is_integer(delegation.accepted_at_ms) and delegation.accepted_at_ms >= 0 and
      is_integer(delegation.last_heartbeat_at) and delegation.last_heartbeat_at >= delegation.accepted_at_ms and
      is_integer(delegation.expires_at_ms) and delegation.expires_at_ms > delegation.last_heartbeat_at
  end

  defp validate_expiry(expires_at_ms, now_ms) when is_integer(expires_at_ms) and expires_at_ms > now_ms, do: :ok
  defp validate_expiry(_expires_at_ms, _now_ms), do: {:error, :invalid_expiry}

  defp valid_return_contract?(%{owner_id: owner_id, contract: contract}), do: present_string?(owner_id) and present_string?(contract)
  defp valid_return_contract?(_contract), do: false
  defp valid_return_contract(contract), do: if(valid_return_contract?(contract), do: :ok, else: {:error, :invalid_return_contract})

  defp scope_key(scope) do
    @scope_identifiers
    |> Enum.map(&Map.get(scope, &1))
    |> Kernel.++(Enum.map(@scope_collections, fn key -> {key, Map.get(scope, key) |> Enum.sort()} end))
  end

  defp required_strings(attrs, keys) do
    if Enum.all?(keys, &present_string?(Map.get(attrs, &1))), do: :ok, else: {:error, :missing_delegation_field}
  end

  defp scope_subset?(child, parent) do
    if Enum.all?(@scope_identifiers, &subset_value?(Map.get(child, &1), Map.get(parent, &1))) and
         Enum.all?(@scope_collections, &subset_collection?(Map.get(child, &1), Map.get(parent, &1))) do
      :ok
    else
      {:error, :scope_widening}
    end
  end

  defp subset_value?(_child, :any), do: true
  defp subset_value?(child, parent), do: child == parent

  defp subset_collection?(_child, []), do: true
  defp subset_collection?(child, parent), do: is_list(child) and Enum.all?(child, &(&1 in parent))

  defp authority_subset?(child, parent) do
    if subset_value?(child.class, parent.class) and subset_collection?(child.capabilities, parent.capabilities) and
         subset_collection?(child.environments, parent.environments) do
      :ok
    else
      {:error, :authority_widening}
    end
  end

  defp budget_subset?(child, parent) do
    if token_budget_subset?(child, parent) and child.max_children <= parent.max_children and
         effort_rank(child.effort) <= effort_rank(parent.effort) and child.model == parent.model do
      :ok
    else
      {:error, :budget_widening}
    end
  end

  defp token_budget_subset?(%{mode: :progress_scoped, max_tokens: nil}, %{mode: :progress_scoped, max_tokens: nil}), do: true
  defp token_budget_subset?(%{max_tokens: maximum} = child, %{mode: :progress_scoped, max_tokens: nil})
       when is_integer(maximum) and maximum > 0,
       do: Map.get(child, :mode, :finite) == :finite
  defp token_budget_subset?(%{max_tokens: child}, %{max_tokens: parent})
       when is_integer(child) and is_integer(parent),
       do: child <= parent
  defp token_budget_subset?(_child, _parent), do: false

  defp effort_rank(effort), do: Enum.find_index(@efforts, &(&1 == effort)) || -1

  defp scopes_overlap?(left, right) do
    Enum.all?(@scope_identifiers, &overlap_value?(Map.get(left, &1), Map.get(right, &1))) and
      Enum.all?(@scope_collections, &overlap_collection?(Map.get(left, &1), Map.get(right, &1)))
  end

  defp overlap_value?(:any, _right), do: true
  defp overlap_value?(_left, :any), do: true
  defp overlap_value?(left, right), do: left == right

  defp overlap_collection?([], _right), do: true
  defp overlap_collection?(_left, []), do: true
  defp overlap_collection?(left, right), do: MapSet.intersection(MapSet.new(left), MapSet.new(right)) |> MapSet.size() > 0

  defp sanitize_delegation(delegation) do
    Map.take(delegation, [
      :id,
      :parent_delegation_id,
      :role,
      :actor_id,
      :scope,
      :authority,
      :budget,
      :runtime_lease,
      :status,
      :accepted_at_ms,
      :last_heartbeat_at,
      :expires_at_ms,
      :expected_deliverable,
      :expected_evidence,
      :return_to_parent,
      :blocked_on,
      :terminal_reason,
      :terminal_evidence,
      :metadata,
      :reviewer_delegation_id,
      :finding
    ])
  end

  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""
end
