defmodule SymphonyElixir.RuntimeIdentity do
  @moduledoc """
  Builds the read-only identity and authority posture advertised by the
  observability state endpoint.

  The launcher supplies the pool identity and accepted source revision.  The
  workspace and pause paths come from the effective workflow configuration,
  while the authority posture is derived from the durable execution fence and
  responsibility graph.  This module never exposes provider credentials and
  does not create a second source of runtime state.
  """

  alias SymphonyElixir.{Config, ExecutionFence, GlobalPause, ResponsibilityGraph}

  @fence_contract "hgs294"
  @delegation_contract "hgs300"
  @identity_fields [:pool_key, :repository_ref, :workspace_root, :global_pause_file, :accepted_source_head]

  @type snapshot :: map()

  @doc "Returns the current sanitized identity and execution authority posture."
  @spec snapshot(map(), map(), keyword()) :: snapshot()
  def snapshot(fence_state, responsibility_state, opts \\ [])
      when is_map(fence_state) and is_map(responsibility_state) and is_list(opts) do
    env = Keyword.get(opts, :env, System.get_env())
    managed_pool? = Keyword.get(opts, :managed_pool?, managed_pool?(env))
    managed_runtime_configured? = Keyword.get(opts, :managed_runtime_configured?, false)

    identity = identity(env, opts)
    authority = authority(fence_state, responsibility_state)
    source_head_status = source_head_status(identity.accepted_source_head, env)
    missing = Enum.filter(@identity_fields, &blank?(Map.get(identity, &1)))

    identity_status =
      cond do
        not managed_pool? and not managed_runtime_configured? -> "unmanaged"
        missing != [] -> "missing"
        source_head_status in ["stale", "invalid"] -> source_head_status
        true -> "configured"
      end

    readiness_reasons =
      readiness_reasons(
        managed_pool?,
        managed_runtime_configured?,
        missing,
        source_head_status,
        authority
      )

    %{
      runtime_identity:
        identity
        |> Map.put(:status, identity_status)
        |> Map.put(:source_head_status, source_head_status),
      execution_authority: authority,
      managed_work_package:
        manifest_projection(
          %{
            required?: managed_pool?,
            configured?: managed_runtime_configured?,
            state: managed_work_package_state(managed_pool?, managed_runtime_configured?)
          },
          Keyword.get(opts, :managed_delegation_manifest)
        ),
      readiness: %{
        ready?: readiness_reasons == [],
        status: if(readiness_reasons == [], do: "ready", else: "not_ready"),
        reasons: readiness_reasons
      }
    }
  end

  defp manifest_projection(projection, %{source_sha256: digest, signer_key_sha256: signer, entries: entries})
       when is_binary(digest) and is_binary(signer) and is_list(entries) do
    Map.put(projection, :delegation_manifest, %{state: "configured", sha256: digest, signer_key_sha256: signer, authorized_issue_count: length(entries)})
  end

  defp manifest_projection(projection, _manifest), do: projection

  defp identity(env, opts) do
    pause_snapshot = Keyword.get(opts, :pause_snapshot, GlobalPause.snapshot())

    %{
      pool_key: value(env, "SYMPHONY_POOL_KEY"),
      repository_ref: value(env, "SYMPHONY_REPOSITORY_REF"),
      workspace_root: Keyword.get_lazy(opts, :workspace_root, &workspace_root/0),
      global_pause_file: Map.get(pause_snapshot, :path),
      accepted_source_head: value(env, "SYMPHONY_ACCEPTED_SOURCE_HEAD")
    }
  end

  defp workspace_root do
    Config.local_workspace_root()
  rescue
    _error -> nil
  end

  defp managed_pool?(env) when is_map(env) do
    present?(Map.get(env, "SYMPHONY_POOL_KEY")) or
      present?(Map.get(env, "SYMPHONY_REPOSITORY_REF"))
  end

  defp managed_pool?(_env), do: false

  defp source_head_status(accepted, env) do
    case {present?(accepted), current_source_head(env)} do
      {false, _} ->
        "missing"

      {true, current} ->
        cond do
          not valid_source_head?(accepted) -> "invalid"
          is_nil(current) -> "unverified"
          not valid_source_head?(current) -> "invalid"
          current == accepted -> "verified"
          true -> "stale"
        end
    end
  end

  # The launcher provides the one observed source revision from its executable
  # attestation. No local Git command is used here: an installed escript need
  # not contain a .git directory, so an absent observed value is truthfully
  # reported as unverified rather than guessed.
  defp current_source_head(env) when is_map(env) do
    value(env, "SYMPHONY_CURRENT_SOURCE_HEAD")
  end

  defp current_source_head(_env), do: nil

  defp authority(fence_state, responsibility_state) do
    fence_posture = fence_posture(fence_state)
    delegation_posture = delegation_posture(responsibility_state)

    %{
      fence: @fence_contract,
      delegation: @delegation_contract,
      fence_posture: fence_posture,
      delegation_posture: delegation_posture,
      status:
        if(
          fence_posture in ["active", "quiescent"] and
            delegation_posture in ["active", "quiescent"],
          do: "ready",
          else: "unknown"
        )
    }
  end

  defp fence_posture(fence_state) do
    case ExecutionFence.snapshot(fence_state) do
      %{executions: executions} when is_list(executions) ->
        cond do
          Enum.any?(executions, &execution_unknown?/1) -> "unknown"
          Enum.any?(executions, &(&1.status == :active)) -> "active"
          true -> "quiescent"
        end

      _ ->
        "unknown"
    end
  end

  defp execution_unknown?(execution) do
    execution.ownership in [:unknown, :contradictory] or execution.termination_unconfirmed == true
  rescue
    _error -> true
  end

  defp delegation_posture(responsibility_state) do
    case ResponsibilityGraph.snapshot(responsibility_state) do
      %{enforcement: :enforced, delegations: delegations} when is_list(delegations) ->
        if Enum.any?(delegations, &active_responsible?/1), do: "active", else: "quiescent"

      %{enforcement: :manual} ->
        "manual"

      _ ->
        "unknown"
    end
  end

  defp active_responsible?(delegation),
    do: delegation.role == :responsible and delegation.status == :active

  defp readiness_reasons(managed_pool?, managed_runtime_configured?, missing, source_head_status, authority) do
    required? = managed_pool? or managed_runtime_configured?

    []
    |> maybe_reason(managed_pool? and not managed_runtime_configured?, "managed_runtime_configuration_missing")
    |> add_missing_reasons(missing, required?)
    |> maybe_reason(required? and source_head_status == "stale", "accepted_source_head_stale")
    |> maybe_reason(required? and source_head_status == "missing", "accepted_source_head_missing")
    |> maybe_reason(required? and source_head_status == "unverified", "accepted_source_head_unverified")
    |> maybe_reason(required? and source_head_status == "invalid", "accepted_source_head_invalid")
    |> maybe_reason(required? and authority.status != "ready", "execution_authority_unavailable")
    |> Enum.uniq()
  end

  defp add_missing_reasons(reasons, missing, true),
    do: Enum.reduce(missing, reasons, &maybe_reason(&2, true, "runtime_identity_#{&1}_missing"))

  defp add_missing_reasons(reasons, _missing, false), do: reasons

  defp maybe_reason(reasons, true, reason) when is_binary(reason), do: reasons ++ [reason]
  defp maybe_reason(reasons, _condition, _reason), do: reasons

  defp managed_work_package_state(true, true), do: "configured"
  defp managed_work_package_state(true, false), do: "missing"
  defp managed_work_package_state(false, true), do: "configured"
  defp managed_work_package_state(false, false), do: "disabled"

  defp value(env, key) when is_map(env) do
    case Map.get(env, key) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: nil, else: value

      _ ->
        nil
    end
  end

  defp value(_env, _key), do: nil

  defp blank?(nil), do: true
  defp blank?(value), do: not present?(value)

  defp present?(value), do: is_binary(value) and String.trim(value) != ""

  defp valid_source_head?(value),
    do: is_binary(value) and Regex.match?(~r/\A[0-9a-fA-F]{40}\z/, String.trim(value))
end
