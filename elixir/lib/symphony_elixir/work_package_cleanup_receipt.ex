defmodule SymphonyElixir.WorkPackageCleanupReceipt do
  @moduledoc """
  Sends generation-bound cleanup evidence to the provider.

  Cleanup receipts are derived from the durable claim journal and the current
  execution fence. The semantic receipt is journaled before the first request;
  only its freshness timestamp and signature are regenerated when a request is
  retried after a lost response.
  """

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.WorkPackageClaim.Journal

  @request_timeout_ms 5_000
  @contract_version "work-package-cleanup-receipt.v1"
  @receipt_kinds [:termination_confirmed, :repository_cleanup_verified]
  @terminal_outcomes [:completed, :failed, :blocked]

  @type receipt_kind :: :termination_confirmed | :repository_cleanup_verified
  @type terminal_outcome :: :completed | :failed | :blocked
  @type input :: map()
  @type request_fun :: (String.t(), keyword() -> {:ok, Req.Response.t()} | {:error, term()})

  @doc "Posts one immutable cleanup receipt, replaying the journaled semantic tuple safely."
  @spec submit(input(), receipt_kind() | String.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def submit(input, receipt_kind, attrs, opts \\ [])
      when is_map(input) and is_map(attrs) and is_list(opts) do
    request_fun = Keyword.get(opts, :request_fun, &Req.post/2)
    now_fun = Keyword.get(opts, :now_fun, &DateTime.utc_now/0)

    with {:ok, now} <- clock(now_fun),
         :ok <- validate_input(input),
         {:ok, kind} <- normalize_receipt_kind(receipt_kind),
         {:ok, outcome} <- normalize_terminal_outcome(Map.get(attrs, :terminal_outcome, Map.get(attrs, :outcome))),
         :ok <- validate_base_url(input.base_url),
         :ok <- ExecutionFence.validate(input.fence_state),
         {:ok, journal} <- load_journal(input.journal_path),
         {:ok, reservation} <- reservation_for(input, journal),
         {:ok, authority} <- authority(input, reservation, kind),
         {:ok, result} <-
           submit_or_replay(
             input,
             journal,
             reservation,
             authority,
             kind,
             outcome,
             attrs,
             now,
             request_fun
           ) do
      {:ok, result}
    end
  end

  @doc "Convenience wrapper for the first cleanup stage."
  @spec termination_confirmed(input(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def termination_confirmed(input, attrs, opts \\ []) when is_map(input) and is_map(attrs) do
    submit(input, :termination_confirmed, attrs, opts)
  end

  @doc "Convenience wrapper for the repository cleanup stage."
  @spec repository_cleanup_verified(input(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def repository_cleanup_verified(input, attrs, opts \\ []) when is_map(input) and is_map(attrs) do
    submit(input, :repository_cleanup_verified, attrs, opts)
  end

  defp submit_or_replay(
         input,
         journal,
         reservation,
         authority,
         kind,
         outcome,
         attrs,
         now,
         request_fun
       ) do
    key = journal_key(authority)
    receipt_kind = Atom.to_string(kind)

    case Journal.cleanup_receipt(journal, key, receipt_kind) do
      {:ok, semantic} ->
        with :ok <- immutable_request_matches(semantic, kind, outcome, attrs),
             :ok <- valid_stored_semantic(semantic, authority, kind) do
          case stored_acknowledgement(semantic, authority, reservation, kind) do
            {:ok, result} ->
              {:ok, result}

            :missing ->
              submit_unacknowledged(
                input,
                journal,
                reservation,
                authority,
                kind,
                outcome,
                attrs,
                now,
                request_fun
              )

            {:error, reason} ->
              {:error, reason}
          end
        end

      :missing ->
        submit_unacknowledged(
          input,
          journal,
          reservation,
          authority,
          kind,
          outcome,
          attrs,
          now,
          request_fun
        )
    end
  end

  defp submit_unacknowledged(
         input,
         journal,
         reservation,
         authority,
         kind,
         outcome,
         attrs,
         now,
         request_fun
       ) do
    key = journal_key(authority)
    receipt_kind = Atom.to_string(kind)

    with {:ok, semantic, next_journal} <- semantic_receipt(journal, authority, kind, outcome, attrs, now),
         :ok <- persist_semantic_receipt(input.journal_path, journal, next_journal),
         {:ok, receipt} <- attest(semantic, now, input.attestation_key),
         {:ok, response} <-
           request_receipt(
             authority.base_url,
             reservation.projection_id,
             input.runner_token,
             receipt,
             request_fun
           ),
         {:ok, body} <- response_data(response),
         {:ok, result} <- validate_result(body, authority, reservation, receipt),
         {:ok, acknowledged_journal} <- Journal.put_cleanup_receipt_ack(next_journal, key, receipt_kind, result),
         :ok <- Journal.save(input.journal_path, acknowledged_journal) do
      {:ok, result}
    end
  end

  @doc false
  @spec stored_acknowledgement(map(), map(), map(), receipt_kind()) ::
          {:ok, map()} | :missing | {:error, :invalid_cleanup_acknowledgement}
  def stored_acknowledgement(semantic, authority, reservation, kind) do
    case Map.get(semantic, :acknowledgement, Map.get(semantic, "acknowledgement")) do
      nil ->
        :missing

      acknowledgement when is_map(acknowledgement) ->
        with {:ok, normalized} <- normalize_acknowledgement(acknowledgement),
             {:ok, result} <-
               validate_result(acknowledgement_wire(normalized), authority, reservation, semantic),
             true <- result.receipt_kind == Atom.to_string(kind) do
          {:ok, result}
        else
          false -> {:error, :invalid_cleanup_acknowledgement}
          {:error, _reason} -> {:error, :invalid_cleanup_acknowledgement}
        end

      _ ->
        {:error, :invalid_cleanup_acknowledgement}
    end
  end

  @acknowledgement_fields [
    :projection_id,
    :reservation_id,
    :receipt_id,
    :receipt_kind,
    :execution_capacity_state,
    :scope_state,
    :reservation_state,
    :generation,
    :evidence_ref,
    :accepted_head,
    :replayed
  ]

  defp normalize_acknowledgement(acknowledgement) do
    Enum.reduce_while(acknowledgement, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case acknowledgement_key(key) do
        nil -> {:halt, {:error, :invalid_cleanup_acknowledgement}}
        normalized -> {:cont, {:ok, Map.put(acc, normalized, value)}}
      end
    end)
  end

  @doc false
  @spec acknowledgement_key(term()) :: atom() | nil
  def acknowledgement_key(key) when key in @acknowledgement_fields, do: key

  def acknowledgement_key(key) when is_binary(key) do
    case key do
      "projectionId" -> :projection_id
      "reservationId" -> :reservation_id
      "receiptId" -> :receipt_id
      "receiptKind" -> :receipt_kind
      "executionCapacityState" -> :execution_capacity_state
      "scopeState" -> :scope_state
      "reservationState" -> :reservation_state
      "generation" -> :generation
      "evidenceRef" -> :evidence_ref
      "acceptedHead" -> :accepted_head
      "replayed" -> :replayed
      _ -> nil
    end
  end

  def acknowledgement_key(_key), do: nil

  defp acknowledgement_wire(acknowledgement) do
    %{
      "projectionId" => Map.get(acknowledgement, :projection_id),
      "reservationId" => Map.get(acknowledgement, :reservation_id),
      "receiptId" => Map.get(acknowledgement, :receipt_id),
      "receiptKind" => Map.get(acknowledgement, :receipt_kind),
      "executionCapacityState" => Map.get(acknowledgement, :execution_capacity_state),
      "scopeState" => Map.get(acknowledgement, :scope_state),
      "reservationState" => Map.get(acknowledgement, :reservation_state),
      "generation" => Map.get(acknowledgement, :generation),
      "evidenceRef" => Map.get(acknowledgement, :evidence_ref),
      "acceptedHead" => Map.get(acknowledgement, :accepted_head),
      "replayed" => Map.get(acknowledgement, :replayed)
    }
  end

  @doc "Builds the provider HMAC canonical JSON in the frozen wire-field order."
  @spec canonical_json(map()) :: {:ok, String.t()} | {:error, term()}
  def canonical_json(receipt) when is_map(receipt) do
    scope_keys = field(receipt, :scope_keys, [])

    fields = [
      {"contractVersion", field(receipt, :contract_version)},
      {"receiptId", field(receipt, :receipt_id)},
      {"receiptKind", field(receipt, :receipt_kind)},
      {"terminalOutcome", field(receipt, :terminal_outcome)},
      {"observedAt", field(receipt, :observed_at)},
      {"evidenceRef", field(receipt, :evidence_ref)},
      {"acceptedHead", field(receipt, :accepted_head)},
      {"runnerId", field(receipt, :runner_id)},
      {"managedProjectProfileId", field(receipt, :managed_project_profile_id)},
      {"reservationId", field(receipt, :reservation_id)},
      {"reservationNonce", field(receipt, :reservation_nonce)},
      {"issueId", field(receipt, :issue_id)},
      {"generation", field(receipt, :generation)},
      {"sessionId", field(receipt, :session_id)},
      {"processId", field(receipt, :process_id)},
      {"responsibleDelegationId", field(receipt, :responsible_delegation_id)},
      {"executionFenceToken", field(receipt, :execution_fence_token)},
      {"runtimeLeaseId", field(receipt, :runtime_lease_id)},
      {"repositoryRef", field(receipt, :repository_ref)},
      {"scopeKeys", if(is_list(scope_keys), do: Enum.sort(scope_keys), else: scope_keys)},
      {"attestedAt", field(receipt, :attested_at)}
    ]

    if valid_canonical_fields?(fields, receipt) do
      encoded =
        fields
        |> Enum.map(fn {key, value} -> [Jason.encode!(key), ":", Jason.encode!(value)] end)
        |> Enum.intersperse(",")
        |> then(&["{", &1, "}"])
        |> IO.iodata_to_binary()

      {:ok, encoded}
    else
      {:error, :invalid_cleanup_receipt}
    end
  end

  @spec sign(map(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def sign(receipt, key) when is_map(receipt) and is_binary(key) do
    with true <- String.trim(key) != "",
         {:ok, canonical} <- canonical_json(receipt) do
      {:ok, :crypto.mac(:hmac, :sha256, key, canonical) |> Base.url_encode64(padding: false)}
    else
      false -> {:error, :missing_attestation_key}
      {:error, _reason} = error -> error
    end
  end

  defp clock(now_fun) when is_function(now_fun, 0) do
    case now_fun.() do
      %DateTime{} = now -> {:ok, DateTime.truncate(now, :millisecond)}
      _ -> {:error, :invalid_clock}
    end
  rescue
    _error -> {:error, :invalid_clock}
  end

  defp clock(_now_fun), do: {:error, :invalid_clock}

  defp validate_input(input) do
    required_strings = [
      :base_url,
      :runner_token,
      :attestation_key,
      :runner_id,
      :managed_project_profile_id,
      :issue_id,
      :repository_ref,
      :journal_path
    ]

    if Enum.all?(required_strings, &present_string?(Map.get(input, &1))) and
         is_map(input.fence_state) do
      :ok
    else
      {:error, :invalid_cleanup_input}
    end
  end

  defp load_journal(path) do
    case Journal.load(path) do
      :missing -> {:ok, Journal.new()}
      {:ok, state} -> {:ok, state}
      {:error, reason} -> {:error, reason}
    end
  end

  defp reservation_for(input, journal) do
    key = journal_key(input)

    case Map.get(journal.reservations, key) do
      reservation when is_map(reservation) ->
        case Map.get(input, :reservation) do
          provided when is_map(provided) ->
            if reservation_identity(provided) == reservation_identity(reservation),
              do: {:ok, reservation},
              else: {:error, :reservation_authority_mismatch}

          _ ->
            {:ok, reservation}
        end

      nil ->
        {:error, :reservation_missing}
    end
  end

  defp authority(input, reservation, kind) do
    with {:ok, execution} <- current_execution(input.fence_state, input.issue_id),
         {:ok, lease} <- current_lease(execution, reservation),
         :ok <- reservation_matches_input(reservation, input),
         :ok <- cleanup_precondition(execution, lease, reservation, kind) do
      {:ok,
       %{
         base_url: String.trim_trailing(input.base_url, "/"),
         runner_id: reservation.runner_id,
         managed_project_profile_id: reservation.managed_project_profile_id,
         issue_id: reservation.issue_id,
         repository_ref: reservation.repository_ref,
         projection_id: reservation.projection_id,
         reservation_id: reservation.reservation_id,
         reservation_nonce: reservation.reservation_nonce,
         generation: reservation.generation,
         responsible_delegation_id: reservation.responsible_delegation_id,
         execution_fence_token: reservation.execution_fence_token,
         runtime_lease_id: reservation.runtime_lease_id,
         scope_keys: Enum.sort(reservation.scope_keys),
         execution: execution,
         lease: lease
       }}
    end
  end

  defp current_execution(%{executions: executions}, issue_id) when is_map(executions) do
    case Map.get(executions, issue_id) do
      %{issue_id: ^issue_id, generation: generation} = execution when is_integer(generation) ->
        {:ok, execution}

      nil ->
        {:error, :execution_missing}

      _ ->
        {:error, :execution_authority_mismatch}
    end
  end

  defp current_execution(_state, _issue_id), do: {:error, :execution_missing}

  defp current_lease(execution, reservation) do
    case Map.get(execution.leases, reservation.session_id) do
      %{session_id: session_id, process_id: process_id, generation: generation} = lease
      when session_id == reservation.session_id and process_id == reservation.process_id and
             generation == reservation.generation and lease.status in [:active, :released, :expired] ->
        {:ok, lease}

      nil ->
        {:error, :runtime_lease_missing}

      _ ->
        {:error, :runtime_lease_mismatch}
    end
  end

  defp reservation_matches_input(reservation, input) do
    expected = %{
      issue_id: input.issue_id,
      managed_project_profile_id: input.managed_project_profile_id,
      repository_ref: input.repository_ref,
      runner_id: input.runner_id
    }

    if Enum.all?(expected, fn {key, value} -> Map.get(reservation, key) == value end),
      do: :ok,
      else: {:error, :reservation_authority_mismatch}
  end

  defp cleanup_precondition(_execution, lease, _reservation, :termination_confirmed) do
    cond do
      lease.status not in [:released, :expired] ->
        {:error, :termination_not_confirmed}

      lease.termination_required != true ->
        {:error, :termination_not_required}

      not is_integer(Map.get(lease, :termination_confirmed_at_ms)) ->
        {:error, :termination_not_confirmed}

      not present_string?(Map.get(lease, :termination_evidence_ref)) ->
        {:error, :termination_evidence_missing}

      true ->
        :ok
    end
  end

  defp cleanup_precondition(execution, _lease, reservation, :repository_cleanup_verified) do
    cond do
      execution.status != :terminal or execution.cleanup != :cleaned ->
        {:error, :repository_cleanup_not_verified}

      not match?(%{phase: :verified}, Map.get(execution, :cleanup_receipt)) ->
        {:error, :repository_cleanup_not_verified}

      not termination_receipt_recorded?(reservation) ->
        {:error, :termination_receipt_missing}

      true ->
        :ok
    end
  end

  defp termination_receipt_recorded?(reservation) do
    case Map.get(reservation, :cleanup_receipts, %{}) do
      %{"termination_confirmed" => receipt} when is_map(receipt) -> true
      _ -> false
    end
  end

  defp semantic_receipt(journal, authority, kind, outcome, attrs, now) do
    key = journal_key(authority)
    stored = Map.get(journal.reservations[key], :cleanup_receipts, %{}) |> Map.get(Atom.to_string(kind))

    if is_map(stored) do
      with :ok <- immutable_request_matches(stored, kind, outcome, attrs),
           :ok <- valid_stored_semantic(stored, authority, kind) do
        {:ok, stored, journal}
      end
    else
      with {:ok, observed_at} <- observed_at(attrs, now),
           {:ok, evidence_ref} <- evidence_ref(authority, kind, attrs),
           {:ok, accepted_head} <- accepted_head(authority.execution, attrs),
           {:ok, receipt} <- build_semantic(authority, kind, outcome, observed_at, evidence_ref, accepted_head) do
        case Journal.put_cleanup_receipt(journal, key, Atom.to_string(kind), receipt) do
          {:ok, next_journal} -> {:ok, receipt, next_journal}
          {:error, _reason} = error -> error
        end
      end
    end
  end

  defp immutable_request_matches(receipt, kind, outcome, attrs) do
    requested = [
      {:receipt_kind, Atom.to_string(kind)},
      {:terminal_outcome, Atom.to_string(outcome)},
      {:evidence_ref, Map.get(attrs, :evidence_ref)},
      {:accepted_head, Map.get(attrs, :accepted_head)},
      {:observed_at, Map.get(attrs, :observed_at)}
    ]

    Enum.reduce_while(requested, :ok, fn
      {_field, nil}, :ok ->
        {:cont, :ok}

      {field, value}, :ok ->
        if Map.get(receipt, field) == value, do: {:cont, :ok}, else: {:halt, {:error, :cleanup_receipt_conflict}}
    end)
  end

  @doc false
  @spec valid_stored_semantic(map(), map(), receipt_kind()) ::
          :ok | {:error, :cleanup_receipt_authority_mismatch}
  def valid_stored_semantic(receipt, authority, kind) do
    expected = %{
      contract_version: @contract_version,
      receipt_kind: Atom.to_string(kind),
      terminal_outcome: Map.get(receipt, :terminal_outcome),
      runner_id: authority.runner_id,
      managed_project_profile_id: authority.managed_project_profile_id,
      reservation_id: authority.reservation_id,
      reservation_nonce: authority.reservation_nonce,
      issue_id: authority.issue_id,
      execution_fence_token: authority.execution_fence_token,
      runtime_lease_id: authority.runtime_lease_id,
      repository_ref: authority.repository_ref,
      responsible_delegation_id: authority.responsible_delegation_id,
      generation: authority.execution.generation,
      session_id: authority.lease.session_id,
      process_id: authority.lease.process_id,
      scope_keys: authority.scope_keys
    }

    expected_valid? =
      Map.get(receipt, :terminal_outcome) in Enum.map(@terminal_outcomes, &Atom.to_string/1) and
        valid_datetime?(Map.get(receipt, :observed_at)) and
        bounded_string?(Map.get(receipt, :evidence_ref), 256) and
        bounded_string?(Map.get(receipt, :accepted_head), 256) and
        Map.get(receipt, :scope_keys) == authority.scope_keys and
        Map.get(receipt, :receipt_id) == stable_receipt_id(Map.delete(receipt, :receipt_id))

    if expected_valid? and Enum.all?(expected, fn {field_name, value} -> Map.get(receipt, field_name) == value end),
      do: :ok,
      else: {:error, :cleanup_receipt_authority_mismatch}
  end

  defp observed_at(attrs, now) do
    case Map.get(attrs, :observed_at) do
      value when is_binary(value) ->
        if valid_datetime?(value), do: {:ok, value}, else: {:error, :invalid_observed_at}

      nil ->
        {:ok, DateTime.to_iso8601(now)}

      _ ->
        {:error, :invalid_observed_at}
    end
  end

  defp evidence_ref(authority, :termination_confirmed, attrs) do
    expected = Map.get(authority.lease, :termination_evidence_ref)

    case Map.get(attrs, :evidence_ref, expected) do
      value when value == expected and is_binary(value) ->
        if bounded_string?(value, 256), do: {:ok, value}, else: {:error, :invalid_evidence_ref}

      _ ->
        {:error, :termination_evidence_mismatch}
    end
  end

  defp evidence_ref(_authority, _kind, attrs) do
    case Map.get(attrs, :evidence_ref) do
      value when is_binary(value) ->
        if bounded_string?(value, 256), do: {:ok, value}, else: {:error, :invalid_evidence_ref}

      _ ->
        {:error, :invalid_evidence_ref}
    end
  end

  defp accepted_head(execution, attrs) do
    expected = get_in(execution, [:terminal, :accepted_head])

    case Map.get(attrs, :accepted_head, expected) do
      value when is_binary(value) ->
        cond do
          not bounded_string?(value, 256) -> {:error, :invalid_accepted_head}
          is_nil(expected) or expected == value -> {:ok, value}
          true -> {:error, :head_diverged}
        end

      _ ->
        {:error, :invalid_accepted_head}
    end
  end

  defp build_semantic(authority, kind, outcome, observed_at, evidence_ref, accepted_head) do
    base = %{
      contract_version: @contract_version,
      receipt_kind: Atom.to_string(kind),
      terminal_outcome: Atom.to_string(outcome),
      observed_at: observed_at,
      evidence_ref: evidence_ref,
      accepted_head: accepted_head,
      runner_id: authority.runner_id,
      managed_project_profile_id: authority.managed_project_profile_id,
      reservation_id: authority.reservation_id,
      reservation_nonce: authority.reservation_nonce,
      issue_id: authority.issue_id,
      generation: authority.execution.generation,
      session_id: authority.lease.session_id,
      process_id: authority.lease.process_id,
      responsible_delegation_id: authority.responsible_delegation_id,
      execution_fence_token: authority.execution_fence_token,
      runtime_lease_id: authority.runtime_lease_id,
      repository_ref: authority.repository_ref,
      scope_keys: authority.scope_keys
    }

    receipt_id = stable_receipt_id(base)
    {:ok, Map.put(base, :receipt_id, receipt_id)}
  end

  defp stable_receipt_id(base) do
    seed =
      base
      |> Map.delete(:receipt_id)
      |> Map.delete(:attested_at)
      |> Map.delete(:signature)
      |> Map.delete(:acknowledgement)
      |> Map.delete("acknowledgement")
      |> canonical_seed()

    "cleanup-" <> (:crypto.hash(:sha256, seed) |> Base.encode16(case: :lower))
  end

  defp canonical_seed(base) do
    base
    |> Enum.sort_by(fn {key, _value} -> Atom.to_string(key) end)
    |> Enum.map(fn {key, value} ->
      [Jason.encode!(Atom.to_string(key)), ":", Jason.encode!(value)]
    end)
    |> Enum.intersperse(",")
    |> then(&["{", &1, "}"])
    |> IO.iodata_to_binary()
  end

  defp attest(semantic, now, key) do
    unsigned = Map.put(semantic, :attested_at, DateTime.to_iso8601(now))

    with {:ok, signature} <- sign(unsigned, key) do
      {:ok, Map.put(unsigned, :signature, signature)}
    end
  end

  defp persist_semantic_receipt(_path, old_journal, old_journal), do: :ok

  defp persist_semantic_receipt(path, _old_journal, journal) do
    Journal.save(path, journal)
  end

  defp request_receipt(base_url, projection_id, runner_token, receipt, request_fun) do
    url = base_url <> "/runner/v1/work-packages/" <> projection_id <> "/cleanup-receipt"

    request(
      request_fun,
      url,
      runner_token,
      wire_receipt(receipt)
    )
  end

  defp wire_receipt(receipt) do
    Map.new(receipt, fn {key, value} -> {wire_key(key), value} end)
  end

  defp wire_key(:contract_version), do: "contractVersion"
  defp wire_key(:receipt_id), do: "receiptId"
  defp wire_key(:receipt_kind), do: "receiptKind"
  defp wire_key(:terminal_outcome), do: "terminalOutcome"
  defp wire_key(:observed_at), do: "observedAt"
  defp wire_key(:evidence_ref), do: "evidenceRef"
  defp wire_key(:accepted_head), do: "acceptedHead"
  defp wire_key(:runner_id), do: "runnerId"
  defp wire_key(:managed_project_profile_id), do: "managedProjectProfileId"
  defp wire_key(:reservation_id), do: "reservationId"
  defp wire_key(:reservation_nonce), do: "reservationNonce"
  defp wire_key(:issue_id), do: "issueId"
  defp wire_key(:generation), do: "generation"
  defp wire_key(:session_id), do: "sessionId"
  defp wire_key(:process_id), do: "processId"
  defp wire_key(:responsible_delegation_id), do: "responsibleDelegationId"
  defp wire_key(:execution_fence_token), do: "executionFenceToken"
  defp wire_key(:runtime_lease_id), do: "runtimeLeaseId"
  defp wire_key(:repository_ref), do: "repositoryRef"
  defp wire_key(:scope_keys), do: "scopeKeys"
  defp wire_key(:attested_at), do: "attestedAt"
  defp wire_key(:signature), do: "signature"
  defp wire_key(key), do: Atom.to_string(key)

  defp request(request_fun, url, token, payload) do
    options = [
      headers: [{"authorization", "Bearer #{token}"}, {"content-type", "application/json"}],
      json: payload,
      connect_options: [timeout: @request_timeout_ms],
      receive_timeout: @request_timeout_ms,
      retry: false
    ]

    case request_fun.(url, options) do
      {:ok, %Req.Response{status: status} = response} when status in 200..299 -> {:ok, response}
      {:ok, %Req.Response{status: status}} -> {:error, {:provider_status, status}}
      {:error, reason} -> {:error, {:provider_request, reason}}
      _ -> {:error, :invalid_provider_response}
    end
  rescue
    _error -> {:error, :provider_request_failed}
  end

  defp response_data(%Req.Response{body: %{"data" => data}}) when is_map(data), do: {:ok, data}
  defp response_data(%Req.Response{body: body}) when is_map(body), do: {:ok, body}

  defp response_data(%Req.Response{body: body}) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, %{"data" => data}} when is_map(data) -> {:ok, data}
      {:ok, data} when is_map(data) -> {:ok, data}
      _ -> {:error, :invalid_provider_payload}
    end
  end

  defp response_data(_response), do: {:error, :invalid_provider_payload}

  defp validate_result(data, _authority, reservation, receipt) when is_map(data) do
    expected = %{
      "projectionId" => reservation.projection_id,
      "reservationId" => reservation.reservation_id,
      "receiptId" => receipt.receipt_id,
      "receiptKind" => receipt.receipt_kind,
      "generation" => receipt.generation,
      "evidenceRef" => receipt.evidence_ref,
      "acceptedHead" => receipt.accepted_head
    }

    states_ok? =
      data["executionCapacityState"] == "released" and
        case receipt.receipt_kind do
          "termination_confirmed" ->
            {data["scopeState"], data["reservationState"]} in [{"held", "claimed"}, {"released", "released"}]

          "repository_cleanup_verified" ->
            {data["scopeState"], data["reservationState"]} == {"released", "released"}
        end

    values_ok? = Enum.all?(expected, fn {key, value} -> Map.get(data, key) == value end)

    if values_ok? and states_ok? and data["reservationState"] in ["claimed", "released"] and
         is_boolean(data["replayed"]) do
      {:ok,
       %{
         projection_id: data["projectionId"],
         reservation_id: data["reservationId"],
         receipt_id: data["receiptId"],
         receipt_kind: data["receiptKind"],
         execution_capacity_state: data["executionCapacityState"],
         scope_state: data["scopeState"],
         reservation_state: data["reservationState"],
         generation: data["generation"],
         evidence_ref: data["evidenceRef"],
         accepted_head: data["acceptedHead"],
         replayed: data["replayed"]
       }}
    else
      {:error, :cleanup_result_mismatch}
    end
  end

  defp validate_result(_data, _authority, _reservation, _receipt),
    do: {:error, :invalid_provider_payload}

  defp normalize_receipt_kind(kind) when kind in @receipt_kinds, do: {:ok, kind}

  defp normalize_receipt_kind(kind) when is_binary(kind) do
    case Enum.find(@receipt_kinds, &(Atom.to_string(&1) == kind)) do
      nil -> {:error, :invalid_receipt_kind}
      value -> {:ok, value}
    end
  end

  defp normalize_receipt_kind(_kind), do: {:error, :invalid_receipt_kind}

  defp normalize_terminal_outcome(outcome) when outcome in @terminal_outcomes, do: {:ok, outcome}

  defp normalize_terminal_outcome(outcome) when is_binary(outcome) do
    case Enum.find(@terminal_outcomes, &(Atom.to_string(&1) == outcome)) do
      nil -> {:error, :invalid_terminal_outcome}
      value -> {:ok, value}
    end
  end

  defp normalize_terminal_outcome(nil), do: {:error, :invalid_terminal_outcome}
  defp normalize_terminal_outcome(_outcome), do: {:error, :invalid_terminal_outcome}

  defp journal_key(input_or_authority) do
    issue_id = Map.get(input_or_authority, :issue_id)
    profile_id = Map.get(input_or_authority, :managed_project_profile_id)
    repository_ref = Map.get(input_or_authority, :repository_ref)
    generation = Map.get(input_or_authority, :generation) || get_in(input_or_authority, [:fence_state, :executions, issue_id, :generation])

    if is_integer(generation) and generation > 0 do
      Journal.reservation_key(issue_id, profile_id, repository_ref, generation)
    else
      Journal.reservation_key(issue_id, profile_id, repository_ref)
    end
  end

  defp reservation_identity(reservation) do
    Map.take(reservation, [
      :issue_id,
      :managed_project_profile_id,
      :repository_ref,
      :projection_id,
      :reservation_id,
      :reservation_nonce,
      :runner_id,
      :generation,
      :session_id,
      :process_id,
      :responsible_delegation_id,
      :execution_fence_token,
      :runtime_lease_id,
      :scope_keys
    ])
  end

  defp validate_base_url(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} when scheme in ["http", "https"] and is_binary(host) and host != "" ->
        :ok

      _ ->
        {:error, :invalid_base_url}
    end
  end

  defp valid_canonical_fields?(fields, receipt) do
    Enum.all?(fields, fn {key, value} ->
      if key == "scopeKeys" do
        is_list(value) and value != [] and Enum.all?(value, &bounded_string?(&1, 512))
      else
        not is_nil(value) and
          if key == "generation", do: is_integer(value) and value > 0, else: bounded_string?(value, 512)
      end
    end) and
      field(receipt, :contract_version) == @contract_version and
      field(receipt, :receipt_kind) in Enum.map(@receipt_kinds, &Atom.to_string/1) and
      field(receipt, :terminal_outcome) in Enum.map(@terminal_outcomes, &Atom.to_string/1)
  end

  defp field(map, key, default \\ nil) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, wire_key(key), default)
    end
  end

  defp valid_datetime?(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, _date_time, _offset} -> true
      _ -> false
    end
  end

  defp valid_datetime?(_value), do: false

  defp bounded_string?(value, max) do
    present_string?(value) and byte_size(value) <= max
  end

  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""
end
