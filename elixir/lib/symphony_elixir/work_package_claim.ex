defmodule SymphonyElixir.WorkPackageClaim do
  @moduledoc """
  Claims one provider work-package reservation using persisted Symphony authority.

  The adapter is intentionally separate from worker dispatch. It binds every
  request to the current execution-fence generation, runtime lease, and active
  responsible delegation, and journals a reservation before attempting claim.
  """

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.WorkPackageClaim.{Dispatch, HostWitness}
  alias SymphonyElixir.WorkPackageClaim.Journal

  @connect_timeout_ms 5_000
  @request_timeout_ms 30_000
  @contract_version "work-package-runtime-attestation.v1"

  @type input :: %{
          base_url: String.t(),
          runner_token: String.t(),
          attestation_key: String.t(),
          runner_id: String.t(),
          managed_project_profile_id: String.t(),
          issue_id: String.t(),
          issue_identifier: String.t() | nil,
          repository_ref: String.t(),
          fence_state: ExecutionFence.state(),
          responsibility_graph: ResponsibilityGraph.state(),
          journal_path: Path.t()
        }

  @type request_fun :: (String.t(), keyword() -> {:ok, Req.Response.t()} | {:error, term()})

  @spec claim(input(), keyword()) :: {:ok, map()} | {:error, term()}
  def claim(input, opts \\ []) when is_map(input) and is_list(opts) do
    request_fun = Keyword.get(opts, :request_fun, &Req.post/2)
    now_fun = Keyword.get(opts, :now_fun, &DateTime.utc_now/0)

    with %DateTime{} = now <- now_fun.(),
         {:ok, authority} <- authority(input, DateTime.to_unix(now, :millisecond)),
         {:ok, journal} <- load_journal(input.journal_path),
         {:ok, reservation, journal} <- ensure_reservation(authority, journal, request_fun),
         {:ok, attestation} <- attestation(authority, reservation, now),
         {:ok, journal} <- Dispatch.submit(journal, journal_key(authority), input, now),
         :ok <- Journal.save(input.journal_path, journal),
         :ok <- HostWitness.record(input, "claim_intent", reservation) do
      submit_claim(input, authority, reservation, attestation, journal, request_fun)
    end
  end

  defp submit_claim(input, authority, reservation, attestation, journal, request_fun) do
    with {:ok, response} <- request_claim(authority, reservation, attestation, request_fun),
         {:ok, body} <- response_data(response),
         {:ok, result} <- validate_claim_result(body, authority, reservation),
         :ok <- HostWitness.record(input, "claim_bound", reservation),
         {:ok, journal} <- Dispatch.confirm(journal, journal_key(authority)),
         :ok <- Journal.save(input.journal_path, journal) do
      {:ok, %{reservation: reservation, attestation: attestation, response: result}}
    else
      {:error, {:provider_status, status} = reason} when status in 400..499 and status not in [408, 425, 429] ->
        with {:ok, journal} <- Dispatch.block(journal, journal_key(authority)),
             :ok <- Journal.save(input.journal_path, journal) do
          {:error, {:claim_blocked, reason}}
        else
          {:error, failure} -> {:error, {:claim_indeterminate, failure}}
        end

      {:error, reason} ->
        {:error, {:claim_indeterminate, reason}}
    end
  end

  @doc "Durably closes the replay window before attempting to start a worker."
  @spec begin_spawn(input()) :: :ok | {:error, term()}
  def begin_spawn(input) do
    with {:ok, authority} <- authority(input, System.system_time(:millisecond)),
         {:ok, journal} <- Journal.load(input.journal_path),
         {:ok, journal} <- Dispatch.begin_spawn(journal, journal_key(authority), input),
         :ok <- Journal.save(input.journal_path, journal),
         {:ok, _authority} <- authority(input, System.system_time(:millisecond)),
         :ok <- HostWitness.record(input, "spawn_intent", journal.reservations[journal_key(authority)]) do
      :ok
    else
      :missing -> {:error, :claim_journal_missing}
      error -> error
    end
  end

  @doc "Durably fences a confirmed claim after the final pause check wins."
  @spec begin_paused_recovery(input()) :: :ok | {:error, term()}
  def begin_paused_recovery(input) do
    with {:ok, authority} <- recovery_authority(input, System.system_time(:millisecond)),
         {:ok, journal} <- Journal.load(input.journal_path),
         key = journal_key(authority),
         reservation when is_map(reservation) <- Map.get(journal.reservations, key),
         :ok <- reservation_matches_authority(reservation, authority),
         {:ok, journal} <- Dispatch.begin_recovery(journal, key, input),
         :ok <- Journal.save(input.journal_path, journal) do
      :ok
    else
      :missing -> {:error, :claim_journal_missing}
      error -> error
    end
  end

  @doc "Builds the provider HMAC canonical JSON in wire-field order."
  @spec canonical_json(map()) :: {:ok, String.t()} | {:error, term()}
  def canonical_json(attestation) when is_map(attestation) do
    fields = [
      {"contractVersion", Map.get(attestation, :contract_version)},
      {"runnerId", Map.get(attestation, :runner_id)},
      {"managedProjectProfileId", Map.get(attestation, :managed_project_profile_id)},
      {"reservationId", Map.get(attestation, :reservation_id)},
      {"reservationNonce", Map.get(attestation, :reservation_nonce)},
      {"issueId", Map.get(attestation, :issue_id)},
      {"generation", Map.get(attestation, :generation)},
      {"sessionId", Map.get(attestation, :session_id)},
      {"processId", Map.get(attestation, :process_id)},
      {"responsibleDelegationId", Map.get(attestation, :responsible_delegation_id)},
      {"executionFenceToken", Map.get(attestation, :execution_fence_token)},
      {"runtimeLeaseId", Map.get(attestation, :runtime_lease_id)},
      {"repositoryRef", Map.get(attestation, :repository_ref)},
      {"scopeKeys", Enum.sort(Map.get(attestation, :scope_keys, []))},
      {"attestedAt", Map.get(attestation, :attested_at)}
    ]

    if Enum.all?(fields, fn {_key, value} -> not is_nil(value) end) and
         is_integer(Map.get(attestation, :generation)) and Map.get(attestation, :generation) > 0 and
         is_list(Map.get(attestation, :scope_keys)) do
      encoded =
        fields
        |> Enum.map(fn {key, value} -> [Jason.encode!(key), ":", Jason.encode!(value)] end)
        |> Enum.intersperse(",")
        |> then(&["{", &1, "}"])
        |> IO.iodata_to_binary()

      {:ok, encoded}
    else
      {:error, :invalid_attestation}
    end
  end

  @spec sign(map(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def sign(attestation, key) when is_map(attestation) and is_binary(key) do
    with true <- String.trim(key) != "",
         {:ok, canonical} <- canonical_json(attestation) do
      {:ok, :crypto.mac(:hmac, :sha256, key, canonical) |> Base.url_encode64(padding: false)}
    else
      false -> {:error, :missing_attestation_key}
      {:error, _reason} = error -> error
    end
  end

  defp authority(input, now_ms) do
    authority(input, now_ms, :admission)
  end

  defp recovery_authority(input, now_ms), do: authority(input, now_ms, :recovery)

  defp authority(input, now_ms, mode) do
    with :ok <- validate_claim_input(input),
         {:ok, base_url} <- validate_base_url(input.base_url),
         :ok <- ExecutionFence.validate(input.fence_state),
         :ok <- ResponsibilityGraph.validate(input.responsibility_graph),
         {:ok, execution} <- current_execution(input.fence_state, input.issue_id),
         {:ok, lease} <- active_worker_lease(execution),
         {:ok, delegation} <-
           ResponsibilityGraph.admission_delegation(
             input.responsibility_graph,
             input.issue_id,
             input[:issue_identifier],
             input.repository_ref
           ),
         :ok <- valid_delegation_lease(delegation, lease, input.repository_ref, now_ms, mode),
         :ok <- repository_matches(execution, input.repository_ref) do
      generation = execution.generation
      session_id = lease.session_id

      {:ok,
       %{
         base_url: String.trim_trailing(base_url, "/"),
         runner_token: input.runner_token,
         attestation_key: input.attestation_key,
         runner_id: input.runner_id,
         managed_project_profile_id: input.managed_project_profile_id,
         issue_id: input.issue_id,
         repository_ref: input.repository_ref,
         generation: generation,
         session_id: session_id,
         process_id: lease.process_id,
         responsible_delegation_id: delegation.id,
         expected_projection_id: managed_projection(input, delegation),
         execution_fence_token: "#{input.issue_id}:#{generation}",
         runtime_lease_id: session_id,
         journal_path: input.journal_path
       }}
    end
  end

  defp validate_claim_input(input) do
    required = [
      :base_url,
      :runner_token,
      :attestation_key,
      :runner_id,
      :managed_project_profile_id,
      :issue_id,
      :repository_ref,
      :journal_path
    ]

    if Enum.all?(required, &present_string?(Map.get(input, &1))) and
         is_map(Map.get(input, :fence_state)) and is_map(Map.get(input, :responsibility_graph)),
       do: :ok,
       else: {:error, :invalid_claim_input}
  end

  defp managed_projection(%{managed_delegations: %{}}, delegation), do: delegation.scope.work_package_id
  defp managed_projection(_input, _delegation), do: nil

  defp projection_matches?(projection_id, authority) do
    case authority.expected_projection_id do
      nil -> true
      expected -> projection_id == expected
    end
  end

  defp repository_matches(%{repository: repository}, repository), do: :ok
  defp repository_matches(_execution, _repository), do: {:error, :repository_mismatch}

  defp current_execution(%{executions: executions}, issue_id) do
    case Map.get(executions, issue_id) do
      %{status: :active, ownership: :reconciled} = execution -> {:ok, execution}
      %{status: :active} -> {:error, :execution_not_reconciled}
      nil -> {:error, :execution_missing}
      _ -> {:error, :execution_not_active}
    end
  end

  defp current_execution(_state, _issue_id), do: {:error, :execution_missing}

  defp active_worker_lease(%{leases: leases, generation: generation}) do
    active_leases =
      leases
      |> Map.values()
      |> Enum.filter(&(&1.role == :worker and &1.status == :active and &1.generation == generation))

    case active_leases do
      [lease] -> {:ok, lease}
      [] -> {:error, :runtime_lease_missing}
      _ -> {:error, :runtime_lease_ambiguous}
    end
  end

  defp valid_delegation_lease(
         %{id: id, runtime_lease: runtime_lease} = delegation,
         lease,
         repository_ref,
         now_ms,
         mode
       )
       when is_binary(id) and mode in [:admission, :recovery] do
    expires_at_ms = Map.get(delegation, :expires_at_ms)
    expiry_valid = is_integer(expires_at_ms) and (mode == :recovery or expires_at_ms > now_ms)

    if is_map(runtime_lease) and
         Map.take(runtime_lease, [:issue_id, :repository, :generation, :session_id, :process_id]) ==
           Map.take(lease, [:issue_id, :repository, :generation, :session_id, :process_id]) and
         runtime_lease.repository == repository_ref and
         expiry_valid do
      :ok
    else
      {:error, :runtime_lease_mismatch}
    end
  end

  defp valid_delegation_lease(_delegation, _lease, _repository_ref, _now_ms, _mode),
    do: {:error, :runtime_lease_missing}

  defp load_journal(path) do
    case Journal.load(path) do
      :missing -> {:ok, Journal.new()}
      {:ok, state} -> {:ok, state}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_reservation(authority, journal, request_fun) do
    key = journal_key(authority)
    legacy_key = Journal.reservation_key(authority.issue_id, authority.managed_project_profile_id, authority.repository_ref)

    case Map.get(journal.reservations, key) do
      nil ->
        existing_legacy = if(key == legacy_key, do: nil, else: Map.get(journal.reservations, legacy_key))

        with {:ok, reservation} <- request_reservation_or_replay(authority, existing_legacy, request_fun),
             reservation =
               Map.merge(
                 reservation,
                 Map.take(authority, [
                   :runner_id,
                   :generation,
                   :session_id,
                   :process_id,
                   :responsible_delegation_id,
                   :execution_fence_token,
                   :runtime_lease_id
                 ])
               ),
             :ok <- reservation_matches_authority(reservation, authority),
             {:ok, next_journal} <- Journal.put(journal, key, reservation) do
          {:ok, reservation, next_journal}
        end

      reservation ->
        with :ok <- reservation_matches_authority(reservation, authority),
             :ok <- replay_marker_present(reservation) do
          {:ok, reservation, journal}
        end
    end
  end

  defp request_reservation_or_replay(authority, %{generation: generation} = reservation, _request_fun)
       when generation == authority.generation do
    with :ok <- replay_marker_present(reservation), do: {:ok, reservation}
  end

  defp request_reservation_or_replay(authority, _legacy_reservation, request_fun),
    do: request_reservation(authority, request_fun)

  defp replay_marker_present(%{dispatch: dispatch}) when is_map(dispatch), do: :ok
  defp replay_marker_present(_reservation), do: {:error, :legacy_claim_requires_reconciliation}

  defp request_reservation(authority, request_fun) do
    url = authority.base_url <> "/runner/v1/work-packages/reservations/by-issue"

    payload = %{
      issueId: authority.issue_id,
      managedProjectProfileId: authority.managed_project_profile_id,
      repositoryRef: authority.repository_ref
    }

    with {:ok, response} <- request(request_fun, url, authority.runner_token, payload, :reservation_lookup),
         {:ok, data} <- response_data(response) do
      parse_reservation(data, authority)
    end
  end

  defp parse_reservation(data, authority) when is_map(data) do
    with {:ok, projection_id} <- response_string(data, "projectionId"),
         {:ok, reservation_id} <- response_string(data, "reservationId"),
         {:ok, nonce} <- response_string(data, "reservationNonce"),
         {:ok, workspace_id} <- response_string(data, "workspaceId"),
         {:ok, company_id} <- response_string(data, "companyId"),
         {:ok, issue_id} <- response_string(data, "issueId"),
         {:ok, profile_id} <- response_string(data, "managedProjectProfileId"),
         {:ok, repository_ref} <- response_string(data, "repositoryRef"),
         {:ok, scope_keys} <- response_scope_keys(data, "scopeKeys"),
         true <-
           issue_id == authority.issue_id and profile_id == authority.managed_project_profile_id and
             repository_ref == authority.repository_ref and projection_matches?(projection_id, authority) do
      {:ok,
       %{
         issue_id: issue_id,
         managed_project_profile_id: authority.managed_project_profile_id,
         repository_ref: repository_ref,
         projection_id: projection_id,
         reservation_id: reservation_id,
         workspace_id: workspace_id,
         company_id: company_id,
         reservation_nonce: nonce,
         scope_keys: scope_keys
       }}
    else
      false -> {:error, :reservation_scope_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp parse_reservation(_data, _authority), do: {:error, :invalid_reservation_response}

  defp reservation_matches_authority(reservation, authority) do
    expected =
      Map.take(authority, [
        :issue_id,
        :managed_project_profile_id,
        :repository_ref,
        :runner_id,
        :generation,
        :session_id,
        :process_id,
        :responsible_delegation_id,
        :execution_fence_token,
        :runtime_lease_id
      ])

    actual = Map.take(reservation, Map.keys(expected))

    if actual == expected and projection_matches?(reservation.projection_id, authority) and
         present_string?(reservation.reservation_nonce) and
         is_list(reservation.scope_keys),
       do: :ok,
       else: {:error, :reservation_authority_mismatch}
  end

  defp attestation(authority, reservation, now) do
    with %DateTime{} <- now,
         unsigned = %{
           contract_version: @contract_version,
           runner_id: authority.runner_id,
           managed_project_profile_id: authority.managed_project_profile_id,
           reservation_id: reservation.reservation_id,
           reservation_nonce: reservation.reservation_nonce,
           issue_id: authority.issue_id,
           generation: authority.generation,
           session_id: authority.session_id,
           process_id: authority.process_id,
           responsible_delegation_id: authority.responsible_delegation_id,
           execution_fence_token: authority.execution_fence_token,
           runtime_lease_id: authority.runtime_lease_id,
           repository_ref: authority.repository_ref,
           scope_keys: reservation.scope_keys,
           attested_at: DateTime.to_iso8601(DateTime.truncate(now, :millisecond))
         },
         {:ok, signature} <- sign(unsigned, authority.attestation_key) do
      {:ok, Map.put(unsigned, :signature, signature)}
    else
      _ -> {:error, :invalid_clock}
    end
  end

  defp request_claim(authority, reservation, attestation, request_fun) do
    url = authority.base_url <> "/runner/v1/work-packages/" <> reservation.projection_id <> "/claim"
    request(request_fun, url, authority.runner_token, %{attestation: wire_attestation(attestation)})
  end

  defp wire_attestation(attestation) do
    Map.new(attestation, fn {key, value} ->
      {camel_case(key), value}
    end)
  end

  defp camel_case(:contract_version), do: "contractVersion"
  defp camel_case(:runner_id), do: "runnerId"
  defp camel_case(:managed_project_profile_id), do: "managedProjectProfileId"
  defp camel_case(:reservation_id), do: "reservationId"
  defp camel_case(:reservation_nonce), do: "reservationNonce"
  defp camel_case(:issue_id), do: "issueId"
  defp camel_case(:generation), do: "generation"
  defp camel_case(:session_id), do: "sessionId"
  defp camel_case(:process_id), do: "processId"
  defp camel_case(:responsible_delegation_id), do: "responsibleDelegationId"
  defp camel_case(:execution_fence_token), do: "executionFenceToken"
  defp camel_case(:runtime_lease_id), do: "runtimeLeaseId"
  defp camel_case(:repository_ref), do: "repositoryRef"
  defp camel_case(:scope_keys), do: "scopeKeys"
  defp camel_case(:attested_at), do: "attestedAt"
  defp camel_case(:signature), do: "signature"

  defp request(request_fun, url, token, payload, stage \\ :claim) do
    options = [
      headers: [{"authorization", "Bearer #{token}"}, {"content-type", "application/json"}],
      json: payload,
      connect_options: [timeout: @connect_timeout_ms],
      receive_timeout: @request_timeout_ms,
      retry: false
    ]

    case request_fun.(url, options) do
      {:ok, %Req.Response{status: status} = response} when status in 200..299 ->
        {:ok, response}

      {:ok, %Req.Response{status: 409, body: %{"error" => %{"code" => "work_package_reservation_not_reissuable"}}}}
      when stage == :reservation_lookup ->
        {:error, :reservation_not_ready}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:provider_status, status}}

      {:error, reason} ->
        {:error, {:provider_request, reason}}

      _ ->
        {:error, :invalid_provider_response}
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

  defp validate_claim_result(
         %{
           "projectionId" => projection_id,
           "projectionState" => "active",
           "mutationState" => "applied",
           "claimEvidence" => evidence
         },
         authority,
         reservation
       )
       when is_map(evidence) do
    expected = %{
      "responsibleDelegationId" => authority.responsible_delegation_id,
      "executionFenceToken" => authority.execution_fence_token,
      "runtimeLeaseId" => authority.runtime_lease_id
    }

    if projection_id == reservation.projection_id and Map.take(evidence, Map.keys(expected)) == expected do
      {:ok,
       %{
         projection_id: projection_id,
         projection_state: "active",
         mutation_state: "applied",
         claim_evidence: expected
       }}
    else
      {:error, :claim_result_mismatch}
    end
  end

  defp validate_claim_result(_body, _authority, _reservation), do: {:error, :invalid_claim_result}

  defp response_string(data, key) do
    case Map.get(data, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_reservation_field, key}}
    end
  end

  defp response_scope_keys(data, key) do
    case Map.get(data, key) do
      values when is_list(values) and values != [] ->
        if Enum.all?(values, &present_string?/1),
          do: {:ok, Enum.uniq(values) |> Enum.sort()},
          else: {:error, :invalid_scope_keys}

      _ ->
        {:error, :invalid_scope_keys}
    end
  end

  defp journal_key(authority) do
    Journal.reservation_key(
      authority.issue_id,
      authority.managed_project_profile_id,
      authority.repository_ref,
      authority.generation
    )
  end

  defp validate_base_url(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} when scheme in ["http", "https"] and is_binary(host) and host != "" -> {:ok, url}
      _ -> {:error, :invalid_base_url}
    end
  end

  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""
end
