defmodule SymphonyElixir.RKE2Job.AbortPrepareCaller do
  @moduledoc """
  Coordinates the trusted host's durable root-witness and Dahlia prepare steps
  before passing the saved observation and journal guard to the RKE2 adapter.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{AbortPrepareJournal, JournalPrepareAckGuard, ManagedExecutorAdapter}
  alias SymphonyElixir.WorkPackageClaim.HostWitness

  @contract_version "work-package-pre-execution-abort-prepare.v1"
  @test_environment Mix.env() == :test
  @request_timeout_ms 10_000
  @request_fields ~w(
    contractVersion prepareId reservationId slotLeaseId assignmentDigest allocationId jobResourceVersion
    observedAt suspended executionStarted activePods succeededPods failedPods podListResourceVersion
    ownedPodsAbsent slotClaimPodsAbsent
  )
  @uuid ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/

  @type result :: {:ok, map()} | {:held, term()} | {:error, term()}

  @doc "Persists and submits one exact abort-prepare request after the root intent is durable."
  @spec prepare(map(), map(), String.t(), map()) :: result()
  def prepare(allocation, assignment, idempotency_key, context)
      when is_map(allocation) and is_map(assignment) and is_binary(idempotency_key) and is_map(context) do
    with :ok <- validate_inputs(allocation, assignment, idempotency_key, context),
         {:ok, claim} <- claim(context),
         :ok <- validate_claim_assignment(claim, assignment, context),
         {:ok, stored_or_missing} <- load_or_missing(context, claim),
         {:ok, record} <- ensure_request(stored_or_missing, allocation, assignment, idempotency_key, context, claim),
         :ok <- bind_record(record, allocation, assignment, context, claim),
         :ok <- ensure_root_intent(record, context, claim),
         {:ok, acknowledgement} <- provider_ack(record, context, claim) do
      {:ok,
       %{
         observation: record.observation,
         prepare_ack: acknowledgement,
         prepare_ack_guard: JournalPrepareAckGuard,
         prepare_ack_guard_context: %{journal_root: context.journal_root, claim: claim}
       }}
    else
      {:held, _reason} = held -> held
      {:error, _reason} = error -> error
    end
  rescue
    _ -> {:held, :abort_prepare_caller_failed_closed}
  catch
    _kind, _reason -> {:held, :abort_prepare_caller_failed_closed}
  end

  def prepare(_allocation, _assignment, _idempotency_key, _context),
    do: {:error, :invalid_abort_prepare_caller_input}

  @doc "Feeds the saved observation and concrete journal guard into adapter confirmation."
  @spec confirm(map(), map(), String.t(), map(), map()) :: :ok | {:held, term()} | {:error, term()}
  def confirm(allocation, assignment, idempotency_key, prepared, caller_context)
      when is_map(prepared) and is_map(caller_context) do
    with :ok <- validate_inputs(allocation, assignment, idempotency_key, caller_context),
         {:ok, claim} <- claim(caller_context),
         :ok <- validate_claim_assignment(claim, assignment, caller_context),
         {:ok, record} <- AbortPrepareJournal.load(caller_context.journal_root, claim),
         true <- record.assignment_digest == assignment.sha256,
         true <- record.allocation_id == allocation.id,
         :ok <- revalidate_root_intent(record, caller_context, claim),
         true <- is_map(Map.get(prepared, :prepare_ack)) do
      context =
        caller_context.adapter_context
        |> Map.put(:prepare_ack_guard, JournalPrepareAckGuard)
        |> Map.put(:prepare_ack_guard_context, %{journal_root: caller_context.journal_root, claim: claim})
        |> Map.put(:confirmed_delete_journal, %{journal_root: caller_context.journal_root, claim: claim, record: record})

      ManagedExecutorAdapter.confirm_abort_unstarted_owned(
        allocation,
        assignment,
        idempotency_key,
        record.observation,
        prepared.prepare_ack,
        context
      )
    else
      {:held, _reason} = held -> held
      :missing -> {:held, :abort_prepare_journal_missing}
      _ -> {:held, :prepare_ack_observation_mismatch}
    end
  rescue
    _ -> {:held, :abort_prepare_root_intent_unverified}
  end

  def confirm(_allocation, _assignment, _idempotency_key, _prepared, _adapter_context),
    do: {:held, :abort_prepare_ack_guard_missing}

  @doc false
  @spec production_context_allowed?(map()) :: boolean()
  def production_context_allowed?(context) when is_map(context) do
    trusted_root_configuration?(context) and valid_host_roots?(context) and
      no_production_test_seams?(context)
  rescue
    _ -> false
  end

  def production_context_allowed?(_context), do: false

  defp validate_inputs(allocation, assignment, key, context) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         true <- allocation_shape?(allocation),
         true <- key == assignment.sha256 <> ":abort_unstarted",
         true <- is_map(Map.get(context, :adapter_context)),
         true <- is_map(Map.get(context, :witness_input)),
         true <- is_map(Map.get(context, :reservation)),
         true <- is_binary(Map.get(context, :journal_root)) and Path.type(context.journal_root) == :absolute,
         true <- is_binary(Map.get(context, :workspace_root)) and Path.type(context.workspace_root) == :absolute,
         false <- inside_workspace?(context.journal_root, context.workspace_root),
         true <- trusted_roots?(context),
         true <- valid_test_injections?(context),
         :ok <- provider_configuration(Map.get(context, :provider_context)) do
      :ok
    else
      _ -> {:error, :invalid_abort_prepare_caller_input}
    end
  end

  defp allocation_shape?(%{id: id, status: :ready}), do: is_binary(id) and byte_size(id) in 1..1024
  defp allocation_shape?(_allocation), do: false

  defp inside_workspace?(journal_root, workspace_root) do
    root = Path.expand(journal_root)
    workspace = Path.expand(workspace_root)

    {root, workspace, separator} =
      if match?({:win32, _}, :os.type()),
        do: {String.downcase(root), String.downcase(workspace), "\\"},
        else: {root, workspace, "/"}

    root == workspace or String.starts_with?(root, String.trim_trailing(workspace, separator) <> separator)
  end

  defp provider_configuration(%{base_url: base, runner_token: token})
       when is_binary(base) and is_binary(token) and byte_size(token) > 0 do
    if valid_provider_base_url?(base),
      do: :ok,
      else: {:error, :invalid_abort_prepare_provider_configuration}
  end

  defp provider_configuration(_), do: {:error, :invalid_abort_prepare_provider_configuration}

  defp trusted_roots?(context) do
    @test_environment or production_context_allowed?(context)
  end

  defp trusted_root_configuration?(context) do
    context.journal_root == Application.get_env(:symphony_elixir, :abort_prepare_journal_root) and
      context.workspace_root == Application.get_env(:symphony_elixir, :abort_prepare_workspace_root)
  end

  defp valid_host_roots?(context) do
    is_binary(context.journal_root) and Path.type(context.journal_root) == :absolute and
      is_binary(context.workspace_root) and Path.type(context.workspace_root) == :absolute and
      not inside_workspace?(context.journal_root, context.workspace_root)
  end

  defp no_production_test_seams?(context) do
    not Map.has_key?(context, :post_fun) and not Map.has_key?(context.witness_input, :host_witness_fun) and
      Map.get(context, :host_witness, HostWitness) == HostWitness
  end

  defp valid_test_injections?(context) do
    @test_environment or
      (not Map.has_key?(context, :post_fun) and not Map.has_key?(context.witness_input, :host_witness_fun) and
         Map.get(context, :host_witness, HostWitness) == HostWitness)
  end

  defp valid_provider_base_url?(base) do
    case URI.parse(base) do
      %URI{scheme: "https", host: host, userinfo: nil, query: nil, fragment: nil, path: path}
      when is_binary(host) and host != "" and path in [nil, "", "/"] ->
        true

      _ ->
        false
    end
  end

  defp claim(context) do
    witness = Map.get(context, :host_witness, HostWitness)

    if is_atom(witness) and Code.ensure_loaded?(witness) and function_exported?(witness, :request, 3) do
      case witness.request(context.witness_input, "claim_bound", context.reservation) do
        {:ok, %{"claim" => claim}} ->
          {:ok, Map.put(claim, "pool", context.witness_input.pool_key)}

        _ ->
          {:error, :host_witness_claim_incomplete}
      end
    else
      {:error, :host_witness_unavailable}
    end
  rescue
    _ -> {:error, :host_witness_claim_incomplete}
  end

  defp load_or_missing(context, claim) do
    case AbortPrepareJournal.load(context.journal_root, claim) do
      :missing -> {:ok, :missing}
      {:ok, record} -> {:ok, record}
      {:held, reason} -> {:held, reason}
    end
  end

  defp ensure_request(:missing, allocation, assignment, key, context, claim) do
    with {:ok, observation} <-
           ManagedExecutorAdapter.prepare_abort_unstarted_owned(
             allocation,
             assignment,
             key,
             context.adapter_context
           ),
         :ok <- validate_observation(observation, allocation, assignment, context.reservation) do
      create_request_record(allocation, assignment, observation, context, claim)
    else
      {:held, _reason} = held -> held
      {:error, _reason} = error -> error
    end
  end

  defp ensure_request(record, _allocation, _assignment, _key, _context, _claim), do: {:ok, record}

  defp create_request_record(allocation, assignment, observation, context, claim) do
    prepare_id = Ecto.UUID.generate()
    request = request_fields(prepare_id, allocation, assignment, observation, context.reservation)

    with {:ok, request_bytes} <- ordered_json(request) do
      record = %{
        claim: claim,
        assignment_digest: assignment.sha256,
        allocation_id: allocation.id,
        prepare_id: prepare_id,
        request_sha256: sha256(request_bytes),
        request_bytes: request_bytes,
        observation: observation
      }

      case AbortPrepareJournal.record(context.journal_root, claim, record) do
        {:ok, saved} -> {:ok, saved}
        {:held, reason} -> {:held, reason}
      end
    end
  end

  defp bind_record(record, allocation, assignment, context, claim) do
    expected_reservation = context.reservation.reservation_id

    with true <- record.claim == claim,
         true <- record.assignment_digest == assignment.sha256,
         true <- record.allocation_id == allocation.id,
         true <- claim["reservationId"] == expected_reservation,
         :ok <- validate_claim_assignment(claim, assignment, context) do
      :ok
    else
      _ -> {:held, :abort_prepare_claim_binding_mismatch}
    end
  end

  defp validate_claim_assignment(claim, assignment, context) do
    lease = assignment.lease
    expected_delegation = List.last(assignment.intent_ancestry)
    binding = Map.get(context.adapter_context, :claim_binding, %{})

    expected = %{
      "issueId" => lease.issue_id,
      "repositoryRef" => assignment.repository_ref,
      "runnerId" => assignment.seat,
      "responsibleDelegationId" => expected_delegation,
      "generation" => lease.generation,
      "sessionId" => lease.session_id,
      "processId" => lease.process_id,
      "executionFenceToken" => "#{lease.issue_id}:#{lease.generation}",
      "runtimeLeaseId" => lease.session_id
    }

    expected_binding = %{
      projection_id: claim["projectionId"],
      reservation_id: claim["reservationId"],
      workspace_id: claim["workspaceId"],
      company_id: claim["companyId"],
      issue_id: claim["issueId"],
      runner_id: claim["runnerId"],
      managed_project_profile_id: claim["managedProjectProfileId"],
      repository_ref: claim["repositoryRef"],
      generation: claim["generation"],
      session_id: claim["sessionId"],
      process_id: claim["processId"],
      responsible_delegation_id: claim["responsibleDelegationId"],
      execution_fence_token: claim["executionFenceToken"],
      runtime_lease_id: claim["runtimeLeaseId"],
      scope_keys: Enum.sort(claim["scopeKeys"]),
      nonce_sha256: claim["nonceHash"]
    }

    binding_matches? = Enum.sort(Map.keys(binding)) == Enum.sort(Map.keys(expected_binding)) and binding == expected_binding

    if binding_matches? and claim["pool"] == context.witness_input.pool_key and
         Enum.all?(expected, fn {field, value} -> claim[field] == value end),
       do: :ok,
       else: {:held, :abort_prepare_claim_binding_mismatch}
  rescue
    _ -> {:held, :abort_prepare_claim_binding_mismatch}
  end

  defp ensure_root_intent(record, context, _claim) do
    witness = Map.get(context, :host_witness, HostWitness)
    already_recorded? = AbortPrepareJournal.intent_recorded?(context.journal_root, record.claim, record)

    with {:ok, receipt} <-
           witness.record_abort_prepare_intent_receipt(
             context.witness_input,
             context.reservation,
             record.prepare_id,
             record.request_sha256
           ),
         :ok <- validate_root_receipt(record, context, already_recorded?, receipt),
         :ok <- AbortPrepareJournal.record_intent(context.journal_root, record.claim, record, receipt) do
      :ok
    else
      {:error, _} -> {:held, :abort_prepare_root_intent_unverified}
      {:held, _} = held -> held
      _ -> {:held, :abort_prepare_root_intent_unverified}
    end
  rescue
    _ -> {:held, :abort_prepare_root_intent_unverified}
  end

  defp revalidate_root_intent(record, context, claim) do
    witness = Map.get(context, :host_witness, HostWitness)

    with true <- AbortPrepareJournal.intent_recorded?(context.journal_root, claim, record),
         {:ok, receipt} <-
           witness.record_abort_prepare_intent_receipt(
             context.witness_input,
             context.reservation,
             record.prepare_id,
             record.request_sha256
           ),
         true <- AbortPrepareJournal.intent_receipt_matches?(context.journal_root, claim, record, receipt) do
      :ok
    else
      _ -> {:held, :abort_prepare_root_intent_unverified}
    end
  rescue
    _ -> {:held, :abort_prepare_root_intent_unverified}
  end

  defp validate_root_receipt(record, context, true, receipt) do
    if AbortPrepareJournal.intent_receipt_matches?(context.journal_root, record.claim, record, receipt),
      do: :ok,
      else: {:held, :abort_prepare_root_intent_unverified}
  end

  defp validate_root_receipt(_record, _context, false, receipt) do
    if is_map(receipt) and receipt["version"] == 1 and is_integer(receipt["sequence"]) and receipt["sequence"] > 0 and
         is_binary(receipt["hash"]) and String.match?(receipt["hash"], ~r/\A[a-f0-9]{64}\z/) and
         is_boolean(receipt["replayed"]),
       do: :ok,
       else: {:held, :abort_prepare_root_intent_unverified}
  end

  defp provider_ack(record, context, claim) do
    case load_ack(context.journal_root, claim, record) do
      {:ok, acknowledgement} -> {:ok, acknowledgement}
      :missing -> post_prepare(record, context, claim)
      {:held, reason} -> {:held, reason}
    end
  end

  defp load_ack(root, claim, record) do
    with {:ok, path} <- ack_path(root, claim),
         {:ok, bytes} <- read_ack_file(path),
         {:ok,
          %{
            "schema_version" => 1,
            "prepare_id" => prepare_id,
            "request_sha256" => request_hash,
            "acknowledgement" => acknowledgement
          }} <- Jason.decode(bytes),
         true <- prepare_id == record.prepare_id and request_hash == record.request_sha256,
         :ok <- valid_ack_shape(acknowledgement, record) do
      {:ok, acknowledgement}
    else
      {:error, :enoent} -> :missing
      _ -> {:held, :abort_prepare_ack_journal_invalid}
    end
  rescue
    _ -> {:held, :abort_prepare_ack_journal_invalid}
  end

  defp post_prepare(record, context, claim) do
    provider = context.provider_context

    url =
      String.trim_trailing(provider.base_url, "/") <>
        "/runner/v1/work-packages/" <>
        URI.encode(context.reservation.projection_id, &URI.char_unreserved?/1) <> "/pre-execution-abort-prepare"

    post_fun = Map.get(context, :post_fun, &Req.post/2)

    request_options = [
      headers: [{"authorization", "Bearer " <> provider.runner_token}, {"content-type", "application/json"}],
      body: record.request_bytes,
      connect_options: [timeout: @request_timeout_ms],
      receive_timeout: @request_timeout_ms,
      retry: false,
      redirect: false
    ]

    with {:ok, response} <- safe_post(post_fun, url, request_options),
         {:ok, data} <- response_data(response),
         :ok <- provider_ack_fields(data),
         acknowledgement = %{
           "prepareId" => data["prepareId"],
           "projectionId" => data["projectionId"],
           "reservationId" => data["reservationId"],
           "preparedAt" => data["preparedAt"],
           "replayed" => data["replayed"],
           "prepareRequestSHA256" => record.request_sha256
         },
         :ok <- valid_ack_shape(acknowledgement, record),
         :ok <- AbortPrepareJournal.record_ack(context.journal_root, claim, record, acknowledgement) do
      {:ok, acknowledgement}
    else
      {:held, reason} -> {:held, reason}
      _ -> {:held, :abort_prepare_provider_outcome_uncertain}
    end
  end

  defp safe_post(fun, url, options) when is_function(fun, 2) do
    case fun.(url, options) do
      {:ok, %Req.Response{status: status} = response} when status in 200..299 -> {:ok, response}
      {:ok, %Req.Response{status: status}} -> {:error, {:provider_status, status}}
      {:error, _reason} -> {:error, :provider_outcome_uncertain}
      _ -> {:error, :invalid_provider_response}
    end
  rescue
    _ -> {:error, :provider_outcome_uncertain}
  catch
    _kind, _reason -> {:error, :provider_outcome_uncertain}
  end

  defp safe_post(_fun, _url, _options), do: {:error, :provider_outcome_uncertain}

  defp response_data(%Req.Response{body: %{"data" => data}}) when is_map(data), do: {:ok, data}
  defp response_data(%Req.Response{body: data}) when is_map(data), do: {:ok, data}

  defp response_data(%Req.Response{body: body}) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, %{"data" => data}} when is_map(data) -> {:ok, data}
      {:ok, data} when is_map(data) -> {:ok, data}
      _ -> {:error, :invalid_provider_payload}
    end
  end

  defp response_data(_response), do: {:error, :invalid_provider_payload}

  defp valid_ack_shape(ack, record) when is_map(ack) do
    keys = ~w(prepareId projectionId reservationId preparedAt replayed prepareRequestSHA256)

    with true <- Enum.sort(Map.keys(ack)) == Enum.sort(keys),
         true <- ack["prepareId"] == record.prepare_id,
         true <- ack["projectionId"] == record.claim["projectionId"],
         true <- ack["reservationId"] == record.claim["reservationId"],
         true <- is_boolean(ack["replayed"]),
         true <- ack["prepareRequestSHA256"] == record.request_sha256,
         true <- timestamp?(ack["preparedAt"]) do
      :ok
    else
      _ -> {:error, :invalid_abort_prepare_provider_acknowledgement}
    end
  end

  defp valid_ack_shape(_ack, _record), do: {:error, :invalid_abort_prepare_provider_acknowledgement}

  defp provider_ack_fields(data) when is_map(data) do
    if Enum.sort(Map.keys(data)) == Enum.sort(~w(prepareId projectionId reservationId preparedAt replayed)) and
         bounded_string?(data["prepareId"], 36) and bounded_string?(data["projectionId"], 256) and
         bounded_string?(data["reservationId"], 256) and bounded_string?(data["preparedAt"], 64) and
         is_boolean(data["replayed"]),
       do: :ok,
       else: {:error, :invalid_abort_prepare_provider_acknowledgement}
  end

  defp validate_observation(observation, allocation, assignment, reservation) do
    with true <- is_map(observation),
         true <- observation["allocationId"] == allocation.id,
         true <- observation["compiledIdentity"]["assignmentSHA256"] == assignment.sha256,
         true <- observation["slotBinding"]["assignmentSHA256"] == assignment.sha256,
         true <- observation["slotBinding"]["leaseId"] =~ @uuid,
         true <- observation["job"]["suspended"] == true and observation["job"]["noExecution"] == true,
         true <-
           observation["podSnapshot"]["complete"] == true and
             observation["podSnapshot"]["ownedPodsAbsent"] == true,
         true <- is_binary(reservation.reservation_id) and is_binary(reservation.projection_id) do
      :ok
    else
      _ -> {:held, :abort_prepare_observation_invalid}
    end
  end

  defp request_fields(prepare_id, allocation, assignment, observation, reservation) do
    snapshot = observation["podSnapshot"]

    %{
      "contractVersion" => @contract_version,
      "prepareId" => prepare_id,
      "reservationId" => reservation.reservation_id,
      "slotLeaseId" => observation["slotBinding"]["leaseId"],
      "assignmentDigest" => assignment.sha256,
      "allocationId" => allocation.id,
      "jobResourceVersion" => observation["job"]["resourceVersion"],
      "observedAt" => observation["observedAt"],
      "suspended" => true,
      "executionStarted" => false,
      "activePods" => 0,
      "succeededPods" => 0,
      "failedPods" => 0,
      "podListResourceVersion" => snapshot["resourceVersion"],
      "ownedPodsAbsent" => true,
      "slotClaimPodsAbsent" => true
    }
  end

  defp ordered_json(request) do
    if Enum.all?(@request_fields, &Map.has_key?(request, &1)) do
      pairs = Enum.map(@request_fields, fn key -> [Jason.encode!(key), ":", Jason.encode!(request[key])] end)
      body = pairs |> Enum.intersperse(",") |> then(&["{", &1, "}"]) |> IO.iodata_to_binary()
      {:ok, body}
    else
      {:error, :invalid_abort_prepare_request}
    end
  rescue
    _ -> {:error, :invalid_abort_prepare_request}
  end

  defp ack_path(root, claim) do
    with {:ok, key} <- AbortPrepareJournal.identity_key(claim),
         true <- Path.type(root) == :absolute do
      {:ok, Path.join(root, key <> ".ack.json")}
    else
      _ -> {:error, :invalid_abort_prepare_journal_root}
    end
  end

  defp read_ack_file(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular, mode: mode}} ->
        if private_ack_mode?(mode), do: read_bounded_ack(path), else: {:error, :insecure_journal_file}

      {:error, :enoent} ->
        {:error, :enoent}

      _ ->
        {:error, :invalid_journal_file}
    end
  end

  defp read_bounded_ack(path) do
    case File.read(path) do
      {:ok, bytes} when byte_size(bytes) <= 8_192 -> {:ok, bytes}
      {:ok, _bytes} -> {:error, :invalid_journal_file}
      {:error, _} = error -> error
    end
  end

  defp private_ack_mode?(mode) do
    windows_test? =
      match?({:win32, _}, :os.type()) and Code.ensure_loaded?(ExUnit) and
        Process.get(:abort_prepare_journal_windows_test_only) == true

    windows_test? or (match?({:unix, _}, :os.type()) and Bitwise.band(mode, 0o077) == 0)
  end

  defp timestamp?(value) when is_binary(value), do: match?({:ok, _, _}, DateTime.from_iso8601(value))
  defp timestamp?(_value), do: false
  defp bounded_string?(value, max), do: is_binary(value) and byte_size(value) in 1..max and String.valid?(value)
  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
