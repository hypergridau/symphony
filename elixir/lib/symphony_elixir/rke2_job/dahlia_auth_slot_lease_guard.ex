defmodule SymphonyElixir.RKE2Job.DahliaAuthSlotLeaseGuard do
  @moduledoc """
  Trusted Frigga-host client for Dahlia's exclusive Codex OAuth slot lease.

  The host must first reserve the slot and put Dahlia's returned lease ID and
  claim into the Job configuration. This guard verifies that reservation again
  before creation, then binds and checks the exact registered Job allocation.
  Release uses a trusted host observer's fresh cleanup receipt after exact Job
  deletion. Missing or mismatched evidence retains the lease.
  """

  @behaviour SymphonyElixir.RKE2Job.AuthSlotLeaseGuard

  alias SymphonyElixir.RKE2Job.{AuthCacheVerifierObserver, AuthSlotSpec, HTTPClient, ResultJournal}

  @connect_timeout_ms 5_000
  @request_timeout_ms 10_000
  @hgs733_issue_uuid "b60d9711-d8ed-4a69-8910-570d0b4bbe7a"
  @hgs733_constraint_prefix "qualification/hgs-733/pre-start-auth-denial/"
  @hgs733_reason_code "hgs733_pre_start_denial_qualification"

  @doc "Reserves one catalogued slot and returns the exact trusted Job configuration."
  @spec prepare_slot(map(), String.t(), map(), term()) :: {:ok, map()} | {:held, atom()}
  def prepare_slot(%{sha256: digest, seat: seat} = assignment, slot_id, catalog, context)
      when is_binary(digest) and is_binary(seat) and is_binary(slot_id) and is_map(catalog) do
    binding_digest = subject_digest(context, assignment)

    preflight = %{
      slot_id: slot_id,
      claim_name: Map.get(catalog, slot_id),
      claim_uid: "preflight",
      lease_id: "preflight",
      assignment_sha256: digest,
      binding_sha256: binding_digest,
      seat: seat
    }

    with true <- valid_digest?(binding_digest),
         {:ok, _fragments} <- AuthSlotSpec.compile(assignment, preflight, catalog, binding_digest),
         {:ok, claim_uid} <- read_claim_uid(context, preflight.claim_name),
         {:ok, data} <- post(context, "/reserve", %{assignmentDigest: binding_digest, slotId: slot_id, claimUid: claim_uid}),
         true <- is_boolean(data["replayed"]),
         slot = %{
           slot_id: data["slotId"],
           claim_name: data["claimName"],
           claim_uid: data["claimUid"],
           lease_id: data["leaseId"],
           assignment_sha256: digest,
           binding_sha256: binding_digest,
           seat: seat
         },
         true <- slot.slot_id == slot_id and slot.claim_uid == claim_uid and valid_lease_id?(slot.lease_id),
         {:ok, _fragments} <- AuthSlotSpec.compile(assignment, slot, catalog, binding_digest) do
      {:ok, slot}
    else
      _ -> {:held, :codex_auth_slot_reservation_unverified}
    end
  end

  def prepare_slot(_assignment, _slot_id, _catalog, _context),
    do: {:held, :codex_auth_slot_reservation_unverified}

  @doc "Reads the bound PVC identity for a retained slot without reserving or changing its lease."
  @spec verify_claim_uid(map(), term()) :: :ok | {:held, atom()}
  def verify_claim_uid(%{claim_name: name, claim_uid: uid}, context)
      when is_binary(name) and is_binary(uid) do
    case read_claim_uid(context, name) do
      {:ok, ^uid} -> :ok
      _ -> {:held, :codex_auth_slot_claim_identity_unverified}
    end
  end

  def verify_claim_uid(_slot, _context), do: {:held, :codex_auth_slot_claim_identity_unverified}

  @impl true
  def reserve(slot, assignment, context) do
    with :ok <- matching_assignment?(slot, assignment, context),
         {:ok, claim_uid} <- read_claim_uid(context, slot.claim_name),
         true <- claim_uid == slot.claim_uid,
         {:ok, data} <-
           post(context, "/reserve", %{
             assignmentDigest: Map.get(slot, :binding_sha256, assignment.sha256),
             slotId: slot.slot_id,
             claimUid: claim_uid
           }),
         true <-
           data["leaseId"] == slot.lease_id and data["slotId"] == slot.slot_id and
             data["claimName"] == slot.claim_name and data["claimUid"] == claim_uid and
             is_boolean(data["replayed"]) do
      :ok
    else
      _ -> {:held, :codex_auth_slot_reservation_unverified}
    end
  end

  @impl true
  def bind_uid(slot, assignment, allocation, context) do
    with :ok <- matching_assignment?(slot, assignment, context),
         {:ok, claim_uid} <- read_claim_uid(context, slot.claim_name),
         true <- claim_uid == slot.claim_uid,
         {:ok, data} <-
           post(context, "/" <> slot.lease_id <> "/bind-job", %{
             allocationId: allocation.id
           }),
         true <- data["bound"] == true do
      :ok
    else
      _ -> {:held, :codex_auth_slot_binding_unverified}
    end
  end

  @impl true
  def authorize(slot, assignment, allocation, context) do
    with :ok <- matching_assignment?(slot, assignment, context),
         {:ok, claim_uid} <- read_claim_uid(context, slot.claim_name),
         true <- claim_uid == slot.claim_uid,
         {:ok, data} <-
           post(context, "/" <> slot.lease_id <> "/authorize", %{
             allocationId: allocation.id
           }),
         true <- data["authorized"] == true do
      :ok
    else
      _ -> {:held, :codex_auth_slot_authorization_unverified}
    end
  end

  @doc "Checks exact slot authorization before the claim handoff establishes spawn intent."
  @spec authorize_pre_spawn(map(), map(), map(), term()) ::
          :ok | {:denied, :codex_auth_slot_denied} | {:held, atom()}
  def authorize_pre_spawn(slot, assignment, allocation, context) do
    with :ok <- matching_assignment?(slot, assignment, context),
         {:ok, claim_uid} <- read_claim_uid(context, slot.claim_name),
         true <- claim_uid == slot.claim_uid do
      case pre_spawn_qualification(assignment) do
        :ordinary ->
          with {:ok, response} <- pre_spawn_authorization_response(context, slot, allocation), do: response

        :hgs733 ->
          with :ok <- quarantine_qualified_slot(context, slot),
               {:ok, response} <- pre_spawn_authorization_response(context, slot, allocation) do
            response
          else
            _ -> {:held, :codex_auth_slot_authorization_unverified}
          end

        :invalid ->
          {:held, :codex_auth_slot_authorization_unverified}
      end
    else
      _ -> {:held, :codex_auth_slot_authorization_unverified}
    end
  rescue
    _error -> {:held, :codex_auth_slot_authorization_unverified}
  end

  @impl true
  def verify_bound(slot, assignment, allocation, context) do
    with :ok <- matching_assignment?(slot, assignment, context),
         {:ok, data} <-
           post(context, "/" <> slot.lease_id <> "/verify-bound", %{
             allocationId: allocation.id
           }),
         true <- data == %{"bound" => true} do
      :ok
    else
      _ -> {:held, :codex_auth_slot_bound_verification_unverified}
    end
  end

  @impl true
  def release(slot, assignment, allocation, context) do
    with :ok <- matching_assignment?(slot, assignment, context),
         {:ok, namespace, uid} <- allocation_identity(allocation, assignment.sha256),
         {:ok, receipts} <- cleanup_receipts(context, slot, assignment, allocation, namespace, uid),
         :ok <- release_saved_receipts(context, slot, assignment, allocation, namespace, uid, receipts) do
      :ok
    else
      _ -> {:held, :codex_auth_slot_release_verification_unavailable}
    end
  rescue
    _error -> {:held, :codex_auth_slot_release_verification_unavailable}
  end

  defp cleanup_receipts(context, slot, assignment, allocation, namespace, uid) do
    root = Map.get(context, :result_journal_root)

    case ResultJournal.load_cleanup_receipts(assignment, uid, root) do
      {:ok, receipts} ->
        {:ok, receipts}

      :missing ->
        with {:ok, receipt} <- observe_and_record(context, slot, assignment, allocation, namespace, uid, 0),
             do: {:ok, [{0, receipt}]}

      other ->
        other
    end
  end

  defp observe_and_record(context, slot, assignment, allocation, namespace, uid, version) do
    observer =
      Map.get(context, :cleanup_receipt_fun, fn slot, assignment, allocation ->
        AuthCacheVerifierObserver.observe(slot, assignment, allocation, Map.get(context, :auth_cache_verifier_context))
      end)

    root = Map.get(context, :result_journal_root)

    with true <- is_function(observer, 3),
         {:ok, receipt} <- observer.(slot, assignment, allocation),
         :ok <- matching_receipt?(receipt, slot, namespace, uid),
         {:ok, _path} <- ResultJournal.record_cleanup_receipt(assignment, uid, receipt, root, version) do
      ResultJournal.load_cleanup_receipt(assignment, uid, root, version)
    end
  end

  defp release_saved_receipts(context, slot, assignment, allocation, namespace, uid, receipts) do
    case try_saved_receipts(context, slot, allocation, namespace, uid, receipts) do
      :ok ->
        :ok

      {:error, :denied} ->
        refresh_bound_receipt(context, slot, assignment, allocation, namespace, uid, receipts)

      other ->
        other
    end
  end

  defp try_saved_receipts(context, slot, allocation, namespace, uid, receipts) do
    Enum.reduce_while(receipts, {:error, :denied}, fn {_version, receipt}, _acc ->
      with :ok <- matching_receipt?(receipt, slot, namespace, uid),
           {:ok, %{"released" => true}} <-
             post(context, "/" <> slot.lease_id <> "/release", %{
               allocationId: allocation.id,
               receipt: receipt
             }) do
        {:halt, :ok}
      else
        {:error, :denied} -> {:cont, {:error, :denied}}
        other -> {:halt, other}
      end
    end)
  end

  defp refresh_bound_receipt(context, slot, assignment, allocation, namespace, uid, [{version, _} | _]) do
    with :ok <- verify_bound(slot, assignment, allocation, context),
         {:ok, receipt} <- observe_and_record(context, slot, assignment, allocation, namespace, uid, version + 1) do
      try_saved_receipts(context, slot, allocation, namespace, uid, [{version + 1, receipt}])
    end
  end

  defp allocation_identity(%{id: "rke2job:v1:" <> encoded}, digest) do
    with {:ok, payload} <- Base.url_decode64(encoded, padding: false),
         {:ok, [1, namespace, _name, uid, ^digest]} <- Jason.decode(payload),
         true <- valid_slot_name?(namespace) and valid_claim_uid?(uid) do
      {:ok, namespace, uid}
    else
      _ -> {:error, :invalid_allocation}
    end
  end

  defp allocation_identity(_allocation, _digest), do: {:error, :invalid_allocation}

  defp matching_receipt?(receipt, slot, namespace, uid) when is_map(receipt) do
    expected = %{
      "namespace" => namespace,
      "jobUid" => uid,
      "claimName" => slot.claim_name,
      "claimUid" => slot.claim_uid,
      "jobAbsent" => true,
      "ownedPodsAbsent" => true,
      "claimPodsAbsent" => true,
      "authCacheStatus" => "codex_login_status_authenticated"
    }

    if map_size(receipt) == 13 and
         Enum.all?(expected, fn {key, value} -> Map.get(receipt, key) == value end) and
         valid_receipt_values?(receipt) do
      :ok
    else
      {:error, :invalid_receipt}
    end
  end

  defp matching_receipt?(_receipt, _slot, _namespace, _uid), do: {:error, :invalid_receipt}

  defp valid_receipt_values?(receipt) do
    valid_lease_id?(receipt["receiptId"]) and
      valid_observed_at?(receipt["observedAt"]) and
      Enum.all?(~w(podListResourceVersion claimPodListResourceVersion), fn key ->
        is_binary(receipt[key]) and byte_size(receipt[key]) in 1..128
      end) and
      is_integer(receipt["authCacheBytes"]) and receipt["authCacheBytes"] in 1..10_000_000
  end

  defp valid_observed_at?(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, _datetime, _offset} -> true
      _ -> false
    end
  end

  defp valid_observed_at?(_value), do: false

  defp matching_assignment?(
         %{assignment_sha256: digest, seat: seat} = slot,
         %{sha256: digest, seat: seat} = assignment,
         context
       ) do
    binding_digest = Map.get(slot, :binding_sha256, assignment.sha256)

    if valid_lease_id?(Map.get(slot, :lease_id)) and valid_slot_name?(Map.get(slot, :slot_id)) and
         valid_slot_name?(Map.get(slot, :claim_name)) and valid_claim_uid?(Map.get(slot, :claim_uid)) and
         valid_digest?(binding_digest) and binding_digest == subject_digest(context, assignment) do
      :ok
    else
      :invalid_binding
    end
  end

  defp matching_assignment?(_slot, _assignment, _context), do: :invalid_binding

  defp subject_digest(context, assignment) do
    Map.get(context, :assignment_subject_digest, Map.get(assignment, :sha256))
  end

  defp valid_digest?(value) when is_binary(value), do: Regex.match?(~r/\A[a-f0-9]{64}\z/, value)
  defp valid_digest?(_value), do: false

  defp valid_lease_id?(value) when is_binary(value),
    do: Regex.match?(~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, value)

  defp valid_lease_id?(_value), do: false

  defp valid_slot_name?(value) when is_binary(value),
    do: byte_size(value) <= 63 and Regex.match?(~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/, value)

  defp valid_slot_name?(_value), do: false

  defp valid_claim_uid?(value) when is_binary(value),
    do: byte_size(value) in 1..256 and Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._:-]*\z/, value)

  defp valid_claim_uid?(_value), do: false

  defp read_claim_uid(context, claim_name) when is_map(context) and is_binary(claim_name) do
    namespace = Map.get(context, :pvc_namespace)
    kube_context = Map.get(context, :pvc_client_context)
    reader = Map.get(context, :pvc_read_fun, &HTTPClient.get_pvc/3)

    if is_binary(namespace) and is_function(reader, 3) do
      reader.(namespace, claim_name, kube_context)
      |> verified_pvc_uid(namespace, claim_name)
    else
      {:error, :pvc_reader_unavailable}
    end
  rescue
    _error -> {:error, :pvc_identity_unverified}
  end

  defp read_claim_uid(_context, _claim_name), do: {:error, :pvc_reader_unavailable}

  defp verified_pvc_uid(
         {:ok,
          %{
            "apiVersion" => "v1",
            "kind" => "PersistentVolumeClaim",
            "metadata" => %{"namespace" => namespace, "name" => claim_name, "uid" => uid} = metadata,
            "status" => %{"phase" => "Bound"}
          }},
         namespace,
         claim_name
       ) do
    if valid_claim_uid?(uid) and is_nil(Map.get(metadata, "deletionTimestamp")),
      do: {:ok, uid},
      else: {:error, :pvc_identity_unverified}
  end

  defp verified_pvc_uid(_response, _namespace, _claim_name), do: {:error, :pvc_identity_unverified}

  defp post(context, suffix, body) do
    with {:ok, base_url, token, reservation_id} <- configuration(context),
         url =
           base_url <>
             "/runner/v1/verified-assignments/" <>
             URI.encode(reservation_id, &URI.char_unreserved?/1) <> "/codex-auth-slots" <> suffix,
         {:ok, %Req.Response{status: status, body: %{"data" => data}}} when status in 200..299 and is_map(data) <-
           Map.get(context, :post_fun, &Req.post/2).(url,
             headers: [{"authorization", "Bearer " <> token}],
             json: body,
             connect_options: [timeout: @connect_timeout_ms],
             receive_timeout: @request_timeout_ms,
             retry: false,
             redirect: false
           ) do
      {:ok, data}
    else
      {:ok, %Req.Response{status: 409}} -> {:error, :denied}
      _ -> {:error, :unverified}
    end
  rescue
    _error -> {:error, :unverified}
  end

  defp pre_spawn_qualification(%{environment: %{constraints: constraints}} = assignment) when is_list(constraints) do
    task_constraints = Enum.filter(constraints, &(is_binary(&1) and String.starts_with?(&1, "qualification/")))
    hgs733_constraints = Enum.filter(task_constraints, &String.starts_with?(&1, @hgs733_constraint_prefix))

    case hgs733_constraints do
      [] ->
        :ordinary

      [constraint] when length(task_constraints) == 1 ->
        case Map.get(assignment, :lease) do
          %{issue_id: @hgs733_issue_uuid, generation: generation} when is_integer(generation) and generation > 0 ->
            if constraint == @hgs733_constraint_prefix <> @hgs733_issue_uuid <> "/generation-#{generation}",
              do: :hgs733,
              else: :invalid

          _ ->
            :invalid
        end

      _ ->
        :invalid
    end
  end

  defp pre_spawn_qualification(%{environment: _environment}), do: :invalid

  defp pre_spawn_qualification(_assignment), do: :ordinary

  defp quarantine_qualified_slot(context, slot) do
    case post(context, "/" <> slot.lease_id <> "/quarantine", %{reasonCode: @hgs733_reason_code}) do
      {:ok, %{"quarantined" => true}} -> :ok
      _ -> {:error, :quarantine_unverified}
    end
  end

  defp pre_spawn_authorization_response(context, slot, allocation) do
    with {:ok, base_url, token, reservation_id} <- configuration(context),
         url =
           base_url <>
             "/runner/v1/verified-assignments/" <>
             URI.encode(reservation_id, &URI.char_unreserved?/1) <> "/codex-auth-slots/" <> slot.lease_id <> "/authorize",
         response <-
           Map.get(context, :post_fun, &Req.post/2).(url,
             headers: [{"authorization", "Bearer " <> token}],
             json: %{allocationId: allocation.id},
             connect_options: [timeout: @connect_timeout_ms],
             receive_timeout: @request_timeout_ms,
             retry: false,
             redirect: false
           ) do
      authorize_pre_spawn_response(response)
    else
      _ -> {:error, :unverified}
    end
  rescue
    _error -> {:error, :unverified}
  end

  defp authorize_pre_spawn_response({:ok, %Req.Response{status: status, body: %{"data" => %{"authorized" => true}}}})
       when status in 200..299,
       do: {:ok, :ok}

  defp authorize_pre_spawn_response({:ok, %Req.Response{status: 409, body: body}}) do
    if valid_slot_denial?(body),
      do: {:ok, {:denied, :codex_auth_slot_denied}},
      else: {:error, :unverified}
  end

  defp authorize_pre_spawn_response(_response), do: {:error, :unverified}

  defp valid_slot_denial?(
         %{
           "error" => %{
             "code" => "codex_auth_slot_denied",
             "category" => "state_conflict",
             "message" => message,
             "details" => details
           },
           "meta" => %{
             "request_id" => request_id,
             "release_version" => release_version,
             "api_version" => api_version
           }
         } = body
       ) do
    MapSet.equal?(MapSet.new(Map.keys(body)), MapSet.new(["error", "meta"])) and
      MapSet.equal?(MapSet.new(Map.keys(body["error"])), MapSet.new(["code", "category", "message", "details"])) and
      MapSet.equal?(MapSet.new(Map.keys(body["meta"])), MapSet.new(["request_id", "release_version", "api_version"])) and
      nonempty_text?(message) and is_map(details) and nonempty_text?(request_id) and
      nonempty_text?(release_version) and nonempty_text?(api_version)
  end

  defp valid_slot_denial?(_body), do: false

  defp nonempty_text?(value), do: is_binary(value) and String.valid?(value) and String.trim(value) != ""

  defp configuration(%{base_url: base_url, runner_token: token, reservation_id: reservation_id})
       when is_binary(base_url) and is_binary(token) and byte_size(token) > 0 and
              is_binary(reservation_id) and byte_size(reservation_id) in 1..256 do
    if String.valid?(reservation_id) and valid_base_url?(base_url) do
      {:ok, String.trim_trailing(base_url, "/"), token, reservation_id}
    else
      {:error, :invalid_configuration}
    end
  end

  defp configuration(_context), do: {:error, :invalid_configuration}

  defp valid_base_url?(base_url) do
    uri = URI.parse(base_url)

    uri.scheme == "https" and is_binary(uri.host) and uri.host != "" and
      uri.userinfo == nil and uri.query == nil and uri.fragment == nil and
      uri.path in [nil, "", "/"]
  end
end
