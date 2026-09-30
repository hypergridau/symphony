defmodule SymphonyElixir.RKE2Job.DahliaAssignmentBinding do
  @moduledoc """
  Binds the exact root-pinned managed assignment to Dahlia before a host reserves
  an OAuth slot or creates its suspended Job.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.ManagedResponsibility.Manifest

  @connect_timeout_ms 5_000
  @request_timeout_ms 10_000
  @manifest_limit 262_144
  @digest ~r/\A[a-f0-9]{64}\z/

  @spec bind(map(), map(), map()) :: {:ok, map()} | {:held, atom()}
  def bind(assignment, claim_binding, config)
      when is_map(assignment) and is_map(claim_binding) and is_map(config) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         :ok <- matching_claim?(assignment, claim_binding),
         {:ok, issue_identifier} <- issue_identifier(assignment, config),
         {:ok, manifest_bytes, signature_hex} <- verified_manifest(config),
         {:ok, origin, token, reservation_id} <- configuration(config, claim_binding),
         url = origin <> "/v1/host/assignments/" <> URI.encode(reservation_id, &URI.char_unreserved?/1) <> "/bind",
         body = %{
           issueIdentifier: issue_identifier,
           manifestBase64: Base.encode64(manifest_bytes),
           signatureHex: signature_hex
         },
         {:ok, data} <- post(config, url, token, body),
         {:ok, result} <- verified_response(data, assignment) do
      {:ok, result}
    else
      {:error, :denied} -> {:held, :managed_assignment_binding_denied}
      _ -> {:held, :managed_assignment_binding_unverified}
    end
  rescue
    _error -> {:held, :managed_assignment_binding_unverified}
  end

  def bind(_assignment, _claim_binding, _config), do: {:held, :managed_assignment_binding_unverified}

  defp matching_claim?(
         %{lease: %{issue_id: issue_id, generation: generation}, repository_ref: repository_ref},
         %{
           issue_id: issue_id,
           generation: generation,
           repository_ref: repository_ref,
           runner_id: runner_id,
           reservation_id: reservation_id
         }
       )
       when is_binary(runner_id) and byte_size(runner_id) in 1..256 and is_binary(reservation_id) and
              byte_size(reservation_id) in 1..256,
       do: :ok

  defp matching_claim?(_assignment, _claim_binding), do: {:error, :managed_assignment_claim_mismatch}

  defp issue_identifier(%{lease: %{issue_id: issue_id}}, %{managed_delegations: %{entries: entries}})
       when is_binary(issue_id) and is_list(entries) do
    case Enum.filter(entries, &(&1[:issue_id] == issue_id and is_binary(&1[:identifier]))) do
      [%{identifier: identifier}] -> {:ok, identifier}
      _ -> {:error, :managed_assignment_issue_identifier_unavailable}
    end
  end

  defp issue_identifier(_assignment, _config), do: {:error, :managed_assignment_issue_identifier_unavailable}

  defp verified_manifest(%{
         managed_delegations: %{
           source_bytes: bytes,
           source_signature_hex: signature_hex,
           source_public_key_hex: public_key_hex,
           source_sha256: configured_digest,
           signer_key_sha256: configured_signer,
           schema_version: version
         }
       })
       when is_binary(bytes) and is_binary(signature_hex) and is_binary(public_key_hex) and
              is_binary(configured_digest) and is_binary(configured_signer) and
              version in [1, 2] and byte_size(bytes) <= @manifest_limit do
    actual_digest = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    with true <- actual_digest == configured_digest,
         {:ok, signer_digest} <- Manifest.verify_signature(bytes, signature_hex, public_key_hex, version),
         true <- signer_digest == configured_signer do
      {:ok, bytes, signature_hex}
    else
      _ -> {:error, :managed_assignment_manifest_unverified}
    end
  end

  defp verified_manifest(_config), do: {:error, :managed_assignment_manifest_unverified}

  defp configuration(
         %{assignment_bind_origin: origin, runner_token: token},
         %{reservation_id: reservation_id}
       )
       when is_binary(origin) and is_binary(token) and is_binary(reservation_id) and
              byte_size(token) > 0 and byte_size(reservation_id) in 1..256 do
    if valid_origin?(origin) and String.valid?(reservation_id) do
      {:ok, String.trim_trailing(origin, "/"), token, reservation_id}
    else
      {:error, :invalid_configuration}
    end
  end

  defp configuration(_config, _claim_binding), do: {:error, :invalid_configuration}

  defp post(config, url, token, body) do
    case Map.get(config, :assignment_bind_post_fun, &Req.post/2).(url,
           headers: [{"authorization", "Bearer " <> token}],
           json: body,
           connect_options: [timeout: @connect_timeout_ms],
           receive_timeout: @request_timeout_ms,
           retry: false,
           redirect: false
         ) do
      {:ok, %Req.Response{status: status, body: data}} when status in 200..299 and is_map(data) ->
        {:ok, data}

      {:ok, %Req.Response{status: status}} when status == 409 ->
        {:error, :denied}

      _ ->
        {:error, :unverified}
    end
  rescue
    _error -> {:error, :unverified}
  end

  defp verified_response(
         %{"status" => "bound", "assignmentDigest" => digest, "branchRef" => branch_ref} = data,
         assignment
       )
       when map_size(data) == 3 and is_binary(digest) and is_binary(branch_ref) do
    expected_branch_ref = "refs/heads/" <> assignment.branch

    if Regex.match?(@digest, digest) and branch_ref == expected_branch_ref do
      {:ok, %{assignment_digest: digest, branch_ref: branch_ref}}
    else
      {:error, :managed_assignment_response_mismatch}
    end
  end

  defp verified_response(_data, _assignment), do: {:error, :managed_assignment_response_invalid}

  defp valid_origin?(origin) do
    uri = URI.parse(origin)

    uri.scheme == "https" and is_binary(uri.host) and uri.host != "" and uri.userinfo == nil and
      uri.query == nil and uri.fragment == nil and uri.path in [nil, "", "/"]
  end
end
