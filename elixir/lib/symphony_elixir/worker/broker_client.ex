defmodule SymphonyElixir.Worker.BrokerClient do
  @moduledoc """
  Job-local transport for Dahlia's exact assignment credential broker.

  It reads the rotating projected service-account token for every operation. A
  lost response is held without an automatic retry because issuance and GitHub
  writes can have succeeded remotely. Credential bytes are returned only to the
  caller and are never included in errors or logs.
  """

  @origin "http://runner-credential-broker.dahlia.svc.cluster.local:4030"
  @token_file "/var/run/secrets/frigga-broker/token"
  @timeout_ms 10_000
  @test_environment Mix.env() == :test

  @type result(value) ::
          {:ok, value} | {:error, :broker_denied | :invalid_broker_request} | {:held, :broker_uncertain}

  @spec issue(map(), :git_checkout | :git_checkout_push_pr, String.t(), DateTime.t(), pos_integer(), map()) ::
          result(map())
  def issue(subject, use, idempotency_key, now, ttl_seconds, context \\ %{}) do
    if valid_issue_request?(subject, use, idempotency_key, now, ttl_seconds) do
      request = %{
        contractVersion: "assignment-credential-lease.v1",
        idempotencyKey: idempotency_key,
        subject: subject,
        provider: "github_app_installation",
        use: Atom.to_string(use),
        requestedAt: DateTime.to_iso8601(now),
        notAfter: now |> DateTime.add(ttl_seconds, :second) |> DateTime.to_iso8601()
      }

      post("/v1/issue", request, context) |> issued_response(request)
    else
      {:error, :invalid_broker_request}
    end
  end

  @spec checkout(String.t(), String.t(), String.t(), map()) :: result(%{installation_token: String.t()})
  def checkout(lease_id, repository_ref, not_after, context \\ %{}) do
    if valid_lease_id?(lease_id) and valid_repo_ref?(repository_ref) and is_binary(not_after) do
      post("/v1/checkout", %{leaseId: lease_id}, context)
      |> checkout_response(repository_ref, not_after)
    else
      {:error, :invalid_broker_request}
    end
  end

  @spec commit(String.t(), String.t(), String.t(), [map()], map()) :: result(String.t())
  def commit(lease_id, expected_oid, message, additions, context \\ %{}) do
    if valid_lease_id?(lease_id) and oid?(expected_oid) and valid_message?(message) and valid_additions?(additions) do
      body = %{leaseId: lease_id, expectedHeadOid: expected_oid, message: message, additions: additions}
      post("/v1/commit", body, context) |> commit_response()
    else
      {:error, :invalid_broker_request}
    end
  end

  @spec branch(String.t(), String.t(), String.t(), map()) :: result(String.t())
  def branch(lease_id, expected_base_oid, branch_ref, context \\ %{}) do
    if valid_lease_id?(lease_id) and oid?(expected_base_oid) and valid_branch_ref?(branch_ref) do
      post("/v1/branch", %{leaseId: lease_id, expectedBaseOid: expected_base_oid}, context)
      |> branch_response(branch_ref)
    else
      {:error, :invalid_broker_request}
    end
  end

  @spec pull_request(String.t(), String.t(), map()) :: result(%{number: pos_integer(), url: String.t()})
  def pull_request(lease_id, repository_ref, context \\ %{}) do
    if valid_lease_id?(lease_id) and valid_repo_ref?(repository_ref) do
      post("/v1/pull-request", %{leaseId: lease_id}, context) |> pull_request_response(repository_ref)
    else
      {:error, :invalid_broker_request}
    end
  end

  @spec revoke(String.t(), map()) ::
          :ok | {:error, :invalid_broker_request} | {:held, :broker_uncertain}
  def revoke(lease_id, context \\ %{}) do
    if valid_lease_id?(lease_id) do
      case post("/v1/revoke", %{leaseId: lease_id}, context) do
        {:ok, %{"issued" => true, "metadata" => %{"leaseId" => ^lease_id, "state" => state}}}
        when state in ["revoked", "expired"] ->
          :ok

        _ ->
          {:held, :broker_uncertain}
      end
    else
      {:error, :invalid_broker_request}
    end
  end

  defp issued_response({:ok, %{"issued" => true, "metadata" => metadata}}, request) when is_map(metadata) do
    if valid_issued_metadata?(metadata, request), do: {:ok, metadata}, else: {:held, :broker_uncertain}
  end

  defp issued_response({:ok, %{"issued" => false, "reason" => reason}}, _request)
       when reason in [
              "invalid_request",
              "authority_denied",
              "assignment_mismatch",
              "provider_unavailable",
              "lease_denied",
              "delivery_failed",
              "lease_not_found",
              "revocation_denied"
            ],
       do: {:error, :broker_denied}

  defp issued_response({:ok, %{"issued" => false}}, _request), do: {:held, :broker_uncertain}
  defp issued_response({:ok, _body}, _request), do: {:held, :broker_uncertain}
  defp issued_response(other, _request), do: other

  defp checkout_response(
         {:ok, %{"status" => "issued", "installationToken" => token, "repositoryRef" => returned_ref, "useNotAfter" => returned_cutoff}},
         repository_ref,
         not_after
       )
       when is_binary(token) and byte_size(token) in 1..16_384 and returned_ref == repository_ref and
              returned_cutoff == not_after,
       do: {:ok, %{installation_token: token}}

  defp checkout_response({:ok, %{"status" => "denied"}}, _repository_ref, _not_after), do: {:error, :broker_denied}
  defp checkout_response({:ok, _body}, _repository_ref, _not_after), do: {:held, :broker_uncertain}
  defp checkout_response(other, _repository_ref, _not_after), do: other

  defp commit_response({:ok, %{"status" => "committed", "oid" => oid}}) do
    if oid?(oid), do: {:ok, oid}, else: {:held, :broker_uncertain}
  end

  defp commit_response({:ok, %{"status" => "denied"}}), do: {:error, :broker_denied}
  defp commit_response({:ok, _body}), do: {:held, :broker_uncertain}
  defp commit_response(other), do: other

  defp branch_response({:ok, %{"status" => "created", "branchRef" => branch_ref, "headOid" => oid}}, branch_ref) do
    if oid?(oid), do: {:ok, oid}, else: {:held, :broker_uncertain}
  end

  defp branch_response({:ok, %{"status" => "denied"}}, _branch_ref), do: {:error, :broker_denied}
  defp branch_response({:ok, _body}, _branch_ref), do: {:held, :broker_uncertain}
  defp branch_response(other, _branch_ref), do: other

  defp pull_request_response({:ok, %{"status" => "created", "number" => number, "url" => url}}, repository_ref)
       when is_integer(number) and number > 0 do
    if url == "https://github.com/#{repository_ref}/pull/#{number}",
      do: {:ok, %{number: number, url: url}},
      else: {:held, :broker_uncertain}
  end

  defp pull_request_response({:ok, %{"status" => "denied"}}, _repository_ref), do: {:error, :broker_denied}
  defp pull_request_response({:ok, _body}, _repository_ref), do: {:held, :broker_uncertain}
  defp pull_request_response(other, _repository_ref), do: other

  defp post(path, body, context) do
    with {:ok, token} <- projected_token(context),
         {:ok, plug} <- test_plug(context) do
      options = [
        method: :post,
        url: @origin <> path,
        headers: [{"authorization", "Bearer " <> token}, {"accept", "application/json"}],
        json: body,
        connect_options: [timeout: @timeout_ms],
        receive_timeout: @timeout_ms,
        retry: false,
        redirect: false
      ]

      options = if plug, do: Keyword.put(options, :plug, {Req.Test, plug}), else: options

      case Req.request(options) do
        {:ok, %{status: status, body: response}} when status in 200..299 and is_map(response) -> {:ok, response}
        {:ok, %{status: status}} when status in 400..499 -> {:error, :broker_denied}
        _ -> {:held, :broker_uncertain}
      end
    else
      _ -> {:error, :invalid_broker_request}
    end
  end

  defp projected_token(context) do
    path = if @test_environment, do: Map.get(context, :token_file, @token_file), else: @token_file

    case File.read(path) do
      {:ok, token} when byte_size(token) in 1..16_384 ->
        valid_projected_token(token)

      _ ->
        :error
    end
  end

  defp valid_projected_token(token) do
    if String.valid?(token) do
      trimmed = String.trim(token)
      if Regex.match?(~r/\A[A-Za-z0-9._~+\/-]+=*\z/, trimmed), do: {:ok, trimmed}, else: :error
    else
      :error
    end
  end

  defp test_plug(context) do
    plug = Map.get(context, :test_plug)
    if is_nil(plug) or (@test_environment and is_atom(plug)), do: {:ok, plug}, else: :error
  end

  defp valid_issued_metadata?(metadata, request) do
    valid_lease_id?(metadata["leaseId"]) and metadata["state"] == "active" and
      metadata["contractVersion"] == request.contractVersion and
      metadata["idempotencyKey"] == request.idempotencyKey and
      metadata["subject"] == Jason.decode!(Jason.encode!(request.subject)) and
      valid_issued_scope?(metadata, request)
  end

  defp valid_issued_scope?(metadata, request) do
    scopes = if request.use == "git_checkout", do: ["contents:read"], else: ["contents:write", "pull_requests:write"]

    metadata["scopeLabels"] == scopes and
      metadata["provider"] == request.provider and metadata["use"] == request.use and
      metadata["requestedAt"] == request.requestedAt and metadata["notAfter"] == request.notAfter and
      metadata["providerRepositoryId"] == request.subject.repositoryId and
      metadata["providerRepositoryRef"] == request.subject.repositoryRef
  end

  defp valid_issue_request?(subject, use, key, now, ttl) do
    valid_subject?(subject) and use in [:git_checkout, :git_checkout_push_pr] and
      valid_key?(key) and match?(%DateTime{}, now) and is_integer(ttl) and ttl in 1..600
  end

  defp valid_subject?(subject) when is_map(subject) do
    expected = [:assignmentDigest, :issueUuid, :generation, :runnerId, :repositoryId, :repositoryRef, :branchRef]

    Enum.sort(Map.keys(subject)) == Enum.sort(expected) and
      valid_subject_identity?(subject) and valid_subject_repository?(subject)
  end

  defp valid_subject?(_subject), do: false

  defp valid_subject_identity?(subject) do
    issue_uuid = ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/i

    is_binary(subject.issueUuid) and Regex.match?(issue_uuid, subject.issueUuid) and
      is_binary(subject.assignmentDigest) and Regex.match?(~r/\A[a-f0-9]{64}\z/, subject.assignmentDigest) and
      is_integer(subject.generation) and subject.generation > 0 and
      is_binary(subject.runnerId) and subject.runnerId != ""
  end

  defp valid_subject_repository?(subject) do
    is_binary(subject.repositoryId) and Regex.match?(~r/\A[1-9][0-9]*\z/, subject.repositoryId) and
      valid_repo_ref?(subject.repositoryRef) and is_binary(subject.branchRef) and
      String.starts_with?(subject.branchRef, "refs/heads/")
  end

  defp valid_key?(value),
    do: is_binary(value) and byte_size(value) in 1..256 and String.valid?(value) and String.trim(value) != ""

  defp valid_lease_id?(value),
    do: is_binary(value) and byte_size(value) in 1..256 and Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._:-]*\z/, value)

  defp valid_repo_ref?(value), do: is_binary(value) and Regex.match?(~r/\A[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\z/, value)

  defp valid_branch_ref?(value),
    do: is_binary(value) and byte_size(value) in 12..256 and Regex.match?(~r|\Arefs/heads/[A-Za-z0-9][A-Za-z0-9._/-]*\z|, value) and not String.contains?(value, "..")

  defp oid?(value), do: is_binary(value) and Regex.match?(~r/\A[a-f0-9]{40}\z/, value)

  defp valid_message?(value),
    do: is_binary(value) and byte_size(value) in 1..256 and String.valid?(value) and String.trim(value) != ""

  defp valid_additions?(items) when is_list(items) and length(items) in 1..20 do
    paths = Enum.map(items, fn item -> if is_map(item), do: Map.get(item, :path), else: nil end)

    Enum.all?(items, fn item ->
      is_map(item) and Enum.sort(Map.keys(item)) == [:contents, :path] and
        valid_path?(item.path) and
        is_binary(item.contents) and String.valid?(item.contents)
    end) and length(Enum.uniq(paths)) == length(paths) and
      Enum.reduce(items, 0, fn item, bytes -> bytes + byte_size(item.contents) end) <= 512 * 1024
  end

  defp valid_additions?(_items), do: false

  defp valid_path?(path) when is_binary(path) do
    byte_size(path) in 1..240 and String.valid?(path) and not String.starts_with?(path, ["/", "./"]) and
      not String.ends_with?(path, "/") and not String.contains?(path, ["\\", <<0>>]) and
      Enum.all?(String.split(path, "/"), &(&1 not in ["", ".", ".."] and not String.ends_with?(&1, ".lock")))
  end

  defp valid_path?(_path), do: false
end
