defmodule SymphonyElixir.Worker.Assignment do
  @moduledoc """
  Decodes the Job's assignment argument and broker subject without external effects.

  The host must compile the Job from a signature-verified manifest. This worker-side
  check catches a changed argument, environment digest, or repository selector before
  any broker request; it does not replace host signature verification.
  """

  alias SymphonyElixir.ManagedAssignmentBundle

  @max_json_bytes 32_768
  @keys ~w(
    schema_version objective id identity content repository_ref base_ref branch seat
    lease issue_id repository generation session_id process_id intent_ancestry
    acceptance deliverable evidence context_secret_refs environment platform
    classification constraints placement target_environment sha256
  )
  @key_atoms Map.new(@keys, &{&1, String.to_atom(&1)})

  @type decoded :: %{bundle: map(), subject: map()}

  @spec decode(String.t(), String.t(), String.t()) :: {:ok, decoded()} | {:error, :invalid_worker_assignment}
  def decode(json, expected_digest, repository_id)
      when is_binary(json) and is_binary(expected_digest) and is_binary(repository_id) do
    with true <- byte_size(json) in 1..@max_json_bytes,
         {:ok, raw} <- Jason.decode(json),
         {:ok, bundle} <- atomize_known_keys(raw),
         {:ok, bundle} <- normalize_environment(bundle),
         :ok <- ManagedAssignmentBundle.validate_bundle(bundle),
         true <- bundle.sha256 == expected_digest,
         true <- valid_repository_id?(repository_id) do
      {:ok, %{bundle: bundle, subject: subject(bundle, repository_id)}}
    else
      _ -> {:error, :invalid_worker_assignment}
    end
  end

  def decode(_json, _expected_digest, _repository_id), do: {:error, :invalid_worker_assignment}

  defp atomize_known_keys(value) when is_map(value) do
    Enum.reduce_while(value, {:ok, %{}}, fn {key, item}, {:ok, result} ->
      with atom when is_atom(atom) <- Map.get(@key_atoms, key),
           {:ok, converted} <- atomize_known_keys(item) do
        {:cont, {:ok, Map.put(result, atom, converted)}}
      else
        _ -> {:halt, {:error, :invalid_worker_assignment}}
      end
    end)
  end

  defp atomize_known_keys(value) when is_list(value) do
    Enum.reduce_while(value, {:ok, []}, fn item, {:ok, result} ->
      case atomize_known_keys(item) do
        {:ok, converted} -> {:cont, {:ok, [converted | result]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, converted} -> {:ok, Enum.reverse(converted)}
      error -> error
    end
  end

  defp atomize_known_keys(value), do: {:ok, value}

  defp normalize_environment(%{environment: %{placement: placement, target_environment: target} = environment} = bundle) do
    with {:ok, placement} <- allowed_atom(placement, %{"internal_beta" => :internal_beta, "hosted_production" => :hosted_production}),
         {:ok, target} <- allowed_atom(target, %{"rke2" => :rke2, "lke" => :lke}) do
      {:ok, %{bundle | environment: %{environment | placement: placement, target_environment: target}}}
    end
  end

  defp normalize_environment(_bundle), do: {:error, :invalid_worker_assignment}

  defp allowed_atom(value, choices) do
    case Map.fetch(choices, value) do
      {:ok, atom} -> {:ok, atom}
      :error -> {:error, :invalid_worker_assignment}
    end
  end

  defp valid_repository_id?(value),
    do: byte_size(value) <= 20 and Regex.match?(~r/\A[1-9][0-9]*\z/, value)

  defp subject(bundle, repository_id) do
    %{
      assignmentDigest: bundle.sha256,
      issueUuid: bundle.lease.issue_id,
      generation: bundle.lease.generation,
      runnerId: bundle.seat,
      repositoryId: repository_id,
      repositoryRef: bundle.repository_ref,
      branchRef: "refs/heads/" <> bundle.branch
    }
  end
end
