defmodule SymphonyElixir.ManagedAssignmentBundle do
  @moduledoc """
  Builds the deterministic, non-secret assignment contract passed to a managed worker.

  Callers supply the platform and environment policy explicitly. This module does
  not infer missing authority from host state or worker prompts.
  """

  @schema_version 2
  @supported_platforms ["linux-x86_64"]
  @type t :: %{
          schema_version: 2,
          objective: %{id: String.t(), identity: String.t(), content: String.t()},
          repository_ref: String.t(),
          base_ref: String.t(),
          branch: String.t(),
          seat: String.t(),
          lease: map(),
          intent_ancestry: [String.t()],
          acceptance: %{deliverable: String.t(), evidence: String.t()},
          context_secret_refs: [String.t()],
          environment: %{
            platform: String.t(),
            classification: String.t(),
            constraints: [String.t()],
            placement: :internal_beta | :hosted_production,
            target_environment: :rke2 | :lke
          },
          sha256: String.t()
        }

  @spec build(map()) :: {:ok, t()} | {:error, term()}
  def build(attrs) when is_map(attrs) do
    with :ok <- validate(attrs),
         bundle =
           Map.take(attrs, [
             :objective,
             :repository_ref,
             :base_ref,
             :branch,
             :seat,
             :lease,
             :intent_ancestry,
             :acceptance,
             :context_secret_refs
           ]),
         bundle = Map.update!(bundle, :context_secret_refs, &(Enum.uniq(&1) |> Enum.sort())),
         environment = %{
           platform: attrs.platform,
           classification: attrs.environment_classification,
           constraints: attrs.environment_constraints |> Enum.uniq() |> Enum.sort(),
           placement: attrs.placement,
           target_environment: attrs.target_environment
         },
         bundle = Map.put(bundle, :environment, environment),
         bundle = Map.put(bundle, :schema_version, @schema_version),
         {:ok, canonical} <- canonical_json(bundle) do
      {:ok, Map.put(bundle, :sha256, :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower))}
    end
  end

  def build(_attrs), do: {:error, :invalid_assignment_bundle}

  @spec validate_bundle(map()) :: :ok | {:error, term()}
  def validate_bundle(%{schema_version: @schema_version, sha256: digest} = bundle)
      when is_binary(digest) do
    with {:ok, attrs} <- bundle_attributes(bundle),
         :ok <- validate(attrs),
         {:ok, expected} <- build(attrs),
         true <- expected == bundle do
      :ok
    else
      false -> {:error, :assignment_bundle_digest_mismatch}
      {:error, _reason} = error -> error
    end
  end

  def validate_bundle(_bundle), do: {:error, :invalid_assignment_bundle}

  defp bundle_attributes(%{environment: environment} = bundle) when map_size(environment) == 5 do
    %{
      platform: platform,
      classification: classification,
      constraints: constraints,
      placement: placement,
      target_environment: target_environment
    } = environment

    attrs =
      bundle
      |> Map.drop([:schema_version, :sha256, :environment])
      |> Map.merge(%{
        platform: platform,
        environment_classification: classification,
        environment_constraints: constraints,
        placement: placement,
        target_environment: target_environment
      })

    {:ok, attrs}
  end

  defp bundle_attributes(_bundle), do: {:error, :assignment_bundle_environment_invalid}

  defp validate(attrs) do
    with :ok <-
           required_text(attrs, [
             :repository_ref,
             :base_ref,
             :branch,
             :seat,
             :platform,
             :environment_classification
           ]),
         :ok <- valid_platform(attrs.platform),
         :ok <- valid_environment_classification(attrs.environment_classification),
         :ok <- valid_placement(attrs.placement, attrs.target_environment),
         :ok <- valid_objective(Map.get(attrs, :objective)),
         :ok <- valid_lease(Map.get(attrs, :lease), Map.get(attrs, :repository_ref)),
         :ok <- valid_nonempty_text_list(Map.get(attrs, :intent_ancestry)),
         :ok <- valid_acceptance(Map.get(attrs, :acceptance)),
         :ok <- valid_text_list(Map.get(attrs, :context_secret_refs)),
         :ok <- valid_nonempty_text_list(Map.get(attrs, :environment_constraints)) do
      no_secret_values(attrs)
    end
  end

  defp valid_environment_classification("repository"), do: :ok
  defp valid_environment_classification(_classification), do: {:error, :assignment_bundle_environment_invalid}

  defp valid_placement(:internal_beta, :rke2), do: :ok
  defp valid_placement(:hosted_production, :lke), do: :ok
  defp valid_placement(_placement, _target_environment), do: {:error, :assignment_bundle_environment_invalid}

  defp valid_platform(platform) when platform in @supported_platforms, do: :ok
  defp valid_platform(_platform), do: {:error, :assignment_bundle_environment_invalid}

  defp required_text(attrs, keys) do
    if Enum.all?(keys, fn key -> valid_text?(Map.get(attrs, key)) end),
      do: :ok,
      else: {:error, :assignment_bundle_context_missing}
  end

  defp valid_lease(lease, repository_ref) when is_map(lease) and is_binary(repository_ref) do
    required = [:issue_id, :repository, :session_id, :process_id]

    if MapSet.new(Map.keys(lease)) == MapSet.new(required ++ [:generation]) and
         Enum.all?(required, &valid_text?(Map.get(lease, &1))) and
         is_integer(Map.get(lease, :generation)) and Map.get(lease, :generation) > 0 and
         lease.repository == repository_ref do
      :ok
    else
      {:error, :assignment_bundle_lease_invalid}
    end
  end

  defp valid_lease(_lease, _repository_ref), do: {:error, :assignment_bundle_lease_invalid}

  defp valid_objective(%{id: id, identity: identity, content: content} = objective) do
    if map_size(objective) == 3 and id == identity and Enum.all?([id, identity, content], &valid_text?/1),
      do: :ok,
      else: {:error, :assignment_bundle_objective_invalid}
  end

  defp valid_objective(_objective), do: {:error, :assignment_bundle_objective_invalid}

  defp valid_acceptance(%{deliverable: deliverable, evidence: evidence} = acceptance)
       when is_binary(deliverable) and is_binary(evidence) do
    if map_size(acceptance) == 2 and valid_text?(deliverable) and valid_text?(evidence),
      do: :ok,
      else: {:error, :assignment_bundle_acceptance_invalid}
  end

  defp valid_acceptance(_acceptance), do: {:error, :assignment_bundle_acceptance_invalid}

  defp valid_text_list(values) when is_list(values) do
    if length(values) <= 32 and Enum.all?(values, &valid_text?/1), do: :ok, else: {:error, :assignment_bundle_list_invalid}
  end

  defp valid_text_list(nil), do: {:error, :assignment_bundle_context_missing}
  defp valid_text_list(_values), do: {:error, :assignment_bundle_list_invalid}

  defp valid_nonempty_text_list(values) when is_list(values) and values != [], do: valid_text_list(values)
  defp valid_nonempty_text_list(nil), do: {:error, :assignment_bundle_context_missing}
  defp valid_nonempty_text_list(_values), do: {:error, :assignment_bundle_list_invalid}

  defp no_secret_values(attrs) do
    refs = Map.get(attrs, :context_secret_refs)

    if is_list(refs) and Enum.all?(refs, &Regex.match?(~r/\A[A-Z][A-Z0-9_]*\z/, &1)) do
      :ok
    else
      {:error, :assignment_bundle_secret_refs_invalid}
    end
  end

  defp canonical_json(value) do
    {:ok, encode_canonical(value)}
  rescue
    _ -> {:error, :assignment_bundle_not_serializable}
  end

  defp encode_canonical(value) when is_map(value) do
    value
    |> Enum.map(fn {key, item} -> {to_string(key), item} end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {key, item} -> [Jason.encode!(key), ":", encode_canonical(item)] end)
    |> Enum.intersperse(",")
    |> then(&["{", &1, "}"])
  end

  defp encode_canonical(value) when is_list(value) do
    value
    |> Enum.map(&encode_canonical/1)
    |> Enum.intersperse(",")
    |> then(&["[", &1, "]"])
  end

  defp encode_canonical(value), do: Jason.encode!(value)

  defp valid_text?(value) when is_binary(value) do
    byte_size(value) in 1..8_192 and String.valid?(value) and String.trim(value) != "" and
      Enum.all?(:binary.bin_to_list(value), &(&1 in [9, 10, 13] or (&1 > 31 and &1 != 127)))
  end

  defp valid_text?(_value), do: false
end
