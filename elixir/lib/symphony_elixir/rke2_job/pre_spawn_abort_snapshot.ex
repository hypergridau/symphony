defmodule SymphonyElixir.RKE2Job.PreSpawnAbortSnapshot do
  @moduledoc "Validates the exact credential-free allocation context retained by an abort fence."

  alias SymphonyElixir.RKE2Job.JobSpec

  @fields ~w(namespace image repository_id auth_slot auth_slot_catalog assignment_binding_digest)a
  @slot_fields ~w(slot_id lease_id claim_name claim_uid assignment_sha256 binding_sha256 seat)a
  @digest ~r/\A[a-f0-9]{64}\z/

  @spec valid?(term()) :: boolean()
  def valid?(config) when is_map(config) do
    valid_config_shape?(config) and valid_slot_shape?(config) and valid_context?(config)
  rescue
    _ -> false
  end

  def valid?(_config), do: false

  @spec validate(term(), map()) :: :ok | {:held, atom()}
  def validate(config, assignment) do
    with true <- valid?(config),
         true <- config.auth_slot.assignment_sha256 == assignment.sha256,
         true <- config.auth_slot.seat == assignment.seat,
         {:ok, _job} <- JobSpec.compile(assignment, config) do
      :ok
    else
      _ -> {:held, :pre_spawn_abort_context_changed}
    end
  rescue
    _ -> {:held, :pre_spawn_abort_context_changed}
  end

  defp valid_config_shape?(config), do: Enum.sort(Map.keys(config)) == Enum.sort(@fields)

  defp valid_slot_shape?(config) do
    slot = Map.get(config, :auth_slot)
    is_map(slot) and Enum.sort(Map.keys(slot)) == Enum.sort(@slot_fields)
  end

  defp valid_context?(config) do
    slot = config.auth_slot

    config.namespace == "frigga" and
      Enum.all?([config.image, config.repository_id] ++ Map.values(slot), &text?/1) and
      digest?(config.assignment_binding_digest) and digest?(slot.assignment_sha256) and
      digest?(slot.binding_sha256) and slot.binding_sha256 == config.assignment_binding_digest and
      config.auth_slot_catalog == %{slot.slot_id => slot.claim_name}
  end

  defp text?(value), do: is_binary(value) and byte_size(value) in 1..512 and String.valid?(value)
  defp digest?(value), do: is_binary(value) and Regex.match?(@digest, value)
end
