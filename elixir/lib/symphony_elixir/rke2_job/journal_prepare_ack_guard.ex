defmodule SymphonyElixir.RKE2Job.JournalPrepareAckGuard do
  @moduledoc "Verifies a provider prepare acknowledgment against the durable host checkpoints."

  @behaviour SymphonyElixir.RKE2Job.PrepareAckGuard

  alias SymphonyElixir.RKE2Job.AbortPrepareJournal

  @impl true
  @spec verify(String.t(), String.t(), map(), map(), term()) :: :ok | {:held, atom()}
  def verify(assignment_digest, allocation_id, observation, acknowledgement, context)
      when is_binary(assignment_digest) and is_binary(allocation_id) and is_map(observation) and
             is_map(acknowledgement) and is_map(context) do
    with %{journal_root: root, claim: claim} <- context,
         {:ok, record} <- AbortPrepareJournal.load(root, claim),
         true <- record.assignment_digest == assignment_digest,
         true <- record.allocation_id == allocation_id,
         true <- record.observation == observation,
         :ok <- valid_acknowledgement(record, acknowledgement),
         true <- AbortPrepareJournal.intent_recorded?(root, claim, record),
         true <- AbortPrepareJournal.ack_recorded?(root, claim, record, acknowledgement) do
      :ok
    else
      :missing -> {:held, :abort_prepare_journal_missing}
      {:held, reason} -> {:held, reason}
      _ -> {:held, :prepare_ack_observation_mismatch}
    end
  rescue
    _ -> {:held, :abort_prepare_ack_verification_failed}
  catch
    _kind, _reason -> {:held, :abort_prepare_ack_verification_failed}
  end

  def verify(_assignment_digest, _allocation_id, _observation, _acknowledgement, _context),
    do: {:held, :prepare_ack_observation_mismatch}

  defp valid_acknowledgement(record, acknowledgement) do
    keys = ~w(prepareId projectionId reservationId preparedAt replayed prepareRequestSHA256)

    valid? =
      Enum.sort(Map.keys(acknowledgement)) == Enum.sort(keys) and
        acknowledgement["prepareId"] == record.prepare_id and
        acknowledgement["projectionId"] == record.claim["projectionId"] and
        acknowledgement["reservationId"] == record.claim["reservationId"] and
        acknowledgement["prepareRequestSHA256"] == record.request_sha256 and
        is_boolean(acknowledgement["replayed"]) and
        timestamp?(acknowledgement["preparedAt"])

    if valid?, do: :ok, else: {:held, :prepare_ack_observation_mismatch}
  end

  defp timestamp?(value) when is_binary(value), do: match?({:ok, _, _}, DateTime.from_iso8601(value))
  defp timestamp?(_value), do: false
end
