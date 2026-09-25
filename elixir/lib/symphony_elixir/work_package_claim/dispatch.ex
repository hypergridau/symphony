defmodule SymphonyElixir.WorkPackageClaim.Dispatch do
  @moduledoc "Durable pre-spawn claim lifecycle; a transport error never changes execution identity."

  alias SymphonyElixir.WorkPackageClaim.Journal

  @phases ~w(submitted confirmed recovery_pending spawn_started blocked)
  @keys ~w(phase attempts retry_at_ms authority_digest)
  @max_attempts 6

  @spec decode(term()) :: {:ok, map() | nil} | {:error, term()}
  def decode(nil), do: {:ok, nil}

  def decode(value) when is_map(value) do
    if Enum.sort(Map.keys(value)) == Enum.sort(@keys) do
      decoded = Map.new(value, fn {key, item} -> {String.to_existing_atom(key), item} end)
      if valid?(decoded), do: {:ok, decoded}, else: {:error, :invalid_dispatch_journal}
    else
      {:error, :invalid_dispatch_journal}
    end
  end

  def decode(_value), do: {:error, :invalid_dispatch_journal}

  @spec valid?(term()) :: boolean()
  def valid?(nil), do: true

  def valid?(%{phase: phase, attempts: attempts, retry_at_ms: retry_at, authority_digest: digest} = value) do
    map_size(value) == 4 and phase in @phases and is_integer(attempts) and attempts in 1..@max_attempts and
      is_integer(retry_at) and retry_at >= 0 and is_binary(digest) and byte_size(digest) == 64
  end

  def valid?(_value), do: false

  @spec submit(map(), String.t(), map(), DateTime.t()) :: {:ok, map()} | {:error, term()}
  def submit(journal, key, input, now) do
    reservation = journal.reservations[key]
    previous = reservation[:dispatch]
    digest = authority_digest(input)

    with :ok <- retry_allowed(previous, digest),
         :ok <- retry_due(previous, DateTime.to_unix(now, :millisecond)) do
      attempts = if previous, do: previous.attempts + 1, else: 1
      dispatch = %{phase: "submitted", attempts: attempts, retry_at_ms: DateTime.to_unix(now, :millisecond) + min(5_000 * Integer.pow(2, attempts - 1), 60_000), authority_digest: digest}
      Journal.put(journal, key, Map.put(reservation, :dispatch, dispatch))
    end
  end

  defp retry_allowed(nil, _digest), do: :ok

  defp retry_allowed(%{authority_digest: previous}, digest) when previous != digest,
    do: {:error, :claim_authority_changed}

  defp retry_allowed(%{phase: "spawn_started"}, _digest), do: {:error, :claim_spawn_already_attempted}
  defp retry_allowed(%{phase: "recovery_pending"}, _digest), do: {:error, :claim_reconciliation_required}
  defp retry_allowed(%{phase: "blocked"}, _digest), do: {:error, :claim_reconciliation_required}

  defp retry_allowed(%{phase: "confirmed", attempts: attempts}, _digest) when attempts >= @max_attempts,
    do: {:error, :claim_confirmed_revalidation_required}

  defp retry_allowed(%{attempts: attempts}, _digest) when attempts >= @max_attempts,
    do: {:error, :claim_recovery_exhausted}

  defp retry_allowed(_previous, _digest), do: :ok

  defp retry_due(nil, _now_ms), do: :ok
  defp retry_due(dispatch, now_ms), do: retry_status(%{dispatch: dispatch}, now_ms)

  @spec confirm(map(), String.t()) :: {:ok, map()} | {:error, term()}
  def confirm(journal, key), do: change_phase(journal, key, "submitted", "confirmed")

  @spec block(map(), String.t()) :: {:ok, map()} | {:error, term()}
  def block(journal, key), do: change_phase(journal, key, "submitted", "blocked")

  @doc "Fences a submitted or confirmed claim before external recovery can begin."
  @spec begin_recovery(map(), String.t()) :: {:ok, map()} | {:error, term()}
  def begin_recovery(journal, key) do
    case journal.reservations[key] do
      %{dispatch: %{phase: phase} = dispatch} = reservation when phase in ["submitted", "confirmed"] ->
        Journal.put(journal, key, %{reservation | dispatch: %{dispatch | phase: "recovery_pending"}})

      _ ->
        {:error, :invalid_claim_dispatch_transition}
    end
  end

  @spec begin_spawn(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def begin_spawn(journal, key, input) do
    case get_in(journal, [:reservations, key, :dispatch]) do
      %{authority_digest: digest} ->
        if digest == authority_digest(input),
          do: change_phase(journal, key, "confirmed", "spawn_started"),
          else: {:error, :claim_authority_changed}

      _ ->
        {:error, :claim_confirmation_missing}
    end
  end

  @spec find(map(), String.t(), String.t(), String.t(), pos_integer()) :: {:ok, map()} | {:error, term()}
  def find(journal, issue_id, profile, repository, generation) do
    key = Journal.reservation_key(issue_id, profile, repository, generation)

    case journal.reservations[key] do
      %{dispatch: %{phase: phase}} = reservation when phase in ["submitted", "confirmed"] -> {:ok, reservation}
      %{dispatch: %{phase: "spawn_started"}} -> {:error, :claim_spawn_already_attempted}
      %{dispatch: %{phase: "recovery_pending"}} -> {:error, :claim_reconciliation_required}
      %{dispatch: %{phase: "blocked"}} -> {:error, :claim_reconciliation_required}
      _ -> {:error, :claim_recovery_journal_missing}
    end
  end

  @spec ready?(map(), non_neg_integer()) :: boolean()
  def ready?(%{dispatch: dispatch}, now_ms), do: dispatch.attempts < @max_attempts and dispatch.retry_at_ms <= now_ms

  @spec retry_status(map(), non_neg_integer()) :: :ok | {:error, term()}
  def retry_status(%{dispatch: %{phase: "confirmed", attempts: attempts}}, _now_ms) when attempts >= @max_attempts,
    do: {:error, :claim_confirmed_revalidation_required}

  def retry_status(%{dispatch: %{attempts: attempts}}, _now_ms) when attempts >= @max_attempts,
    do: {:error, :claim_recovery_exhausted}

  def retry_status(reservation, now_ms) do
    if ready?(reservation, now_ms), do: :ok, else: {:error, :claim_recovery_backoff}
  end

  @spec authority_digest(map()) :: String.t()
  def authority_digest(input) do
    manifest = Map.get(input, :managed_delegations)

    :crypto.hash(:sha256, :erlang.term_to_binary({input[:base_url], input.runner_id, input.managed_project_profile_id, input.repository_ref, manifest}, [:deterministic]))
    |> Base.encode16(case: :lower)
  end

  defp change_phase(journal, key, expected, next) do
    case journal.reservations[key] do
      %{dispatch: %{phase: ^expected} = dispatch} = reservation ->
        Journal.put(journal, key, %{reservation | dispatch: %{dispatch | phase: next}})

      _ ->
        {:error, :invalid_claim_dispatch_transition}
    end
  end
end
