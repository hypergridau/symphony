defmodule SymphonyElixir.WorkPackageClaim.Dispatch do
  @moduledoc "Durable pre-spawn claim lifecycle; a transport error never changes execution identity."

  alias SymphonyElixir.RKE2Job.PreSpawnAbortSnapshot
  alias SymphonyElixir.WorkPackageClaim.Journal

  @phases ~w(submitted confirmed allocation_pending allocation_suspended abort_pending recovery_pending spawn_started blocked)
  @legacy_keys ~w(phase attempts retry_at_ms authority_digest)
  @keys @legacy_keys ++ ["allocation_id"]
  @abort_keys @keys ++ ["abort_reason", "abort_config"]
  @max_attempts 6

  @spec decode(term()) :: {:ok, map() | nil} | {:error, term()}
  def decode(nil), do: {:ok, nil}

  def decode(value) when is_map(value) do
    if Enum.sort(Map.keys(value)) in [Enum.sort(@legacy_keys), Enum.sort(@keys), Enum.sort(@abort_keys)] do
      decoded =
        value
        |> Map.new(fn {key, item} -> {String.to_existing_atom(key), item} end)
        |> Map.put_new(:allocation_id, nil)
        |> decode_abort_config()

      if valid?(decoded), do: {:ok, decoded}, else: {:error, :invalid_dispatch_journal}
    else
      {:error, :invalid_dispatch_journal}
    end
  end

  def decode(_value), do: {:error, :invalid_dispatch_journal}

  @spec valid?(term()) :: boolean()
  def valid?(nil), do: true

  def valid?(%{phase: phase, attempts: attempts, retry_at_ms: retry_at, authority_digest: digest} = value) do
    valid_phase?(phase, value) and is_integer(attempts) and attempts in 1..@max_attempts and
      is_integer(retry_at) and retry_at >= 0 and is_binary(digest) and byte_size(digest) == 64 and
      valid_allocation_id?(Map.get(value, :allocation_id))
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

      dispatch = %{
        phase: "submitted",
        attempts: attempts,
        retry_at_ms: DateTime.to_unix(now, :millisecond) + min(5_000 * Integer.pow(2, attempts - 1), 60_000),
        authority_digest: digest,
        allocation_id: nil
      }

      Journal.put(journal, key, Map.put(reservation, :dispatch, dispatch))
    end
  end

  defp retry_allowed(nil, _digest), do: :ok

  defp retry_allowed(%{authority_digest: previous}, digest) when previous != digest,
    do: {:error, :claim_authority_changed}

  defp retry_allowed(%{phase: "spawn_started"}, _digest), do: {:error, :claim_spawn_already_attempted}
  defp retry_allowed(%{phase: "allocation_pending"}, _digest), do: {:error, :suspended_allocation_controller_required}
  defp retry_allowed(%{phase: "allocation_suspended"}, _digest), do: {:error, :suspended_allocation_controller_required}
  defp retry_allowed(%{phase: "abort_pending"}, _digest), do: {:error, :pre_spawn_abort_pending}
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
  @spec begin_recovery(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def begin_recovery(journal, key, input) when is_map(input) do
    case journal.reservations[key] do
      %{dispatch: %{phase: "allocation_suspended", authority_digest: digest} = dispatch} = reservation
      when is_binary(digest) ->
        if digest == authority_digest(input) do
          Journal.put(journal, key, %{reservation | dispatch: dispatch})
        else
          {:error, :claim_authority_changed}
        end

      %{dispatch: %{phase: phase, authority_digest: digest} = dispatch} = reservation
      when phase in ["submitted", "confirmed"] and is_binary(digest) ->
        if digest == authority_digest(input) do
          Journal.put(journal, key, %{reservation | dispatch: %{dispatch | phase: "recovery_pending"}})
        else
          {:error, :claim_authority_changed}
        end

      _ ->
        {:error, :invalid_claim_dispatch_transition}
    end
  end

  def begin_recovery(_journal, _key, _input), do: {:error, :invalid_claim_dispatch_transition}

  @doc "Moves only an exact confirmed, never-allocated claim into paused recovery."
  @spec begin_confirmed_recovery(map(), String.t()) :: {:ok, map()} | {:error, term()}
  def begin_confirmed_recovery(journal, key) when is_binary(key) do
    case journal.reservations[key] do
      %{dispatch: %{phase: "confirmed", allocation_id: nil} = dispatch} = reservation ->
        Journal.put(journal, key, %{reservation | dispatch: %{dispatch | phase: "recovery_pending"}})

      _ ->
        {:error, :invalid_claim_dispatch_transition}
    end
  end

  def begin_confirmed_recovery(_journal, _key), do: {:error, :invalid_claim_dispatch_transition}

  @doc "Persists the first Job allocation intent before the external create call."
  @spec begin_suspended_allocation(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def begin_suspended_allocation(journal, key, input) when is_map(input) do
    case journal.reservations[key] do
      %{dispatch: %{phase: "confirmed", authority_digest: digest} = dispatch} = reservation
      when is_binary(digest) ->
        if digest == authority_digest(input) do
          Journal.put(journal, key, %{reservation | dispatch: %{dispatch | phase: "allocation_pending"}})
        else
          {:error, :claim_authority_changed}
        end

      _ ->
        {:error, :invalid_claim_dispatch_transition}
    end
  end

  def begin_suspended_allocation(_journal, _key, _input), do: {:error, :invalid_claim_dispatch_transition}

  @doc "Durably records the exact ready allocation while its Job remains suspended."
  @spec record_suspended_allocation(map(), String.t(), map(), String.t()) :: {:ok, map()} | {:error, term()}
  def record_suspended_allocation(journal, key, input, allocation_id)
      when is_map(input) and is_binary(allocation_id) do
    case journal.reservations[key] do
      %{dispatch: %{phase: "confirmed"} = dispatch} = reservation ->
        record_new_suspended_allocation(journal, key, reservation, dispatch, input, allocation_id)

      %{dispatch: %{phase: "allocation_pending"} = dispatch} = reservation ->
        record_new_suspended_allocation(journal, key, reservation, dispatch, input, allocation_id)

      %{dispatch: %{phase: "allocation_suspended"} = dispatch} = reservation ->
        retain_suspended_allocation(journal, key, reservation, dispatch, input, allocation_id)

      _ ->
        {:error, :invalid_claim_dispatch_transition}
    end
  end

  def record_suspended_allocation(_journal, _key, _input, _allocation_id),
    do: {:error, :suspended_allocation_identity_invalid}

  @doc "Fences one explicitly denied, never-started allocation without changing its identity."
  @spec begin_suspended_abort(map(), String.t(), map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def begin_suspended_abort(journal, key, input, allocation_id, config) when is_map(config) do
    case journal.reservations[key] do
      %{dispatch: %{phase: "allocation_suspended", allocation_id: ^allocation_id} = dispatch} = reservation ->
        begin_new_suspended_abort(journal, key, reservation, dispatch, input, config)

      %{dispatch: dispatch} ->
        replay_suspended_abort(journal, dispatch, input, allocation_id, config)

      _ ->
        {:error, :pre_spawn_abort_not_admissible}
    end
  end

  def begin_suspended_abort(_journal, _key, _input, _allocation_id, _reason),
    do: {:error, :pre_spawn_abort_not_admissible}

  defp begin_new_suspended_abort(journal, key, reservation, dispatch, input, config) do
    if dispatch.authority_digest == authority_digest(input) do
      next =
        dispatch
        |> Map.put(:phase, "abort_pending")
        |> Map.put(:abort_reason, "codex_auth_slot_denied")
        |> Map.put(:abort_config, config)

      Journal.put(journal, key, %{reservation | dispatch: next})
    else
      {:error, :claim_authority_changed}
    end
  end

  defp replay_suspended_abort(journal, dispatch, input, allocation_id, config) do
    case dispatch do
      %{
        phase: "abort_pending",
        allocation_id: ^allocation_id,
        abort_config: ^config,
        abort_reason: "codex_auth_slot_denied",
        authority_digest: digest
      } ->
        if digest == authority_digest(input), do: {:ok, journal}, else: {:error, :claim_authority_changed}

      _ ->
        {:error, :pre_spawn_abort_not_admissible}
    end
  end

  defp record_new_suspended_allocation(journal, key, reservation, dispatch, input, allocation_id) do
    cond do
      dispatch.authority_digest != authority_digest(input) ->
        {:error, :claim_authority_changed}

      not valid_allocation_id?(allocation_id) ->
        {:error, :suspended_allocation_identity_invalid}

      true ->
        next_dispatch = dispatch |> Map.put(:phase, "allocation_suspended") |> Map.put(:allocation_id, allocation_id)
        Journal.put(journal, key, %{reservation | dispatch: next_dispatch})
    end
  end

  defp retain_suspended_allocation(journal, key, reservation, %{allocation_id: allocation_id} = dispatch, input, allocation_id) do
    if dispatch.authority_digest == authority_digest(input),
      do: Journal.put(journal, key, %{reservation | dispatch: dispatch}),
      else: {:error, :claim_authority_changed}
  end

  defp retain_suspended_allocation(_journal, _key, _reservation, _dispatch, _input, _allocation_id),
    do: {:error, :suspended_allocation_identity_changed}

  @doc "Fences only a synchronously rejected pre-witness spawn attempt."
  @spec begin_pre_witness_recovery(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def begin_pre_witness_recovery(journal, key, input) when is_map(input) do
    case journal.reservations[key] do
      %{dispatch: %{phase: "spawn_started", authority_digest: digest} = dispatch} = reservation
      when is_binary(digest) ->
        if digest == authority_digest(input) do
          Journal.put(journal, key, %{reservation | dispatch: %{dispatch | phase: "recovery_pending"}})
        else
          {:error, :claim_authority_changed}
        end

      _ ->
        {:error, :invalid_claim_dispatch_transition}
    end
  end

  def begin_pre_witness_recovery(_journal, _key, _input),
    do: {:error, :invalid_claim_dispatch_transition}

  @spec begin_spawn(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def begin_spawn(journal, key, input) do
    case get_in(journal, [:reservations, key, :dispatch]) do
      %{phase: "allocation_suspended"} ->
        {:error, :suspended_allocation_controller_required}

      %{authority_digest: digest} ->
        if digest == authority_digest(input),
          do: change_phase(journal, key, "confirmed", "spawn_started"),
          else: {:error, :claim_authority_changed}

      _ ->
        {:error, :claim_confirmation_missing}
    end
  end

  @doc "Durably records activation intent for the exact suspended allocation; same-ID replay preserves it."
  @spec begin_suspended_spawn(map(), String.t(), map(), String.t()) :: {:ok, map()} | {:error, term()}
  def begin_suspended_spawn(journal, key, input, allocation_id)
      when is_map(input) and is_binary(allocation_id) do
    case journal.reservations[key] do
      %{dispatch: dispatch} = reservation ->
        begin_suspended_dispatch(journal, key, reservation, dispatch, input, allocation_id)

      _ ->
        {:error, :invalid_claim_dispatch_transition}
    end
  end

  def begin_suspended_spawn(_journal, _key, _input, _allocation_id),
    do: {:error, :suspended_allocation_identity_invalid}

  defp begin_suspended_dispatch(
         journal,
         key,
         reservation,
         %{phase: "allocation_suspended", allocation_id: allocation_id} = dispatch,
         input,
         allocation_id
       ) do
    cond do
      not valid_allocation_id?(allocation_id) -> {:error, :suspended_allocation_identity_invalid}
      dispatch.authority_digest != authority_digest(input) -> {:error, :claim_authority_changed}
      true -> Journal.put(journal, key, %{reservation | dispatch: Map.put(dispatch, :phase, "spawn_started")})
    end
  end

  defp begin_suspended_dispatch(
         journal,
         _key,
         _reservation,
         %{phase: "spawn_started", allocation_id: allocation_id} = dispatch,
         input,
         allocation_id
       ) do
    if dispatch.authority_digest == authority_digest(input),
      do: {:ok, journal},
      else: {:error, :claim_authority_changed}
  end

  defp begin_suspended_dispatch(
         _journal,
         _key,
         _reservation,
         %{phase: "allocation_suspended"},
         _input,
         _allocation_id
       ),
       do: {:error, :suspended_allocation_identity_changed}

  defp begin_suspended_dispatch(
         _journal,
         _key,
         _reservation,
         _dispatch,
         _input,
         _allocation_id
       ),
       do: {:error, :invalid_claim_dispatch_transition}

  @doc "Reads an already started exact dispatch for root-only spawn-intent recovery."
  @spec replay_spawn(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def replay_spawn(journal, key, input) do
    case journal.reservations[key] do
      %{dispatch: %{phase: "spawn_started", authority_digest: digest}} = reservation ->
        if digest == authority_digest(input), do: {:ok, reservation}, else: {:error, :claim_authority_changed}

      _ ->
        {:error, :claim_spawn_replay_unavailable}
    end
  end

  @spec find(map(), String.t(), String.t(), String.t(), pos_integer()) :: {:ok, map()} | {:error, term()}
  def find(journal, issue_id, profile, repository, generation) do
    key = Journal.reservation_key(issue_id, profile, repository, generation)

    case journal.reservations[key] do
      %{dispatch: %{phase: phase}} = reservation when phase in ["submitted", "confirmed", "allocation_pending", "allocation_suspended"] ->
        {:ok, reservation}

      %{dispatch: %{phase: "spawn_started", allocation_id: id}} = reservation when is_binary(id) ->
        {:ok, reservation}

      %{dispatch: %{phase: "spawn_started"}} ->
        {:error, :claim_spawn_already_attempted}

      %{dispatch: %{phase: "recovery_pending"}} ->
        {:error, :claim_reconciliation_required}

      %{dispatch: %{phase: "blocked"}} ->
        {:error, :claim_reconciliation_required}

      %{dispatch: %{phase: "abort_pending"}} ->
        {:error, :pre_spawn_abort_pending}

      _ ->
        {:error, :claim_recovery_journal_missing}
    end
  end

  @spec ready?(map(), non_neg_integer()) :: boolean()
  def ready?(%{dispatch: %{phase: "allocation_pending"}}, _now_ms), do: false
  def ready?(%{dispatch: %{phase: "allocation_suspended"}}, _now_ms), do: false
  def ready?(%{dispatch: %{phase: "abort_pending"}}, _now_ms), do: false

  def ready?(%{dispatch: dispatch}, now_ms), do: dispatch.attempts < @max_attempts and dispatch.retry_at_ms <= now_ms

  @spec retry_status(map(), non_neg_integer()) :: :ok | {:error, term()}
  def retry_status(%{dispatch: %{phase: "allocation_pending"}}, _now_ms),
    do: {:error, :suspended_allocation_controller_required}

  def retry_status(%{dispatch: %{phase: "confirmed", attempts: attempts}}, _now_ms) when attempts >= @max_attempts,
    do: {:error, :claim_confirmed_revalidation_required}

  def retry_status(%{dispatch: %{phase: "allocation_suspended"}}, _now_ms),
    do: {:error, :suspended_allocation_controller_required}

  def retry_status(%{dispatch: %{phase: "abort_pending"}}, _now_ms),
    do: {:error, :pre_spawn_abort_pending}

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
        Journal.put(journal, key, %{reservation | dispatch: Map.put(dispatch, :phase, next)})

      _ ->
        {:error, :invalid_claim_dispatch_transition}
    end
  end

  defp decode_abort_config(%{abort_config: config} = dispatch) when is_map(config) do
    fields = ~w(namespace image repository_id auth_slot auth_slot_catalog assignment_binding_digest)
    slot_fields = ~w(slot_id lease_id claim_name claim_uid assignment_sha256 binding_sha256 seat)

    if Enum.sort(Map.keys(config)) == Enum.sort(fields) and is_map(config["auth_slot"]) and
         Enum.sort(Map.keys(config["auth_slot"])) == Enum.sort(slot_fields) do
      decoded = Map.new(config, fn {key, value} -> {String.to_existing_atom(key), value} end)
      slot = Map.new(decoded.auth_slot, fn {key, value} -> {String.to_existing_atom(key), value} end)
      %{dispatch | abort_config: %{decoded | auth_slot: slot}}
    else
      dispatch
    end
  end

  defp decode_abort_config(dispatch), do: dispatch

  defp valid_phase?(
         "abort_pending",
         %{allocation_id: allocation_id, abort_reason: "codex_auth_slot_denied", abort_config: config} = value
       ),
       do:
         map_size(value) == 7 and PreSpawnAbortSnapshot.valid?(config) and
           is_binary(allocation_id) and valid_allocation_id?(allocation_id)

  defp valid_phase?("allocation_suspended", %{allocation_id: allocation_id} = value) do
    map_size(value) == 5 and is_binary(allocation_id) and valid_allocation_id?(allocation_id)
  end

  defp valid_phase?("spawn_started", %{allocation_id: allocation_id} = value) when is_binary(allocation_id),
    do: map_size(value) == 5 and valid_allocation_id?(allocation_id)

  defp valid_phase?("recovery_pending", %{allocation_id: allocation_id} = value) when is_binary(allocation_id),
    do: map_size(value) == 5 and valid_allocation_id?(allocation_id)

  defp valid_phase?(phase, value) when phase in @phases and phase not in ["allocation_suspended", "abort_pending"] do
    (map_size(value) == 4 and not Map.has_key?(value, :allocation_id)) or
      (map_size(value) == 5 and is_nil(Map.get(value, :allocation_id)))
  end

  defp valid_phase?(_phase, _value), do: false

  defp valid_allocation_id?(nil), do: true

  defp valid_allocation_id?(value),
    do: is_binary(value) and byte_size(value) in 1..1_024 and String.printable?(value) and String.trim(value) == value
end
