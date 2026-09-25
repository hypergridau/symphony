defmodule SymphonyElixir.WorkPackageClaim.Journal do
  @moduledoc """
  Private durable journal for work-package reservation replay.

  A reservation nonce is single-use state. The journal is written atomically
  before the provider claim so a lost response or process restart can replay
  the same reservation and authority tuple instead of creating a new one.
  """

  @schema_version 1

  alias SymphonyElixir.WorkPackageClaim.Dispatch

  @type reservation :: %{
          optional(:dispatch) => map(),
          optional(:cleanup_receipts) => %{optional(String.t()) => map()},
          optional(:failed_worker_turns) => %{optional(String.t()) => map()},
          optional(:workspace_id) => String.t(),
          optional(:company_id) => String.t(),
          issue_id: String.t(),
          managed_project_profile_id: String.t(),
          repository_ref: String.t(),
          projection_id: String.t(),
          reservation_id: String.t(),
          reservation_nonce: String.t(),
          scope_keys: [String.t()],
          runner_id: String.t(),
          generation: pos_integer(),
          session_id: String.t(),
          process_id: String.t(),
          responsible_delegation_id: String.t(),
          execution_fence_token: String.t(),
          runtime_lease_id: String.t()
        }

  @type state :: %{schema_version: 1, reservations: %{optional(String.t()) => reservation()}}

  @doc "Returns the legacy reservation key, retained for generation one replay."
  @spec reservation_key(String.t(), String.t(), String.t()) :: String.t()
  def reservation_key(issue_id, profile_id, repository_ref)
      when is_binary(issue_id) and is_binary(profile_id) and is_binary(repository_ref) do
    Enum.join([issue_id, profile_id, repository_ref], "\u0000")
  end

  @doc "Returns a generation-scoped key for a later claim after reconciliation."
  @spec reservation_key(String.t(), String.t(), String.t(), pos_integer()) :: String.t()
  def reservation_key(issue_id, profile_id, repository_ref, generation)
      when is_binary(issue_id) and is_binary(profile_id) and is_binary(repository_ref) and
             is_integer(generation) and generation > 0 do
    key = reservation_key(issue_id, profile_id, repository_ref)
    if generation == 1, do: key, else: key <> "\u0000" <> Integer.to_string(generation)
  end

  @spec load(Path.t()) :: {:ok, state()} | :missing | {:error, term()}
  def load(path) when is_binary(path) do
    case File.read(path) do
      {:ok, contents} -> decode(contents)
      {:error, :enoent} -> recover_missing(path)
      {:error, reason} -> {:error, {:read_failed, reason}}
    end
  end

  @doc "Decodes the exact bounded bytes authenticated by a recovery envelope."
  @spec decode_bytes(binary()) :: {:ok, state()} | {:error, term()}
  def decode_bytes(contents) when is_binary(contents), do: decode(contents)

  @spec put(state(), String.t(), reservation()) :: {:ok, state()} | {:error, term()}
  def put(%{schema_version: @schema_version, reservations: reservations} = state, key, reservation)
      when is_binary(key) and is_map(reservation) do
    {:ok, %{state | reservations: Map.put(reservations, key, reservation)}}
  end

  @doc "Records an explicit Codex `turn/failed` event once for its admitted turn identity."
  @spec put_failed_worker_turn(state(), String.t(), String.t(), map()) ::
          {:ok, state()} | {:error, term()}
  def put_failed_worker_turn(
        %{schema_version: @schema_version, reservations: reservations} = state,
        key,
        turn_key,
        evidence
      )
      when is_binary(key) and is_binary(turn_key) and is_map(evidence) do
    case Map.get(reservations, key) do
      reservation when is_map(reservation) ->
        turns = Map.get(reservation, :failed_worker_turns, %{})

        cond do
          not valid_failed_worker_turns?(%{turn_key => evidence}) ->
            {:error, :invalid_failed_worker_turn}

          same_failed_turn?(Map.get(turns, turn_key), evidence) ->
            {:ok, state}

          Map.has_key?(turns, turn_key) ->
            {:error, :failed_worker_turn_conflict}

          true ->
            next_reservation = Map.put(reservation, :failed_worker_turns, Map.put(turns, turn_key, evidence))
            {:ok, %{state | reservations: Map.put(reservations, key, next_reservation)}}
        end

      nil ->
        {:error, :reservation_missing}
    end
  end

  def put_failed_worker_turn(_state, _key, _turn_key, _evidence), do: {:error, :invalid_failed_worker_turn}

  @doc "Counts only persisted protocol failures for one exact managed issue and repository."
  @spec failed_worker_turn_count(state(), String.t(), String.t(), String.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def failed_worker_turn_count(
        %{schema_version: @schema_version, reservations: reservations} = state,
        issue_id,
        profile_id,
        repository_ref
      )
      when is_binary(issue_id) and is_binary(profile_id) and is_binary(repository_ref) do
    with :ok <- validate(state) do
      count =
        reservations
        |> Map.values()
        |> Enum.filter(fn reservation ->
          reservation.issue_id == issue_id and
            reservation.managed_project_profile_id == profile_id and
            reservation.repository_ref == repository_ref
        end)
        |> Enum.reduce(0, fn reservation, total ->
          total + map_size(Map.get(reservation, :failed_worker_turns, %{}))
        end)

      {:ok, count}
    end
  end

  def failed_worker_turn_count(_state, _issue_id, _profile_id, _repository_ref), do: {:error, :invalid_journal}

  @doc "Stores one immutable cleanup receipt semantic under its reservation journal entry."
  @spec put_cleanup_receipt(state(), String.t(), String.t(), map()) ::
          {:ok, state()} | {:error, term()}
  def put_cleanup_receipt(
        %{schema_version: @schema_version, reservations: reservations} = state,
        key,
        receipt_kind,
        receipt
      )
      when is_binary(key) and is_binary(receipt_kind) and is_map(receipt) do
    case Map.get(reservations, key) do
      %{cleanup_receipts: receipts} = reservation when is_map(receipts) ->
        put_cleanup_receipt_entry(state, key, reservation, receipts, receipt_kind, receipt)

      reservation when is_map(reservation) ->
        put_cleanup_receipt_entry(state, key, reservation, %{}, receipt_kind, receipt)

      nil ->
        {:error, :reservation_missing}
    end
  end

  def put_cleanup_receipt(_state, _key, _receipt_kind, _receipt),
    do: {:error, :invalid_cleanup_receipt}

  @spec cleanup_receipt(state(), String.t(), String.t()) :: {:ok, map()} | :missing
  def cleanup_receipt(
        %{schema_version: @schema_version, reservations: reservations},
        key,
        receipt_kind
      )
      when is_binary(key) and is_binary(receipt_kind) do
    case get_in(reservations, [key, :cleanup_receipts, receipt_kind]) do
      receipt when is_map(receipt) -> {:ok, receipt}
      _ -> :missing
    end
  end

  def cleanup_receipt(_state, _key, _receipt_kind), do: :missing

  @doc "Persists the last provider acknowledgement for a journaled cleanup receipt."
  @spec put_cleanup_receipt_ack(state(), String.t(), String.t(), map()) ::
          {:ok, state()} | {:error, term()}
  def put_cleanup_receipt_ack(
        %{schema_version: @schema_version, reservations: reservations} = state,
        key,
        receipt_kind,
        acknowledgement
      )
      when is_binary(key) and is_binary(receipt_kind) and is_map(acknowledgement) do
    case get_in(reservations, [key, :cleanup_receipts, receipt_kind]) do
      receipt when is_map(receipt) ->
        reservation = Map.fetch!(reservations, key)
        next_receipt = Map.put(receipt, :acknowledgement, acknowledgement)
        next_reservation = put_in(reservation, [:cleanup_receipts, receipt_kind], next_receipt)
        {:ok, %{state | reservations: Map.put(reservations, key, next_reservation)}}

      _ ->
        {:error, :cleanup_receipt_missing}
    end
  end

  def put_cleanup_receipt_ack(_state, _key, _receipt_kind, _acknowledgement),
    do: {:error, :invalid_cleanup_acknowledgement}

  @spec cleanup_receipt_ack(state(), String.t(), String.t()) :: {:ok, map()} | :missing
  def cleanup_receipt_ack(
        %{schema_version: @schema_version, reservations: reservations},
        key,
        receipt_kind
      )
      when is_binary(key) and is_binary(receipt_kind) do
    case get_in(reservations, [key, :cleanup_receipts, receipt_kind, :acknowledgement]) do
      acknowledgement when is_map(acknowledgement) -> {:ok, acknowledgement}
      _ -> :missing
    end
  end

  def cleanup_receipt_ack(_state, _key, _receipt_kind), do: :missing

  @spec new() :: state()
  def new, do: %{schema_version: @schema_version, reservations: %{}}

  @spec save(Path.t(), state()) :: :ok | {:error, term()}
  def save(path, %{schema_version: @schema_version, reservations: reservations} = state)
      when is_binary(path) and is_map(reservations) do
    with :ok <- validate(state),
         {:ok, encoded} <- Jason.encode(encode_state(state)),
         :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- atomic_write(path, encoded) do
      _ = File.chmod(path, 0o600)
      :ok
    end
  end

  @spec validate(state()) :: :ok | {:error, term()}
  def validate(%{schema_version: @schema_version, reservations: reservations}) when is_map(reservations) do
    if Enum.all?(reservations, fn {key, reservation} -> is_binary(key) and valid_reservation?(reservation) end) do
      :ok
    else
      {:error, :invalid_journal}
    end
  end

  def validate(_state), do: {:error, :invalid_journal}

  defp decode(contents) do
    with {:ok, payload} <- Jason.decode(contents),
         {:ok, state} <- decode_state(payload),
         :ok <- validate(state) do
      {:ok, state}
    else
      {:error, reason} -> {:error, {:invalid_journal, reason}}
      _ -> {:error, :invalid_journal}
    end
  end

  defp decode_state(%{"schema_version" => @schema_version, "reservations" => reservations}) when is_map(reservations) do
    decoded =
      Enum.reduce_while(reservations, {:ok, %{}}, fn {key, payload}, {:ok, acc} ->
        case decode_reservation(payload) do
          {:ok, reservation} -> {:cont, {:ok, Map.put(acc, key, reservation)}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case decoded do
      {:ok, reservations} -> {:ok, %{schema_version: @schema_version, reservations: reservations}}
      error -> error
    end
  end

  defp decode_state(_payload), do: {:error, :unsupported_schema}

  defp decode_reservation(payload) when is_map(payload) do
    fields = [
      {:issue_id, "issue_id"},
      {:managed_project_profile_id, "managed_project_profile_id"},
      {:repository_ref, "repository_ref"},
      {:projection_id, "projection_id"},
      {:reservation_id, "reservation_id"},
      {:reservation_nonce, "reservation_nonce"},
      {:runner_id, "runner_id"},
      {:session_id, "session_id"},
      {:process_id, "process_id"},
      {:responsible_delegation_id, "responsible_delegation_id"},
      {:execution_fence_token, "execution_fence_token"},
      {:runtime_lease_id, "runtime_lease_id"},
      {:generation, "generation"},
      {:scope_keys, "scope_keys"}
    ]

    with {:ok, values} <- required_fields(payload, fields),
         {:ok, scope_ids} <- optional_scope_ids(payload),
         true <-
           Enum.all?(
             [
               :issue_id,
               :managed_project_profile_id,
               :repository_ref,
               :projection_id,
               :reservation_id,
               :reservation_nonce,
               :runner_id,
               :session_id,
               :process_id,
               :responsible_delegation_id,
               :execution_fence_token,
               :runtime_lease_id
             ],
             &present_string?(Map.get(values, &1))
           ),
         true <- is_integer(values.generation) and values.generation > 0,
         true <- is_list(values.scope_keys) and values.scope_keys != [] and Enum.all?(values.scope_keys, &present_string?/1),
         {:ok, cleanup_receipts} <- decode_cleanup_receipts(Map.get(payload, "cleanup_receipts")),
         {:ok, failed_worker_turns} <- decode_failed_worker_turns(Map.get(payload, "failed_worker_turns")),
         {:ok, dispatch} <- Dispatch.decode(Map.get(payload, "dispatch")) do
      {:ok,
       Map.merge(values, scope_ids)
       |> maybe_put_decoded(:cleanup_receipts, cleanup_receipts)
       |> maybe_put_decoded(:failed_worker_turns, failed_worker_turns)
       |> maybe_put_decoded(:dispatch, dispatch)}
    else
      false -> {:error, :invalid_reservation}
      error -> error
    end
  end

  defp decode_reservation(_payload), do: {:error, :invalid_reservation}

  defp optional_scope_ids(payload) do
    ids = [{:workspace_id, "workspace_id"}, {:company_id, "company_id"}]

    if Enum.all?(ids, fn {_key, json_key} ->
         not Map.has_key?(payload, json_key) or present_string?(Map.get(payload, json_key))
       end) do
      {:ok, Map.new(for {key, json_key} <- ids, Map.has_key?(payload, json_key), do: {key, payload[json_key]})}
    else
      {:error, :invalid_reservation_scope}
    end
  end

  defp decode_failed_worker_turns(nil), do: {:ok, nil}

  defp decode_failed_worker_turns(turns) when is_map(turns) do
    decoded =
      Map.new(turns, fn {key, evidence} ->
        if is_map(evidence) and Enum.sort(Map.keys(evidence)) == ~w(observed_at_ms payload_sha256 thread_id turn_id) do
          {key,
           %{
             thread_id: Map.get(evidence, "thread_id"),
             turn_id: Map.get(evidence, "turn_id"),
             observed_at_ms: Map.get(evidence, "observed_at_ms"),
             payload_sha256: Map.get(evidence, "payload_sha256")
           }}
        else
          {key, evidence}
        end
      end)

    if valid_failed_worker_turns?(decoded), do: {:ok, decoded}, else: {:error, :invalid_failed_worker_turns}
  end

  defp decode_failed_worker_turns(_turns), do: {:error, :invalid_failed_worker_turns}

  defp decode_cleanup_receipts(nil), do: {:ok, nil}

  defp decode_cleanup_receipts(receipts) when is_map(receipts) do
    Enum.reduce_while(receipts, {:ok, %{}}, fn
      {kind, receipt}, {:ok, acc}
      when kind in ["termination_confirmed", "repository_cleanup_verified"] and is_map(receipt) ->
        case decode_cleanup_receipt(receipt) do
          {:ok, decoded} -> {:cont, {:ok, Map.put(acc, kind, decoded)}}
          {:error, reason} -> {:halt, {:error, reason}}
        end

      _entry, _acc ->
        {:halt, {:error, :invalid_cleanup_receipts}}
    end)
  end

  defp decode_cleanup_receipts(_receipts), do: {:error, :invalid_cleanup_receipts}

  defp decode_cleanup_receipt(receipt) when is_map(receipt) do
    keys = [
      :contract_version,
      :receipt_id,
      :receipt_kind,
      :terminal_outcome,
      :observed_at,
      :evidence_ref,
      :accepted_head,
      :runner_id,
      :managed_project_profile_id,
      :reservation_id,
      :reservation_nonce,
      :issue_id,
      :generation,
      :session_id,
      :process_id,
      :responsible_delegation_id,
      :execution_fence_token,
      :runtime_lease_id,
      :repository_ref,
      :scope_keys,
      :attested_at,
      :signature,
      :acknowledgement
    ]

    if Enum.all?(receipt, fn {key, _value} -> key in Enum.map(keys, &Atom.to_string/1) end) do
      with {:ok, acknowledgement} <- decode_cleanup_acknowledgement(Map.get(receipt, "acknowledgement")) do
        decoded =
          Map.new(receipt, fn {key, value} ->
            {String.to_existing_atom(key), value}
          end)

        {:ok, maybe_put_decoded(decoded, :acknowledgement, acknowledgement)}
      end
    else
      {:error, :invalid_cleanup_receipt}
    end
  end

  defp decode_cleanup_receipt(_receipt), do: {:error, :invalid_cleanup_receipt}

  defp decode_cleanup_acknowledgement(nil), do: {:ok, nil}

  defp decode_cleanup_acknowledgement(acknowledgement) when is_map(acknowledgement) do
    keys = [
      :projection_id,
      :reservation_id,
      :receipt_id,
      :receipt_kind,
      :execution_capacity_state,
      :scope_state,
      :reservation_state,
      :generation,
      :evidence_ref,
      :accepted_head,
      :replayed
    ]

    if Enum.all?(acknowledgement, fn {key, _value} -> key in Enum.map(keys, &Atom.to_string/1) end) do
      {:ok,
       Map.new(acknowledgement, fn {key, value} ->
         {String.to_existing_atom(key), value}
       end)}
    else
      {:error, :invalid_cleanup_acknowledgement}
    end
  end

  defp decode_cleanup_acknowledgement(_acknowledgement),
    do: {:error, :invalid_cleanup_acknowledgement}

  defp required_fields(payload, fields) do
    Enum.reduce_while(fields, {:ok, %{}}, fn {key, json_key}, {:ok, acc} ->
      case Map.fetch(payload, json_key) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        :error -> {:halt, {:error, {:missing_field, json_key}}}
      end
    end)
  end

  defp maybe_put_decoded(map, _key, nil), do: map
  defp maybe_put_decoded(map, key, value), do: Map.put(map, key, value)

  defp valid_reservation?(reservation) when is_map(reservation) do
    string_fields = [
      :issue_id,
      :managed_project_profile_id,
      :repository_ref,
      :projection_id,
      :reservation_id,
      :reservation_nonce,
      :runner_id,
      :session_id,
      :process_id,
      :responsible_delegation_id,
      :execution_fence_token,
      :runtime_lease_id
    ]

    Enum.all?(string_fields, &present_string?(Map.get(reservation, &1))) and
      Enum.all?([:workspace_id, :company_id], fn key ->
        not Map.has_key?(reservation, key) or present_string?(Map.get(reservation, key))
      end) and
      is_integer(reservation[:generation]) and reservation[:generation] > 0 and
      is_list(reservation[:scope_keys]) and reservation[:scope_keys] != [] and
      Enum.all?(reservation[:scope_keys], &present_string?/1) and
      valid_cleanup_receipts?(Map.get(reservation, :cleanup_receipts, %{})) and
      valid_failed_worker_turns?(Map.get(reservation, :failed_worker_turns, %{})) and
      Dispatch.valid?(Map.get(reservation, :dispatch))
  end

  defp valid_reservation?(_reservation), do: false

  defp valid_cleanup_receipts?(receipts) when is_map(receipts) do
    Enum.all?(receipts, fn {kind, receipt} ->
      is_binary(kind) and kind in ["termination_confirmed", "repository_cleanup_verified"] and
        is_map(receipt)
    end)
  end

  defp valid_cleanup_receipts?(_receipts), do: false

  defp valid_failed_worker_turns?(turns) when is_map(turns) do
    Enum.all?(turns, fn {key, evidence} -> valid_failed_worker_turn?(key, evidence) end)
  end

  defp valid_failed_worker_turns?(_turns), do: false

  defp valid_failed_worker_turn?(key, %{thread_id: thread_id, turn_id: turn_id} = evidence) do
    is_binary(key) and byte_size(key) > 0 and byte_size(key) <= 512 and
      map_size(evidence) == 4 and present_string?(thread_id) and present_string?(turn_id) and
      key == "#{thread_id}:#{turn_id}" and valid_failed_worker_turn_metadata?(evidence)
  end

  defp valid_failed_worker_turn?(_key, _evidence), do: false

  defp valid_failed_worker_turn_metadata?(evidence) do
    observed_at_ms = Map.get(evidence, :observed_at_ms)
    payload_sha256 = Map.get(evidence, :payload_sha256)

    is_integer(observed_at_ms) and observed_at_ms > 0 and is_binary(payload_sha256) and
      String.match?(payload_sha256, ~r/\A[0-9a-f]{64}\z/)
  end

  defp same_failed_turn?(existing, evidence) when is_map(existing) and is_map(evidence) do
    Map.take(existing, [:thread_id, :turn_id, :payload_sha256]) ==
      Map.take(evidence, [:thread_id, :turn_id, :payload_sha256])
  end

  defp same_failed_turn?(_existing, _evidence), do: false

  defp put_cleanup_receipt_entry(state, key, reservation, receipts, receipt_kind, receipt) do
    case Map.get(receipts, receipt_kind) do
      nil ->
        next_reservation =
          Map.put(reservation, :cleanup_receipts, Map.put(receipts, receipt_kind, receipt))

        {:ok, %{state | reservations: Map.put(state.reservations, key, next_reservation)}}

      ^receipt ->
        {:ok, state}

      _other ->
        {:error, :cleanup_receipt_conflict}
    end
  end

  defp recover_missing(path) do
    candidates = recovery_candidates(path)

    case candidates do
      [] ->
        :missing

      [candidate | _] ->
        read_recovery_candidate(candidate)
    end
  end

  defp recovery_candidates(path) do
    case File.ls(Path.dirname(path)) do
      {:ok, entries} ->
        entries
        |> Enum.filter(&recovery_entry?(&1, Path.basename(path)))
        |> Enum.map(&Path.join(Path.dirname(path), &1))
        |> Enum.sort_by(&recovery_mtime/1, :desc)

      {:error, _reason} ->
        []
    end
  end

  defp recovery_entry?(entry, basename) do
    String.starts_with?(entry, basename <> ".tmp-") or
      String.starts_with?(entry, basename <> ".previous-")
  end

  defp read_recovery_candidate(candidate) do
    with {:ok, contents} <- File.read(candidate),
         {:ok, state} <- decode(contents) do
      recovery_state(candidate, state)
    else
      {:error, {:invalid_journal, reason}} ->
        {:error, {:invalid_recovery_journal, candidate, reason}}

      {:error, reason} ->
        {:error, {:recovery_read_failed, candidate, reason}}
    end
  end

  defp recovery_state(candidate, state) do
    if String.contains?(Path.basename(candidate), ".tmp-"),
      do: {:ok, state},
      else: {:error, {:journal_recovery_required, candidate}}
  end

  defp recovery_mtime(path) do
    case File.stat(path, time: :posix) do
      {:ok, %{mtime: mtime}} -> mtime
      _ -> 0
    end
  end

  defp encode_state(%{schema_version: version, reservations: reservations}) do
    %{
      "schema_version" => version,
      "reservations" =>
        Map.new(reservations, fn {key, reservation} ->
          {key,
           Map.new(reservation, fn {field, value} ->
             {Atom.to_string(field), value}
           end)}
        end)
    }
  end

  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""

  defp atomic_write(path, contents) do
    temporary_path = "#{path}.tmp-#{System.unique_integer([:positive])}"

    result =
      with :ok <- write_synced(temporary_path, contents),
           :ok <- replace_file(temporary_path, path) do
        :ok
      else
        {:error, reason} -> {:error, reason}
      end

    if result != :ok, do: File.rm(temporary_path)
    result
  end

  defp write_synced(path, contents) do
    case :file.open(String.to_charlist(path), [:write, :binary, :raw, :sync]) do
      {:ok, handle} ->
        chmod_result = File.chmod(path, 0o600)

        result = with :ok <- chmod_result, do: :file.write(handle, contents)

        try do
          result
        after
          :file.close(handle)
        end

      {:error, reason} ->
        {:error, {:write_failed, reason}}
    end
  end

  defp replace_file(temporary_path, path) do
    case File.rename(temporary_path, path) do
      :ok ->
        :ok

      {:error, :eexist} ->
        replace_existing_file(temporary_path, path)

      {:error, reason} ->
        {:error, {:rename_failed, reason}}
    end
  end

  defp replace_existing_file(temporary_path, path) do
    backup_path = "#{path}.previous-#{System.unique_integer([:positive])}"

    with :ok <- File.rename(path, backup_path),
         :ok <- File.rename(temporary_path, path) do
      _ = File.rm(backup_path)
      :ok
    else
      {:error, reason} -> restore_previous_file(backup_path, path, reason)
    end
  end

  defp restore_previous_file(backup_path, path, reason) do
    _ = File.rm(path)

    case File.rename(backup_path, path) do
      :ok ->
        {:error, {:rename_failed, reason}}

      {:error, restore_reason} ->
        {:error, {:rename_failed, reason, {:restore_failed, restore_reason}}}
    end
  end
end
