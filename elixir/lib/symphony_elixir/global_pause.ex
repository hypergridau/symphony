defmodule SymphonyElixir.GlobalPause do
  @moduledoc """
  Reads the operator-controlled global mutable-admission gate.

  A configured gate is fail-closed: only an exact `running` value in a regular
  file with no pause-transition marker permits new mutable workers. The
  root-owned marker carries an epoch that a synchronous state snapshot echoes
  for the operator's pause barrier. Missing, unreadable, or invalid state is
  reported as paused.
  An unset path preserves the upstream runtime's unconfigured/test behavior;
  production pool launchers always provide the path.
  """

  @pause_file_env "SYMPHONY_GLOBAL_PAUSE_FILE"
  @transition_file "global-mutable-pause.transition"
  @running_state "running"
  @paused_state "paused"

  @type status :: %{
          configured?: boolean(),
          paused?: boolean(),
          state: String.t(),
          path: String.t() | nil,
          reason: String.t() | nil
        }

  @spec paused?() :: boolean()
  def paused?, do: snapshot().paused?

  @spec snapshot() :: status()
  def snapshot do
    case System.get_env(@pause_file_env) do
      path when is_binary(path) and path != "" ->
        path = Path.expand(path)

        case read_transition(Path.join(Path.dirname(path), @transition_file)) do
          :none -> read_state(path)
          {:active, epoch} -> Map.put(paused_status(path, "pause_transition"), :transition_epoch, epoch)
          {:error, reason} -> paused_status(path, reason)
        end

      _ ->
        %{
          configured?: false,
          paused?: false,
          state: "unconfigured",
          path: nil,
          reason: "missing_pause_file_path"
        }
    end
  end

  defp read_transition(path) do
    case File.lstat(path) do
      {:error, :enoent} ->
        :none

      {:ok, %{type: :regular, size: size}} when size <= 48 ->
        case File.read(path) do
          {:ok, "pausing:" <> epoch_and_newline} ->
            case epoch_and_newline do
              <<epoch::binary-size(32), "\n">> ->
                if String.match?(epoch, ~r/\A[0-9a-f]{32}\z/),
                  do: {:active, epoch},
                  else: {:error, "invalid_pause_transition"}

              _ ->
                {:error, "invalid_pause_transition"}
            end

          _ ->
            {:error, "invalid_pause_transition"}
        end

      _ ->
        {:error, "invalid_pause_transition"}
    end
  end

  defp read_state(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular}} -> read_regular_state(path)
      {:ok, _} -> paused_status(path, "invalid_pause_file_type")
      {:error, reason} -> paused_status(path, Atom.to_string(reason))
    end
  end

  defp read_regular_state(path) do
    case File.read(path) do
      {:ok, @running_state <> "\n"} -> running_status(path)
      {:ok, @running_state} -> running_status(path)
      {:ok, @paused_state <> "\n"} -> paused_status(path, "operator_paused")
      {:ok, @paused_state} -> paused_status(path, "operator_paused")
      {:ok, _contents} -> paused_status(path, "invalid_pause_file_state")
      {:error, reason} -> paused_status(path, Atom.to_string(reason))
    end
  end

  defp running_status(path) do
    %{configured?: true, paused?: false, state: @running_state, path: path, reason: nil}
  end

  defp paused_status(path, reason) do
    %{configured?: true, paused?: true, state: @paused_state, path: path, reason: reason}
  end
end
