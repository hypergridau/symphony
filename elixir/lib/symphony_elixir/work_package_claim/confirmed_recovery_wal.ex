defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWAL do
  @moduledoc false

  @spec replay([term()], (term() -> :ok | {:error, term()})) :: :ok | {:error, term()}
  def replay(names, apply_one) when is_list(names) and is_function(apply_one, 1) do
    Enum.reduce_while(names, :ok, fn name, :ok ->
      case apply_one.(name) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
        _ -> {:halt, {:error, :invalid_replay_result}}
      end
    end)
  end

  def replay(_names, _apply_one), do: {:error, :invalid_replay_request}

  @spec apply_images(
          [%{name: term(), preimage_sha256: String.t(), postimage_bytes: binary()}],
          (term() -> binary() | {:error, term()}),
          (term(), binary(), boolean() -> :ok | {:error, term()})
        ) :: :ok | {:error, term()}
  def apply_images(images, read_current, persist)
      when is_list(images) and is_function(read_current, 1) and is_function(persist, 3) do
    Enum.reduce_while(images, :ok, fn image, :ok ->
      case apply_one_image(image, read_current, persist) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
        _ -> {:halt, {:error, :invalid_replay_result}}
      end
    end)
  end

  def apply_images(_images, _read_current, _persist), do: {:error, :invalid_replay_request}

  defp apply_one_image(
         %{name: name, preimage_sha256: preimage_sha256, postimage_bytes: postimage_bytes},
         read_current,
         persist
       )
       when is_binary(preimage_sha256) and is_binary(postimage_bytes) do
    case read_current.(name) do
      current when is_binary(current) ->
        already_applied? = current == postimage_bytes

        if already_applied? or sha256(current) == preimage_sha256 do
          persist.(name, postimage_bytes, already_applied?)
        else
          {:error, :transaction_target_conflict}
        end

      {:error, _reason} = error ->
        error

      _ ->
        {:error, :transaction_target_conflict}
    end
  end

  defp apply_one_image(_image, _read_current, _persist), do: {:error, :invalid_replay_request}

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  @spec commit_then_release((-> :ok | {:error, term()}), (-> :ok | {:error, term()})) ::
          :ok | {:error, term()}
  def commit_then_release(commit, release) when is_function(commit, 0) and is_function(release, 0) do
    with :ok <- commit.(),
         :ok <- release.() do
      :ok
    else
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_finalization_result}
    end
  end

  def commit_then_release(_commit, _release), do: {:error, :invalid_finalization_request}
end
