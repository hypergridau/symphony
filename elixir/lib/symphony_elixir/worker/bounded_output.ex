defmodule SymphonyElixir.Worker.BoundedOutput do
  @moduledoc false
  defstruct limit: 0, output: <<>>, truncated?: false
end

defimpl Collectable, for: SymphonyElixir.Worker.BoundedOutput do
  def into(%{limit: limit} = sink) do
    collector = fn
      %{output: output} = acc, {:cont, bytes} when is_binary(bytes) ->
        remaining = max(limit - byte_size(output), 0)
        kept = if byte_size(bytes) > remaining, do: binary_part(bytes, 0, remaining), else: bytes
        %{acc | output: output <> kept, truncated?: acc.truncated? or byte_size(bytes) > remaining}

      acc, :done ->
        acc

      _acc, :halt ->
        :ok
    end

    {sink, collector}
  end
end
