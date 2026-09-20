defmodule SymphonyElixir.RetainedGrantProof.NonceRequest do
  @moduledoc """
  Broker request boundary: exactly 43 canonical unpadded base64url bytes.
  The decoded 32-byte nonce selects no issue, path, key, digest or clock.
  The trusted transport must bound reads and timeouts and reject extra frames.
  This decoder does not establish freshness or consume a nonce.
  """

  @spec decode(term()) :: {:ok, binary()} | {:error, :invalid_retained_nonce_request}
  def decode(frame) when is_binary(frame) and byte_size(frame) == 43 do
    with {:ok, nonce} <- Base.url_decode64(frame, padding: false),
         true <- byte_size(nonce) == 32 and Base.url_encode64(nonce, padding: false) == frame do
      {:ok, nonce}
    else
      _ -> {:error, :invalid_retained_nonce_request}
    end
  end

  def decode(_), do: {:error, :invalid_retained_nonce_request}
end
