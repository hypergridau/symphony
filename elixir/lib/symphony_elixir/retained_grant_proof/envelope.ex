defmodule SymphonyElixir.RetainedGrantProof.Envelope do
  @moduledoc """
  Internal purpose-specific Ed25519 byte envelope, not a signing service.
  Only the separately pinned root launcher supplies the installed seed/key
  binding, collected facts and trusted clock. Authenticity is not current
  authority, replay consumption, disposition or admission. No files or ENV
  are read and no keys, grants or accounting state are allocated.
  """

  @purpose "hypergrid.retained-grant-evidence.v1"
  @keys ~w(expires_at_ms facts issuer_fingerprint nonce observed_at_ms purpose signature version)
  @maximum_facts 262_144
  @maximum_integer 9_007_199_254_740_991

  @type result :: {:ok, map()} | {:error, atom()}

  @spec sign(binary(), binary(), binary(), String.t(), non_neg_integer(), non_neg_integer()) :: result()
  def sign(seed, facts, nonce, installed_fingerprint, observed, expires)
      when is_binary(seed) and byte_size(seed) == 32 and is_binary(facts) and byte_size(facts) in 1..@maximum_facts and
             is_binary(nonce) and byte_size(nonce) == 32 and is_binary(installed_fingerprint) do
    with true <- valid_clock?(observed, expires),
         {public, private} <- :crypto.generate_key(:eddsa, :ed25519, seed),
         true <- fingerprint(public) == installed_fingerprint,
         signature <- :crypto.sign(:eddsa, :none, message(public, nonce, observed, expires, facts), [private, :ed25519]) do
      {:ok,
       %{
         "version" => 1,
         "purpose" => @purpose,
         "issuer_fingerprint" => installed_fingerprint,
         "nonce" => encode(nonce),
         "observed_at_ms" => observed,
         "expires_at_ms" => expires,
         "facts" => encode(facts),
         "signature" => encode(signature)
       }}
    else
      _ -> {:error, :retained_envelope_invalid}
    end
  rescue
    _error in [ErlangError, ArgumentError] -> {:error, :retained_envelope_invalid}
  end

  def sign(_, _, _, _, _, _), do: {:error, :retained_envelope_invalid}

  @spec verify(map(), binary(), String.t(), binary(), non_neg_integer()) :: {:ok, map()} | {:error, atom()}
  def verify(envelope, installed_public, installed_fingerprint, expected_nonce, now)
      when is_map(envelope) and is_binary(installed_public) and byte_size(installed_public) == 32 and
             is_binary(installed_fingerprint) and is_binary(expected_nonce) and byte_size(expected_nonce) == 32 do
    with true <- Enum.sort(Map.keys(envelope)) == @keys,
         true <- bound_envelope?(envelope, installed_public, installed_fingerprint),
         observed = envelope["observed_at_ms"],
         expires = envelope["expires_at_ms"],
         true <- fresh_clock?(observed, expires, now),
         {:ok, ^expected_nonce} <- decode(envelope["nonce"], 32, 32),
         {:ok, facts} <- decode(envelope["facts"], 1, @maximum_facts),
         {:ok, signature} <- decode(envelope["signature"], 64, 64),
         true <- :crypto.verify(:eddsa, :none, message(installed_public, expected_nonce, observed, expires, facts), signature, [installed_public, :ed25519]) do
      {:ok, %{facts: facts, observed_at_ms: observed, expires_at_ms: expires}}
    else
      _ -> {:error, :retained_envelope_invalid}
    end
  rescue
    _error in [ErlangError, ArgumentError] -> {:error, :retained_envelope_invalid}
  end

  def verify(_, _, _, _, _), do: {:error, :retained_envelope_invalid}

  defp bound_envelope?(envelope, public, installed_fingerprint) do
    envelope["version"] === 1 and envelope["purpose"] == @purpose and
      fingerprint(public) == installed_fingerprint and envelope["issuer_fingerprint"] == installed_fingerprint
  end

  defp fresh_clock?(observed, expires, now) do
    valid_clock?(observed, expires) and safe_integer?(now) and observed <= now and now < expires
  end

  defp message(public, nonce, observed, expires, facts), do: @purpose <> <<0>> <> public <> nonce <> <<observed::unsigned-big-64, expires::unsigned-big-64>> <> :crypto.hash(:sha256, facts)

  defp fingerprint(public), do: Base.encode16(:crypto.hash(:sha256, public), case: :lower)
  defp encode(bytes), do: Base.url_encode64(bytes, padding: false)
  defp safe_integer?(value), do: is_integer(value) and value >= 0 and value <= @maximum_integer
  defp valid_clock?(observed, expires), do: safe_integer?(observed) and safe_integer?(expires) and expires > observed and expires - observed <= 60_000

  defp decode(value, minimum, maximum) when is_binary(value) and byte_size(value) <= div(maximum + 2, 3) * 4 do
    with {:ok, bytes} <- Base.url_decode64(value, padding: false),
         true <- byte_size(bytes) >= minimum and byte_size(bytes) <= maximum and encode(bytes) == value do
      {:ok, bytes}
    else
      _ -> {:error, :retained_envelope_invalid}
    end
  end

  defp decode(_, _, _), do: {:error, :retained_envelope_invalid}
end
