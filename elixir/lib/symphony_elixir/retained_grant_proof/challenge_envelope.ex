defmodule SymphonyElixir.RetainedGrantProof.ChallengeEnvelope do
  @moduledoc false

  @purpose "hypergrid.retained-grant-challenge.v1"
  @wire_keys [
    "expires_at_ms",
    "facts",
    "issuer_fingerprint",
    "nonce",
    "observed_at_ms",
    "purpose",
    "signature",
    "version"
  ]
  @base64url ~r/\A[A-Za-z0-9_-]+\z/
  @hex64 ~r/\A[0-9a-f]{64}\z/
  @max_safe_integer 9_007_199_254_740_991
  @max_facts_bytes 262_144

  @type result ::
          {:ok,
           %{
             facts: binary(),
             observed_at_ms: non_neg_integer(),
             expires_at_ms: non_neg_integer()
           }}
          | {:error, :retained_challenge_envelope_invalid}

  @type verified_with_digest :: %{
          facts: binary(),
          observed_at_ms: non_neg_integer(),
          expires_at_ms: non_neg_integer(),
          challenge_digest: String.t()
        }

  @spec verify_with_digest(map(), binary(), binary(), binary(), integer()) ::
          {:ok, verified_with_digest()} | {:error, :retained_challenge_envelope_invalid}
  def verify_with_digest(envelope, installed_public, installed_fingerprint, expected_nonce, now) do
    case verify(envelope, installed_public, installed_fingerprint, expected_nonce, now) do
      {:ok, %{facts: facts, observed_at_ms: observed, expires_at_ms: expires} = verified} ->
        digest =
          signing_message(installed_public, expected_nonce, observed, expires, facts)
          |> then(&:crypto.hash(:sha256, &1))
          |> Base.encode16(case: :lower)

        {:ok, Map.put(verified, :challenge_digest, digest)}

      {:error, :retained_challenge_envelope_invalid} ->
        {:error, :retained_challenge_envelope_invalid}
    end
  end

  @spec verify(map(), binary(), binary(), binary(), integer()) :: result()
  def verify(
        envelope_map,
        independently_installed_raw_public32,
        installed_fingerprint_hex64,
        expected_nonce32,
        trusted_now_ms
      ) do
    try do
      with :ok <-
             validate_inputs(
               independently_installed_raw_public32,
               installed_fingerprint_hex64,
               expected_nonce32,
               trusted_now_ms
             ),
           :ok <- validate_wire_map(envelope_map),
           true <- Map.get(envelope_map, "version") === 1,
           true <- Map.get(envelope_map, "purpose") === @purpose,
           true <- Map.get(envelope_map, "issuer_fingerprint") === installed_fingerprint_hex64,
           {:ok, nonce} <- decode_canonical(Map.get(envelope_map, "nonce"), 32, 32),
           true <- nonce === expected_nonce32,
           {:ok, signature} <- decode_canonical(Map.get(envelope_map, "signature"), 64, 64),
           {:ok, facts} <- decode_canonical(Map.get(envelope_map, "facts"), 1, @max_facts_bytes),
           observed_at_ms <- Map.get(envelope_map, "observed_at_ms"),
           expires_at_ms <- Map.get(envelope_map, "expires_at_ms"),
           true <- safe_integer?(observed_at_ms),
           true <- safe_integer?(expires_at_ms),
           true <- observed_at_ms <= trusted_now_ms,
           true <- trusted_now_ms < expires_at_ms,
           true <- expires_at_ms > observed_at_ms,
           true <- expires_at_ms - observed_at_ms <= 60_000,
           message <-
             signing_message(
               independently_installed_raw_public32,
               nonce,
               observed_at_ms,
               expires_at_ms,
               facts
             ),
           true <-
             :crypto.verify(
               :eddsa,
               :none,
               message,
               signature,
               [independently_installed_raw_public32, :ed25519]
             ) do
        {:ok, %{facts: facts, observed_at_ms: observed_at_ms, expires_at_ms: expires_at_ms}}
      else
        _ -> invalid()
      end
    rescue
      ArgumentError -> invalid()
      ErlangError -> invalid()
    end
  end

  defp validate_inputs(public, fingerprint, nonce, trusted_now_ms) do
    cond do
      not is_binary(public) or byte_size(public) != 32 -> :error
      not fingerprint_matches?(public, fingerprint) -> :error
      not is_binary(nonce) or byte_size(nonce) != 32 -> :error
      not safe_integer?(trusted_now_ms) -> :error
      true -> :ok
    end
  end

  defp validate_wire_map(value) when is_map(value) do
    keys = Map.keys(value)

    if map_size(value) == length(@wire_keys) and
         Enum.all?(keys, &is_binary/1) and Enum.sort(keys) === @wire_keys do
      :ok
    else
      :error
    end
  end

  defp validate_wire_map(_), do: :error

  defp fingerprint_matches?(public, fingerprint) when is_binary(fingerprint) do
    Regex.match?(@hex64, fingerprint) and
      Base.encode16(:crypto.hash(:sha256, public), case: :lower) === fingerprint
  end

  defp fingerprint_matches?(_, _), do: false

  defp safe_integer?(value) do
    is_integer(value) and value >= 0 and value <= @max_safe_integer
  end

  defp decode_canonical(value, min_bytes, max_bytes) when is_binary(value) do
    max_encoded_bytes = div(max_bytes * 4 + 2, 3)

    cond do
      byte_size(value) < 1 or byte_size(value) > max_encoded_bytes ->
        :error

      not Regex.match?(@base64url, value) ->
        :error

      true ->
        case Base.url_decode64(value, padding: false) do
          {:ok, decoded} ->
            if byte_size(decoded) >= min_bytes and byte_size(decoded) <= max_bytes and
                 Base.url_encode64(decoded, padding: false) === value do
              {:ok, decoded}
            else
              :error
            end

          :error ->
            :error
        end
    end
  end

  defp decode_canonical(_, _, _), do: :error

  defp signing_message(public, nonce, observed_at_ms, expires_at_ms, facts) do
    @purpose <>
      <<0>> <>
      public <>
      nonce <>
      <<observed_at_ms::unsigned-big-64>> <>
      <<expires_at_ms::unsigned-big-64>> <>
      :crypto.hash(:sha256, facts)
  end

  defp invalid, do: {:error, :retained_challenge_envelope_invalid}
end
