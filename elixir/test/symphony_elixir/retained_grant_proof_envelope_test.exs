defmodule SymphonyElixir.RetainedGrantProofEnvelopeTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.RetainedGrantProof.Envelope
  @seed :binary.copy(<<1>>, 32)
  @nonce :binary.copy(<<2>>, 32)
  @facts ~s({"fixture":true})

  setup do
    {public, _} = :crypto.generate_key(:eddsa, :ed25519, @seed)
    fingerprint = Base.encode16(:crypto.hash(:sha256, public), case: :lower)
    {:ok, envelope} = Envelope.sign(@seed, @facts, @nonce, fingerprint, 10_000, 20_000)
    %{public: public, fingerprint: fingerprint, envelope: envelope}
  end

  test "exact purpose envelope authenticates bytes without consuming nonce or conferring admission", data do
    for _ <- 1..2 do
      assert {:ok, result} = Envelope.verify(data.envelope, data.public, data.fingerprint, @nonce, 10_000)
      assert result == %{facts: @facts, observed_at_ms: 10_000, expires_at_ms: 20_000}
      refute Map.has_key?(result, :ready)
    end

    assert {:ok, data.envelope} == Envelope.sign(@seed, @facts, @nonce, data.fingerprint, 10_000, 20_000)
    assert {:error, _} = Envelope.verify(data.envelope, data.public, data.fingerprint, @nonce, 20_000)
    assert {:error, _} = Envelope.verify(data.envelope, data.public, data.fingerprint, @nonce, 9_999)
  end

  test "key purpose nonce and every signed field substitution deny", data do
    for {key, value} <- [
          {"version", 2},
          {"purpose", "work-package-recovery"},
          {"issuer_fingerprint", String.duplicate("0", 64)},
          {"nonce", Base.url_encode64(:binary.copy(<<3>>, 32), padding: false)},
          {"facts", Base.url_encode64(@facts <> " ", padding: false)},
          {"observed_at_ms", 9_999},
          {"expires_at_ms", 20_001},
          {"signature", Base.url_encode64(:binary.copy(<<0>>, 64), padding: false)}
        ] do
      assert {:error, _} = Envelope.verify(Map.put(data.envelope, key, value), data.public, data.fingerprint, @nonce, 10_000)
    end

    {other, _} = :crypto.generate_key(:eddsa, :ed25519, :binary.copy(<<4>>, 32))
    assert {:error, _} = Envelope.verify(data.envelope, other, data.fingerprint, @nonce, 10_000)
    assert {:error, _} = Envelope.sign(@seed, @facts, @nonce, String.duplicate("0", 64), 10_000, 20_000)
  end

  test "schemas noncanonical encoding lengths and unsafe clocks fail closed", data do
    for invalid <- [
          nil,
          %{},
          Map.put(data.envelope, "extra", true),
          Map.delete(data.envelope, "signature"),
          Map.put(data.envelope, "facts", nil),
          Map.put(data.envelope, "nonce", data.envelope["nonce"] <> "="),
          Map.put(data.envelope, "signature", data.envelope["signature"] <> "="),
          Map.put(data.envelope, "facts", ""),
          Map.put(data.envelope, "observed_at_ms", 10_000.0),
          Map.put(data.envelope, "expires_at_ms", 9_007_199_254_740_992)
        ] do
      assert {:error, _} = Envelope.verify(invalid, data.public, data.fingerprint, @nonce, 10_000)
    end

    for {observed, expires} <- [{-1, 100}, {0, 0}, {10_000, 9_999}, {10_000, 70_001}, {0, 9_007_199_254_740_992}] do
      assert {:error, _} = Envelope.sign(@seed, @facts, @nonce, data.fingerprint, observed, expires)
    end

    assert {:error, _} = Envelope.sign("short", @facts, @nonce, data.fingerprint, 10_000, 20_000)
    assert {:error, _} = Envelope.sign(@seed, String.duplicate("x", 262_145), @nonce, data.fingerprint, 10_000, 20_000)
    assert {:error, _} = Envelope.sign(@seed, "", @nonce, data.fingerprint, 10_000, 20_000)
  end

  test "noncanonical trailing bits and bounded wire decoding deny before authentication", data do
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

    for key <- ["nonce", "facts", "signature"] do
      wire = data.envelope[key]
      {index, 1} = :binary.match(alphabet, <<:binary.last(wire)>>)
      alias_encoding = binary_part(wire, 0, byte_size(wire) - 1) <> binary_part(alphabet, index + 1, 1)
      assert {:error, _} = Envelope.verify(Map.put(data.envelope, key, alias_encoding), data.public, data.fingerprint, @nonce, 10_000)

      for invalid <- ["A", "abcde", "!", <<255>>, 7, []] do
        assert {:error, _} = Envelope.verify(Map.put(data.envelope, key, invalid), data.public, data.fingerprint, @nonce, 10_000)
      end
    end

    for {key, size} <- [{"nonce", 45}, {"signature", 89}, {"facts", 349_529}] do
      assert {:error, _} = Envelope.verify(Map.put(data.envelope, key, String.duplicate("A", size)), data.public, data.fingerprint, @nonce, 10_000)
    end

    assert {:error, _} = Envelope.verify(%{version: 1}, data.public, data.fingerprint, @nonce, 10_000)
    assert {:error, _} = Envelope.verify(data.envelope, data.public, String.upcase(data.fingerprint), @nonce, 10_000)
    assert {:error, _} = Envelope.verify(data.envelope, data.public, data.fingerprint, :binary.copy(<<3>>, 32), 10_000)
  end

  test "exact maximum facts and TTL remain valid while independent bindings are mandatory", data do
    facts = String.duplicate("x", 262_144)
    assert {:ok, envelope} = Envelope.sign(@seed, facts, @nonce, data.fingerprint, 0, 60_000)
    assert {:ok, %{facts: ^facts}} = Envelope.verify(envelope, data.public, data.fingerprint, @nonce, 59_999)
    assert {:error, _} = Envelope.verify(envelope, data.public, data.fingerprint, @nonce, 60_000)
    assert {:error, _} = Envelope.verify(envelope, nil, data.fingerprint, @nonce, 0)
    assert {:error, _} = Envelope.verify(envelope, data.public, nil, @nonce, 0)
    assert {:error, _} = Envelope.verify(envelope, data.public, data.fingerprint, @nonce, 9_007_199_254_740_992)
  end
end
