defmodule SymphonyElixir.RetainedGrantProof.ChallengeEnvelopeTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RetainedGrantProof.ChallengeEnvelope

  @purpose "hypergrid.retained-grant-challenge.v1"
  @retained_proof_purpose "hypergrid.retained-grant-evidence.v1"
  @recovery_purpose "hypergrid.retained-grant-recovery.v1"
  @seed :binary.copy(<<0x42>>, 32)
  @nonce :binary.copy(<<0x17>>, 32)
  @facts ~s({"schema_version":1,"challenge":"opaque"})
  @observed_at_ms 1_700_000_000_000
  @expires_at_ms @observed_at_ms + 60_000
  @trusted_now_ms @observed_at_ms + 1_000

  setup do
    {public, private} = :crypto.generate_key(:eddsa, :ed25519, @seed)

    %{
      public: public,
      private: private,
      fingerprint: Base.encode16(:crypto.hash(:sha256, public), case: :lower),
      nonce: @nonce
    }
  end

  test "accepts opaque facts and re-verifies the same unexpired envelope read-only", context do
    envelope = valid_envelope(context)

    assert {:ok, result} =
             ChallengeEnvelope.verify(
               envelope,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    assert result == %{
             facts: @facts,
             observed_at_ms: @observed_at_ms,
             expires_at_ms: @expires_at_ms
           }

    assert {:ok, ^result} =
             ChallengeEnvelope.verify(
               envelope,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )
  end

  test "rejects retained-proof and recovery purposes", context do
    for purpose <- [@retained_proof_purpose, @recovery_purpose] do
      assert_invalid(valid_envelope(context, purpose: purpose), context)
    end
  end

  test "rejects wrong public key with its matching fingerprint", context do
    {wrong_public, _wrong_private} =
      :crypto.generate_key(:eddsa, :ed25519, :binary.copy(<<0x24>>, 32))

    wrong_fingerprint = Base.encode16(:crypto.hash(:sha256, wrong_public), case: :lower)
    envelope = Map.put(valid_envelope(context), "issuer_fingerprint", wrong_fingerprint)
    assert_invalid(envelope, context, public: wrong_public, fingerprint: wrong_fingerprint)
  end

  test "rejects wrong installed fingerprint", context do
    assert_invalid(valid_envelope(context), context, fingerprint: String.duplicate("0", 64))
  end

  test "rejects wrong or swapped nonce", context do
    wrong_nonce = :binary.copy(<<0x31>>, 32)
    assert_invalid(valid_envelope(context, nonce: wrong_nonce), context)

    assert ChallengeEnvelope.verify(
             valid_envelope(context),
             context.public,
             context.fingerprint,
             wrong_nonce,
             @trusted_now_ms
           ) == {:error, :retained_challenge_envelope_invalid}
  end

  test "rejects malformed envelope shapes", context do
    assert_invalid_shape(nil, context)
    assert_invalid_shape([], context)
    assert_invalid_shape(%{"version" => 1}, context)
    assert_invalid_shape(Map.put(valid_envelope(context), :version, 1), context)
    assert_invalid_shape(Map.put(valid_envelope(context), "extra", "denied"), context)
  end

  test "rejects wrong version and noncanonical base64url values", context do
    assert_invalid(valid_envelope(context, version: 1.0), context)
    envelope = valid_envelope(context)
    assert_invalid(Map.put(envelope, "nonce", Base.url_encode64(context.nonce)), context)

    padded_signature =
      envelope
      |> Map.fetch!("signature")
      |> Base.url_decode64!(padding: false)
      |> Base.url_encode64()

    assert_invalid(Map.put(envelope, "signature", padded_signature), context)
    padded_facts = Base.url_encode64(<<0>>)
    assert String.ends_with?(padded_facts, "=")
    assert_invalid(Map.put(envelope, "facts", padded_facts), context)
  end

  test "rejects altered facts and deterministic signature corruption", context do
    envelope = valid_envelope(context)

    assert_invalid(
      Map.put(envelope, "facts", Base.url_encode64("altered facts", padding: false)),
      context
    )

    raw_signature = envelope |> Map.fetch!("signature") |> Base.url_decode64!(padding: false)
    <<first, rest::binary>> = raw_signature

    corrupted_signature =
      Base.url_encode64(<<Bitwise.bxor(first, 1), rest::binary>>, padding: false)

    assert_invalid(Map.put(envelope, "signature", corrupted_signature), context)
  end

  test "rejects empty, oversized, and encoded oversized nonce or signature values", context do
    assert_invalid(valid_envelope(context, facts: <<>>), context)
    oversized_facts = :binary.copy(<<0>>, 262_145)

    assert_invalid(
      Map.put(
        valid_envelope(context),
        "facts",
        Base.url_encode64(oversized_facts, padding: false)
      ),
      context
    )

    oversized_nonce = :binary.copy(<<1>>, 33)

    assert_invalid(
      Map.put(
        valid_envelope(context),
        "nonce",
        Base.url_encode64(oversized_nonce, padding: false)
      ),
      context
    )

    oversized_signature = :binary.copy(<<2>>, 65)

    assert_invalid(
      Map.put(
        valid_envelope(context),
        "signature",
        Base.url_encode64(oversized_signature, padding: false)
      ),
      context
    )
  end

  test "rejects invalid freshness and safe-integer values", context do
    assert_invalid(valid_envelope(context, observed_at_ms: @trusted_now_ms + 1), context)
    assert_invalid(valid_envelope(context, expires_at_ms: @observed_at_ms), context)
    assert_invalid(valid_envelope(context, expires_at_ms: @observed_at_ms - 1), context)
    assert_invalid(valid_envelope(context, expires_at_ms: @observed_at_ms + 60_001), context)
    assert_invalid(Map.put(valid_envelope(context), "observed_at_ms", -1), context)
    assert_invalid(valid_envelope(context, expires_at_ms: 9_007_199_254_740_992), context)
    assert_invalid(valid_envelope(context), context, trusted_now_ms: @expires_at_ms)
    assert_invalid(valid_envelope(context), context, trusted_now_ms: -1)
    assert_invalid(valid_envelope(context), context, trusted_now_ms: @trusted_now_ms * 1.0)
    assert_invalid(valid_envelope(context), context, trusted_now_ms: 9_007_199_254_740_992)
  end

  test "verify_with_digest returns the independently computed challenge digest", context do
    envelope = valid_envelope(context)

    assert {:ok, verified} =
             ChallengeEnvelope.verify_with_digest(
               envelope,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    expected_digest =
      :crypto.hash(
        :sha256,
        fixture_message(
          @purpose,
          context.public,
          context.nonce,
          @observed_at_ms,
          @expires_at_ms,
          @facts
        )
      )
      |> Base.encode16(case: :lower)

    assert verified.challenge_digest == expected_digest
  end

  test "verify/5 keeps its existing successful result shape", context do
    envelope = valid_envelope(context)

    assert {:ok, verified} =
             ChallengeEnvelope.verify(
               envelope,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    assert {:ok, verified_with_digest} =
             ChallengeEnvelope.verify_with_digest(
               envelope,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    assert Map.delete(verified_with_digest, :challenge_digest) == verified
  end

  test "repeated readonly verification returns the same digest", context do
    envelope = valid_envelope(context)

    assert {:ok, first} =
             ChallengeEnvelope.verify_with_digest(
               envelope,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    assert {:ok, second} =
             ChallengeEnvelope.verify_with_digest(
               envelope,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    assert second == first
  end

  test "rejects altered purpose, stale facts signature, and zero signature", context do
    assert {:error, :retained_challenge_envelope_invalid} =
             ChallengeEnvelope.verify_with_digest(
               valid_envelope(context, purpose: "hypergrid.invalid-purpose.v1"),
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    original = valid_envelope(context)
    resigned_facts = valid_envelope(context, facts: "changed binary")
    stale_facts = Map.put(resigned_facts, "signature", Map.fetch!(original, "signature"))

    assert {:error, :retained_challenge_envelope_invalid} =
             ChallengeEnvelope.verify_with_digest(
               stale_facts,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    zero_signature =
      Map.put(original, "signature", Base.url_encode64(:binary.copy(<<0>>, 64), padding: false))

    assert {:error, :retained_challenge_envelope_invalid} =
             ChallengeEnvelope.verify_with_digest(
               zero_signature,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )
  end

  test "correctly resigned facts and times produce different digests", context do
    original = valid_envelope(context)

    assert {:ok, original_verified} =
             ChallengeEnvelope.verify_with_digest(
               original,
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    assert {:ok, changed_facts} =
             ChallengeEnvelope.verify_with_digest(
               valid_envelope(context, facts: "changed binary"),
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    assert changed_facts.challenge_digest != original_verified.challenge_digest

    assert {:ok, changed_times} =
             ChallengeEnvelope.verify_with_digest(
               valid_envelope(context,
                 observed_at_ms: @observed_at_ms - 1,
                 expires_at_ms: @expires_at_ms - 1
               ),
               context.public,
               context.fingerprint,
               context.nonce,
               @trusted_now_ms
             )

    assert changed_times.challenge_digest != original_verified.challenge_digest
  end

  defp valid_envelope(context, overrides \\ []) do
    version = Keyword.get(overrides, :version, 1)
    purpose = Keyword.get(overrides, :purpose, @purpose)
    nonce = Keyword.get(overrides, :nonce, context.nonce)
    observed_at_ms = Keyword.get(overrides, :observed_at_ms, @observed_at_ms)
    expires_at_ms = Keyword.get(overrides, :expires_at_ms, @expires_at_ms)
    facts = Keyword.get(overrides, :facts, @facts)

    message =
      fixture_message(purpose, context.public, nonce, observed_at_ms, expires_at_ms, facts)

    signature =
      :crypto.sign(:eddsa, :none, message, [context.private, :ed25519])
      |> Base.url_encode64(padding: false)

    %{
      "version" => version,
      "purpose" => purpose,
      "issuer_fingerprint" => context.fingerprint,
      "nonce" => Base.url_encode64(nonce, padding: false),
      "observed_at_ms" => observed_at_ms,
      "expires_at_ms" => expires_at_ms,
      "facts" => Base.url_encode64(facts, padding: false),
      "signature" => signature
    }
  end

  defp assert_invalid(envelope, context, overrides \\ []) do
    public = Keyword.get(overrides, :public, context.public)
    fingerprint = Keyword.get(overrides, :fingerprint, context.fingerprint)
    nonce = Keyword.get(overrides, :nonce, context.nonce)
    trusted_now_ms = Keyword.get(overrides, :trusted_now_ms, @trusted_now_ms)

    assert ChallengeEnvelope.verify(envelope, public, fingerprint, nonce, trusted_now_ms) ==
             {:error, :retained_challenge_envelope_invalid}
  end

  defp assert_invalid_shape(envelope, context) do
    assert ChallengeEnvelope.verify(
             envelope,
             context.public,
             context.fingerprint,
             context.nonce,
             @trusted_now_ms
           ) == {:error, :retained_challenge_envelope_invalid}
  end

  defp fixture_message(purpose, public, nonce, observed_at_ms, expires_at_ms, facts) do
    purpose <>
      <<0>> <>
      public <>
      nonce <>
      <<observed_at_ms::unsigned-big-64>> <>
      <<expires_at_ms::unsigned-big-64>> <>
      :crypto.hash(:sha256, facts)
  end
end
