defmodule SymphonyElixir.RetainedGrantProof.NonceRequestTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RetainedGrantProof.NonceRequest

  test "decodes exact nonce bytes without interpreting them as authority" do
    for value <- 0..255 do
      nonce = :binary.copy(<<value>>, 32)
      assert {:ok, ^nonce} = NonceRequest.decode(Base.url_encode64(nonce, padding: false))
    end
  end

  test "rejects padding, aliases, concatenation, whitespace and arbitrary request shapes" do
    canonical = Base.url_encode64(<<0::256>>, padding: false)

    for invalid <- [canonical <> "=", canonical <> "\n", " " <> canonical, canonical <> canonical,
                    String.slice(canonical, 0, 42) <> "B", String.duplicate("A", 42), String.duplicate("A", 44),
                    Jason.encode!(%{nonce: canonical}), %{nonce: canonical}, [canonical], nil,
                    String.pad_trailing("/etc/issuer-key", 43), String.duplicate("!", 43), <<255>> <> String.duplicate("A", 42)] do
      assert {:error, :invalid_retained_nonce_request} = NonceRequest.decode(invalid)
    end
  end

  test "rejects caller-provided path, grant, key, counter and clock overrides" do
    nonce = Base.url_encode64(<<0::256>>, padding: false)

    for key <- ~w(path grant key digest counter observed_at_ms expires_at_ms command issue_id) do
      assert {:error, :invalid_retained_nonce_request} = NonceRequest.decode(%{"nonce" => nonce, key => "override"})
      assert {:error, :invalid_retained_nonce_request} = NonceRequest.decode(nonce <> ":" <> key)
    end
  end
end
