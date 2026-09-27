defmodule SymphonyElixir.RKE2JobHostClientContextTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RKE2Job.HostClientContext

  setup do
    root = Path.join(System.tmp_dir!(), "frigga-host-context-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    File.chmod!(root, 0o700)
    File.write!(Path.join(root, "kubernetes-ca.crt"), "synthetic-ca")
    File.chmod!(Path.join(root, "kubernetes-ca.crt"), 0o644)
    token_file = Path.join(root, "kubernetes-api.token")
    File.write!(token_file, "synthetic.jwt.token\n")
    File.chmod!(token_file, 0o640)
    on_exit(fn -> File.rm_rf!(root) end)

    digest = String.duplicate("a", 64)

    %{
      root: root,
      token_file: token_file,
      assignment: %{sha256: digest, environment: %{target_environment: :rke2}},
      key: digest <> ":allocate",
      config: %{credential_root: root, api_server: "https://10.0.14.10:6443"}
    }
  end

  test "reads the current root-controlled token for each exact operation", context do
    assert {:ok, first} =
             HostClientContext.client_context(
               context.assignment,
               :allocate,
               context.key,
               context.config
             )

    assert first == %{
             api_server: "https://10.0.14.10:6443",
             namespace: "frigga",
             bearer_token: "synthetic.jwt.token",
             ca_certfile: Path.join(context.root, "kubernetes-ca.crt"),
             timeout_ms: 10_000
           }

    replacement = context.token_file <> ".next"
    File.write!(replacement, "rotated.jwt.token\n")
    File.chmod!(replacement, 0o640)
    File.rename!(replacement, context.token_file)

    assert {:ok, %{bearer_token: "rotated.jwt.token"}} =
             HostClientContext.client_context(context.assignment, :allocate, context.key, context.config)
  end

  test "holds an altered assignment or operation key before reading credentials", context do
    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(context.assignment, :activate, context.key, context.config)

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(
               put_in(context.assignment.environment.target_environment, :lke),
               :allocate,
               context.key,
               context.config
             )
  end

  test "holds unsafe token custody and malformed token bytes", context do
    File.chmod!(context.token_file, 0o660)

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(context.assignment, :allocate, context.key, context.config)

    File.chmod!(context.token_file, 0o640)
    File.write!(context.token_file, "token with spaces")

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(context.assignment, :allocate, context.key, context.config)

    File.rm!(context.token_file)
    File.ln_s!(Path.join(context.root, "kubernetes-ca.crt"), context.token_file)

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(context.assignment, :allocate, context.key, context.config)
  end
end
