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
      key: digest <> ":allocation",
      config: %{
        credential_root: root,
        api_server: "https://10.0.14.10:6443",
        test_owner_uid: File.stat!(root).uid
      }
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
             HostClientContext.client_context(
               context.assignment,
               :allocate,
               context.assignment.sha256 <> ":allocate",
               context.config
             )

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

  test "uses the adapter's shared abort key for prepare and confirm", context do
    key = context.assignment.sha256 <> ":abort_unstarted"
    assert {:ok, _} = HostClientContext.client_context(context.assignment, :abort_prepare, key, context.config)
    assert {:ok, _} = HostClientContext.client_context(context.assignment, :abort_confirm, key, context.config)

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(
               context.assignment,
               :abort_prepare,
               context.assignment.sha256 <> ":abort_prepare",
               context.config
             )
  end

  test "accepts each adapter operation key and rejects a different phase", context do
    operations = [
      allocate: "allocation",
      activate: "activate",
      delete: "delete",
      abort_prepare: "abort_unstarted",
      abort_confirm: "abort_unstarted",
      finalize: "finalize"
    ]

    for {operation, suffix} <- operations do
      key = context.assignment.sha256 <> ":" <> suffix
      assert {:ok, _} = HostClientContext.client_context(context.assignment, operation, key, context.config)

      assert {:error, :rke2_host_client_context_unavailable} =
               HostClientContext.client_context(
                 context.assignment,
                 operation,
                 context.assignment.sha256 <> ":wrong",
                 context.config
               )
    end
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

  test "holds a missing or oversized token after a rotation", context do
    File.rm!(context.token_file)

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(context.assignment, :allocate, context.key, context.config)

    File.write!(context.token_file, String.duplicate("a", 16_385))
    File.chmod!(context.token_file, 0o640)

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(context.assignment, :allocate, context.key, context.config)
  end

  test "holds a writable credential root or unsafe CA file", context do
    File.chmod!(context.root, 0o720)

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(context.assignment, :allocate, context.key, context.config)

    File.chmod!(context.root, 0o700)
    File.chmod!(Path.join(context.root, "kubernetes-ca.crt"), 0o666)

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(context.assignment, :allocate, context.key, context.config)
  end

  test "holds an invalid timeout or missing API endpoint", context do
    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(
               context.assignment,
               :allocate,
               context.key,
               Map.put(context.config, :timeout_ms, 30_001)
             )

    assert {:error, :rke2_host_client_context_unavailable} =
             HostClientContext.client_context(
               context.assignment,
               :allocate,
               context.key,
               Map.delete(context.config, :api_server)
             )
  end
end
