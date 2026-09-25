Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.WorkPackageRuntimeTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture
  alias SymphonyElixir.WorkPackageRuntime

  @required %{
    "DAHLIA_WORK_PACKAGE_PROVIDER_URL" => "https://provider.example",
    "DAHLIA_WORK_PACKAGE_RUNNER_TOKEN" => "runner-token",
    "DAHLIA_WORK_PACKAGE_ATTESTATION_KEY" => "attestation-key",
    "DAHLIA_RUNNER_ID" => "runner-350",
    "DAHLIA_MANAGED_PROJECT_PROFILE_ID" => "profile-350"
  }

  test "managed runtime stays disabled when no provider tuple is configured" do
    assert :disabled = WorkPackageRuntime.configuration(env: %{})
  end

  test "managed runtime rejects a partial provider tuple" do
    assert {:error, {:incomplete_work_package_runtime, missing}} =
             WorkPackageRuntime.configuration(env: Map.delete(@required, "DAHLIA_WORK_PACKAGE_ATTESTATION_KEY"))

    assert missing == ["DAHLIA_WORK_PACKAGE_ATTESTATION_KEY"]
  end

  test "declared managed pools cannot omit both manifest settings" do
    env = Map.put(@required, "SYMPHONY_POOL_KEY", "managed-pool")
    assert {:error, :managed_delegation_manifest_required} = WorkPackageRuntime.configuration(env: env)
  end

  test "managed runtime builds trusted callbacks and host-only paths" do
    env =
      Map.merge(@required, %{
        "DAHLIA_WORK_PACKAGE_JOURNAL_PATH" => "tmp/runner-journal.json",
        "DAHLIA_WORK_PACKAGE_ARCHIVE_ROOT" => "tmp/runner-archives"
      })

    assert {:ok, runtime} = WorkPackageRuntime.configuration(env: env)
    assert runtime.base_url == "https://provider.example"
    assert runtime.runner_token == "runner-token"
    assert runtime.attestation_key == "attestation-key"
    assert runtime.runner_id == "runner-350"
    assert runtime.managed_project_profile_id == "profile-350"
    assert runtime.managed_delegations == nil
    refute Map.has_key?(runtime, :assignment_context)
    assert runtime.journal_path == Path.expand("tmp/runner-journal.json")
    assert runtime.archive_root == Path.expand("tmp/runner-archives")
    assert is_function(runtime.cleanup_prepare_fun, 4)
    assert is_function(runtime.cleanup_evidence_fun, 3)

    assert "DAHLIA_WORK_PACKAGE_RUNNER_TOKEN" in runtime.secret_environment_names
    assert "DAHLIA_WORK_PACKAGE_ATTESTATION_KEY" in runtime.secret_environment_names
  end

  @tag skip: System.get_env("SYMPHONY_TEST_ROOT_MANIFEST_FILES") != "1"
  test "managed runtime starts with a correctly signed empty v1 grant while admission is paused" do
    assert {:unix, :linux} = :os.type()
    assert {"0\n", 0} = System.cmd("id", ["-u"])

    root = Path.join(System.tmp_dir!(), "work-package-v1-empty-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    path = Path.join(root, "manifest.json")
    bytes = Jason.encode!(Fixture.payload_v1(System.system_time(:millisecond)) |> Map.put("entries", []))
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519)
    File.write!(path, bytes)
    File.chmod!(path, 0o644)

    env =
      Map.merge(@required, %{
        "SYMPHONY_POOL_KEY" => "test-pool",
        "SYMPHONY_REPOSITORY_REF" => "openai/symphony",
        "DAHLIA_MANAGED_DELEGATION_PATH" => path,
        "DAHLIA_MANAGED_DELEGATION_SHA256" => digest(bytes),
        "DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519" => sign(bytes, private_key),
        "DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519" => hex(public_key)
      })

    on_exit(fn -> File.rm_rf!(root) end)
    assert {:ok, %{managed_delegations: %{schema_version: 1, entries: []}}} = WorkPackageRuntime.configuration(env: env)
  end

  test "managed runtime rejects malformed provider and path settings" do
    assert {:error, :invalid_work_package_provider_url} =
             WorkPackageRuntime.configuration(env: Map.put(@required, "DAHLIA_WORK_PACKAGE_PROVIDER_URL", "provider.example"))

    assert {:error, {:invalid_work_package_path, "DAHLIA_WORK_PACKAGE_ARCHIVE_ROOT"}} =
             WorkPackageRuntime.configuration(
               env:
                 Map.merge(@required, %{
                   "DAHLIA_WORK_PACKAGE_ARCHIVE_ROOT" => "   "
                 })
             )
  end

  test "recovery configuration requires both an existing absolute directory and an Ed25519 public key" do
    {public_key, _private_key} = :crypto.generate_key(:eddsa, :ed25519)

    configured =
      Map.merge(@required, %{
        "DAHLIA_WORK_PACKAGE_RECOVERY_DIRECTORY" => System.tmp_dir!(),
        "DAHLIA_WORK_PACKAGE_RECOVERY_PUBLIC_KEY" => Base.url_encode64(public_key, padding: false)
      })

    assert {:ok, %{claim_recovery: %{public_key: ^public_key}}} = WorkPackageRuntime.configuration(env: configured)

    assert {:error, :incomplete_claim_recovery_configuration} =
             WorkPackageRuntime.configuration(env: Map.delete(configured, "DAHLIA_WORK_PACKAGE_RECOVERY_PUBLIC_KEY"))

    assert {:error, :invalid_claim_recovery_configuration} =
             WorkPackageRuntime.configuration(env: Map.put(configured, "DAHLIA_WORK_PACKAGE_RECOVERY_DIRECTORY", "relative"))

    assert {:error, :invalid_claim_recovery_configuration} =
             WorkPackageRuntime.configuration(env: Map.put(configured, "DAHLIA_WORK_PACKAGE_RECOVERY_PUBLIC_KEY", "invalid"))
  end

  test "application supervisor supplies managed options to the real orchestrator child" do
    previous =
      Enum.map(@required, fn {name, value} ->
        {name, System.get_env(name)}
        |> then(fn {key, old_value} ->
          System.put_env(key, value)
          {key, old_value}
        end)
      end)

    on_exit(fn ->
      Enum.each(previous, fn {name, value} ->
        if is_binary(value), do: System.put_env(name, value), else: System.delete_env(name)
      end)
    end)

    {:ok, {_supervisor_flags, children}} = SymphonyElixir.AgentRuntimeSupervisor.init([])

    orchestrator_child =
      Enum.find(children, fn child ->
        child.id == SymphonyElixir.Orchestrator
      end)

    assert {SymphonyElixir.Orchestrator, :start_link, [orchestrator_opts]} = orchestrator_child.start
    assert orchestrator_opts[:execution_supervisor] == :systemd_user
    assert is_map(orchestrator_opts[:work_package_runtime])
    assert is_function(orchestrator_opts[:work_package_runtime].cleanup_prepare_fun, 4)
  end

  test "declared repository pools cannot silently use the legacy nil runtime" do
    previous =
      [
        {"SYMPHONY_POOL_KEY", System.get_env("SYMPHONY_POOL_KEY")},
        {"SYMPHONY_REPOSITORY_REF", System.get_env("SYMPHONY_REPOSITORY_REF")}
        | Enum.map(@required, fn {name, _value} -> {name, System.get_env(name)} end)
      ]

    Enum.each(@required, fn {name, _value} -> System.delete_env(name) end)
    System.put_env("SYMPHONY_POOL_KEY", "managed-pool")

    on_exit(fn ->
      Enum.each(previous, fn {name, value} ->
        if is_binary(value), do: System.put_env(name, value), else: System.delete_env(name)
      end)
    end)

    assert_raise ArgumentError, ~r/managed Symphony pool work-package runtime is not configured/, fn ->
      SymphonyElixir.AgentRuntimeSupervisor.init([])
    end
  end

  defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
  defp hex(bytes), do: Base.encode16(bytes, case: :lower)

  defp sign(bytes, private_key) do
    :crypto.sign(:eddsa, :none, "hypergrid.symphony.managed-delegation.v1\0" <> bytes, [private_key, :ed25519])
    |> hex()
  end
end
