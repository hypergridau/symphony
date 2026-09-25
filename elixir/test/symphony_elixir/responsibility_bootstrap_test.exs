defmodule SymphonyElixir.ResponsibilityBootstrapTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{CLI, Orchestrator, ResponsibilityBootstrap, Workflow}

  @ack_flag "--i-understand-that-this-will-be-running-without-the-usual-guardrails"
  @activate_flag "--activate-responsibility-graph"

  @managed_environment [
    "SYMPHONY_POOL_KEY",
    "SYMPHONY_REPOSITORY_REF",
    "DAHLIA_WORK_PACKAGE_PROVIDER_URL",
    "DAHLIA_WORK_PACKAGE_RUNNER_TOKEN",
    "DAHLIA_WORK_PACKAGE_ATTESTATION_KEY",
    "DAHLIA_RUNNER_ID",
    "DAHLIA_MANAGED_PROJECT_PROFILE_ID",
    "DAHLIA_MANAGED_DELEGATION_PATH",
    "DAHLIA_MANAGED_DELEGATION_SHA256",
    "DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519",
    "DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519",
    "DAHLIA_WORK_PACKAGE_JOURNAL_PATH",
    "DAHLIA_WORK_PACKAGE_ARCHIVE_ROOT",
    "SYMPHONY_GLOBAL_PAUSE_FILE"
  ]

  test "requires a managed pool before local activation" do
    previous = save_environment(@managed_environment)
    on_exit(fn -> restore_environment(previous) end)
    Enum.each(@managed_environment, &System.delete_env/1)

    assert {:error, :managed_pool_required} = ResponsibilityBootstrap.activate(1)
  end

  @tag skip: System.get_env("SYMPHONY_TEST_ROOT_MANIFEST_FILES") != "1"
  test "requires an explicitly paused global mutable gate" do
    test_root = Path.join(System.tmp_dir!(), "symphony-responsibility-bootstrap-#{System.unique_integer([:positive])}")
    pause_path = Path.join(test_root, "global-mutable-pause.state")
    previous = save_environment(@managed_environment)

    on_exit(fn ->
      restore_environment(previous)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(test_root)
    File.write!(pause_path, "running\n")
    install_manifest!(test_root)

    Enum.each(
      %{
        "SYMPHONY_POOL_KEY" => "pool-bootstrap",
        "SYMPHONY_REPOSITORY_REF" => "example/repository",
        "DAHLIA_WORK_PACKAGE_PROVIDER_URL" => "https://provider.example",
        "DAHLIA_WORK_PACKAGE_RUNNER_TOKEN" => "public-runner-token",
        "DAHLIA_WORK_PACKAGE_ATTESTATION_KEY" => "public-attestation-key",
        "DAHLIA_RUNNER_ID" => "runner-bootstrap",
        "DAHLIA_MANAGED_PROJECT_PROFILE_ID" => "profile-bootstrap",
        "SYMPHONY_GLOBAL_PAUSE_FILE" => pause_path
      },
      fn {key, value} -> System.put_env(key, value) end
    )

    assert {:error, :global_pause_not_paused} = ResponsibilityBootstrap.activate(1)
  end

  @tag skip: System.get_env("SYMPHONY_TEST_ROOT_MANIFEST_FILES") != "1"
  test "CLI activation persists through an application restart" do
    test_root =
      Path.join(System.tmp_dir!(), "symphony-responsibility-cli-#{System.unique_integer([:positive])}")

    workflow_path = Path.join(test_root, "WORKFLOW.md")
    pause_path = Path.join(test_root, "global-mutable-pause.state")
    journal_path = Path.join(test_root, "work-package.json")
    archive_root = Path.join(test_root, "cleanup-archives")
    previous_workflow_path = Workflow.workflow_file_path()
    previous_environment = save_environment(@managed_environment)
    previous_memory_tracker_issues = Application.get_env(:symphony_elixir, :memory_tracker_issues)
    application_was_started? = application_started?()

    on_exit(fn ->
      if application_started?() do
        _ = Application.stop(:symphony_elixir)
      end

      Workflow.set_workflow_file_path(previous_workflow_path)
      restore_environment(previous_environment)
      restore_memory_tracker_issues(previous_memory_tracker_issues)

      if application_was_started? do
        _ = Application.ensure_all_started(:symphony_elixir)
      end

      File.rm_rf(test_root)
    end)

    File.mkdir_p!(test_root)
    File.write!(pause_path, "paused\n")
    install_manifest!(test_root)

    write_workflow_file!(workflow_path,
      tracker_kind: "memory",
      workspace_root: test_root,
      poll_interval_ms: 30_000
    )

    Enum.each(
      %{
        "SYMPHONY_POOL_KEY" => "pool-bootstrap",
        "SYMPHONY_REPOSITORY_REF" => "example/repository",
        "DAHLIA_WORK_PACKAGE_PROVIDER_URL" => "https://provider.example",
        "DAHLIA_WORK_PACKAGE_RUNNER_TOKEN" => "public-runner-token",
        "DAHLIA_WORK_PACKAGE_ATTESTATION_KEY" => "public-attestation-key",
        "DAHLIA_RUNNER_ID" => "runner-bootstrap",
        "DAHLIA_MANAGED_PROJECT_PROFILE_ID" => "profile-bootstrap",
        "DAHLIA_WORK_PACKAGE_JOURNAL_PATH" => journal_path,
        "DAHLIA_WORK_PACKAGE_ARCHIVE_ROOT" => archive_root,
        "SYMPHONY_GLOBAL_PAUSE_FILE" => pause_path
      },
      fn {key, value} -> System.put_env(key, value) end
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    :ok = Application.stop(:symphony_elixir)

    deps = %{
      file_regular?: &File.regular?/1,
      set_workflow_file_path: &Workflow.set_workflow_file_path/1,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> Application.ensure_all_started(:symphony_elixir) end,
      activate_responsibility_graph: &ResponsibilityBootstrap.activate/1
    }

    assert :ok = CLI.evaluate([@ack_flag, @activate_flag, workflow_path], deps)
    assert %{responsibility_graph: %{enforcement: :enforced}} = Orchestrator.snapshot()

    :ok = Application.stop(:symphony_elixir)
    assert {:ok, _started} = Application.ensure_all_started(:symphony_elixir)
    assert %{responsibility_graph: %{enforcement: :enforced}} = Orchestrator.snapshot()

    assert :ok = CLI.evaluate([@ack_flag, @activate_flag, workflow_path], deps)
  end

  defp install_manifest!(root) do
    assert {"0\n", 0} = System.cmd("id", ["-u"])
    path = Path.join(root, "managed-delegations.json")

    payload = %{
      schema_version: 1,
      pool_key: "pool-bootstrap",
      repository_ref: "example/repository",
      managed_project_profile_id: "profile-bootstrap",
      authority_ref: "test:bootstrap",
      entries: []
    }

    bytes = Jason.encode!(payload)
    File.write!(path, bytes)
    File.chmod!(path, 0o644)
    digest = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519)
    signature = :crypto.sign(:eddsa, :none, "hypergrid.symphony.managed-delegation.v1\0" <> bytes, [private_key, :ed25519])
    System.put_env("DAHLIA_MANAGED_DELEGATION_PATH", path)
    System.put_env("DAHLIA_MANAGED_DELEGATION_SHA256", digest)
    System.put_env("DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519", Base.encode16(signature, case: :lower))
    System.put_env("DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519", Base.encode16(public_key, case: :lower))
  end

  defp save_environment(keys), do: Map.new(keys, &{&1, System.get_env(&1)})

  defp application_started? do
    Enum.any?(Application.started_applications(), fn {app, _description, _version} ->
      app == :symphony_elixir
    end)
  end

  defp restore_memory_tracker_issues(nil), do: Application.delete_env(:symphony_elixir, :memory_tracker_issues)

  defp restore_memory_tracker_issues(value),
    do: Application.put_env(:symphony_elixir, :memory_tracker_issues, value)

  defp restore_environment(environment) do
    Enum.each(environment, fn
      {key, nil} -> System.delete_env(key)
      {key, value} -> System.put_env(key, value)
    end)
  end
end
