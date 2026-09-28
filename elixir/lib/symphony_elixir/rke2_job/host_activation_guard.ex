defmodule SymphonyElixir.RKE2Job.HostActivationGuard do
  @moduledoc """
  Final host-owned admission check for one already journaled RKE2 Job activation.

  The Orchestrator calls this synchronously inside its serialized spawn callback.
  The root pause setter waits for that callback before acknowledging a pause.
  """

  @behaviour SymphonyElixir.RKE2Job.ActivationGuard

  alias SymphonyElixir.{GlobalPause, ManagedAssignmentBundle, WorkPackageClaim}

  @impl true
  def authorize(assignment, %{id: allocation_id, status: :ready}, key, context)
      when is_map(assignment) and is_binary(allocation_id) and is_binary(key) and is_map(context) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         true <- key == assignment.sha256 <> ":activate",
         %{configured?: true, paused?: false, state: "running"} <- GlobalPause.snapshot(),
         %{claim_input: input, claim_binding: binding} when is_map(input) and is_map(binding) <- context,
         true <- exact_binding?(assignment, input, binding),
         {:ok, %{phase: "spawn_started", allocation_id: ^allocation_id}} <- WorkPackageClaim.handoff_allocation(input),
         :ok <- WorkPackageClaim.replay_spawn_intent(input) do
      :ok
    else
      _ -> {:held, :rke2_host_activation_unverified}
    end
  rescue
    _ -> {:held, :rke2_host_activation_unverified}
  end

  def authorize(_assignment, _allocation, _key, _context),
    do: {:held, :rke2_host_activation_unverified}

  defp exact_binding?(assignment, input, binding) do
    lease = assignment.lease

    input.issue_id == lease.issue_id and input.repository_ref == assignment.repository_ref and
      input.runner_id == binding.runner_id and input.issue_id == binding.issue_id and
      input.managed_project_profile_id == binding.managed_project_profile_id and
      binding.repository_ref == assignment.repository_ref and binding.generation == lease.generation and
      binding.session_id == lease.session_id and binding.process_id == lease.process_id
  end
end
