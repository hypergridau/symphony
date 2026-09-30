defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryContext do
  @moduledoc false

  @enforce_keys [:issue_id, :pool, :nonce, :workflow_path, :runtime, :host_ops]
  defstruct [:issue_id, :pool, :nonce, :workflow_path, :runtime, :host_ops]

  @type runtime :: %{
          required(:pool_key) => String.t(),
          required(:journal_path) => Path.t(),
          required(:execution_fence_path) => Path.t(),
          required(:responsibility_graph_path) => Path.t()
        }

  @type host_ops :: %{atom() => function()}

  @type t :: %__MODULE__{
          issue_id: String.t(),
          pool: String.t(),
          nonce: String.t(),
          workflow_path: Path.t(),
          runtime: runtime(),
          host_ops: host_ops()
        }
end
