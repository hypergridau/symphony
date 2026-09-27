# Disposable Symphony worker image

The image packages the fixed `/usr/local/bin/symphony-worker` entrypoint, the Elixir production escript, Git, and the pinned Codex CLI. `package-lock.json` records the npm package integrity for `@openai/codex` 0.157.1. The task Job must still select the resulting image by digest in trusted host configuration.

Build locally from the repository root:

```sh
docker build -f elixir/worker-image/Dockerfile -t symphony-worker:local .
```

The `disposable-worker-image` workflow builds and publishes `ghcr.io/hypergridau/symphony-worker:sha-<commit>` on eligible pushes to `main` and prints the registry digest in its run summary. This source change does not publish or deploy an image. No image digest has been qualified in an RKE2 Job.

The runner invokes `codex exec` with the `workspace-write` sandbox, GPT-6 Luna, high reasoning effort, and ephemeral session storage. The cached OAuth home is mounted for the Codex client; the active Job is its trust boundary. The checkout token is only passed to the short-lived Git clone process through its environment and is never placed in the clone URL or command arguments.
