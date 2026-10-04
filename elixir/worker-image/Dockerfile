FROM node:22.19.0-bookworm-slim@sha256:4a4884e8a44826194dff92ba316264f392056cbe243dcc9fd3551e71cea02b90 AS codex
WORKDIR /opt/codex
COPY elixir/worker-image/package.json elixir/worker-image/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

FROM elixir:1.19.5-otp-28-slim@sha256:597474ac81f0d0bfa4d67546c2591a033315a2b9e6ab2f1a22824af9c913066a AS build
WORKDIR /src/elixir
COPY elixir/ .
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && mix local.hex --force \
 && mix local.rebar --force \
 && MIX_ENV=prod mix deps.get \
 && MIX_ENV=prod mix escript.build

FROM elixir:1.19.5-otp-28-slim@sha256:597474ac81f0d0bfa4d67546c2591a033315a2b9e6ab2f1a22824af9c913066a AS worker
ENV HOME=/tmp \
    NPM_CONFIG_CACHE=/tmp/npm-cache \
    PATH=/opt/codex/node_modules/.bin:/usr/local/bin:/usr/bin:/bin
RUN apt-get update \
 && apt-get install -y --no-install-recommends acl ca-certificates git libstdc++6 passwd \
 && rm -rf /var/lib/apt/lists/* \
 && /usr/sbin/groupadd --gid 10001 symphony \
 && /usr/sbin/useradd --uid 10001 --gid 10001 --home-dir /tmp --no-create-home --shell /usr/sbin/nologin symphony \
 && install -d -o 10001 -g 10001 -m 0700 /workspace /tmp
COPY --from=codex /usr/local/ /usr/local/
COPY --from=codex --chown=10001:10001 /opt/codex/ /opt/codex/
COPY --from=build --chown=10001:10001 /src/elixir/bin/symphony /usr/local/bin/symphony-worker
USER 10001:10001
WORKDIR /workspace
ENTRYPOINT ["/usr/local/bin/symphony-worker"]
