FROM elixir:1.19.5-otp-28-slim@sha256:597474ac81f0d0bfa4d67546c2591a033315a2b9e6ab2f1a22824af9c913066a

WORKDIR /src/elixir
COPY elixir/ .
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates python3 \
 && rm -rf /var/lib/apt/lists/* \
 && mix local.hex --force \
 && mix local.rebar --force \
 && MIX_ENV=test mix deps.get \
 && MIX_ENV=test mix compile

ENV MIX_ENV=test HGS740_ROOT_FIXTURE=1
CMD ["mix", "test", "--no-compile", "test/root_fixtures/root_fixture.exs", "test/root_fixtures/root_abort_input_publisher_fixture.exs"]
