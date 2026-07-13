# Multi-stage build for the `pulsusdb` binary (issue #7 e2e harness plan).
# A Debian build stage compiles the release binary with the pinned
# toolchain (rust-toolchain.toml); the runtime stage is `debian:bookworm`
# too (slim variant) — same libc as the build stage, so there is no
# host/container glibc mismatch, and the shipped image is far smaller than
# the full toolchain image. Produces the local `pulsusdb:e2e` tag that
# `deploy/e2e/compose.single.yaml` / `compose.cluster.yaml` build against;
# no image is published to a registry from here (out of scope — tracked
# separately for the M7 release job).
#
# Build:
#   podman build -t pulsusdb:e2e .
#   # or: docker build -t pulsusdb:e2e .

FROM docker.io/library/rust:1.93-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p pulsus-server --bin pulsusdb

FROM docker.io/library/debian:bookworm-slim AS runtime
# `wget` backs this image's own `/ready` healthcheck (compose overrides);
# `ca-certificates` for any outbound TLS the process makes (e.g. a
# `CLICKHOUSE_PROTO=https` deployment).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/pulsusdb /usr/local/bin/pulsusdb

EXPOSE 3100
ENTRYPOINT ["/usr/local/bin/pulsusdb"]
