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
# The build stage is split via cargo-chef into a `planner` stage that
# computes a dependency-only recipe from the workspace manifests, and a
# `build` stage that first `cargo chef cook`s just that recipe — a layer
# keyed only on Cargo.toml/Cargo.lock/rust-toolchain.toml, so it is skipped
# whenever a commit only touches application source — before copying in the
# full source and building the release binary. Combined with the buildx
# `type=gha` layer cache used in CI, this means dependency compilation is
# cached across commits instead of being redone on every image build.
#
# Build:
#   podman build -t pulsusdb:e2e .
#   # or: docker build -t pulsusdb:e2e .

FROM docker.io/library/rust:1.93-bookworm AS planner
RUN cargo install cargo-chef --locked --version 0.1.77
WORKDIR /src
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM docker.io/library/rust:1.93-bookworm AS build
RUN cargo install cargo-chef --locked --version 0.1.77
WORKDIR /src
COPY --from=planner /src/recipe.json recipe.json
# rust-toolchain.toml pins an exact patch (1.93.0) that the `rust:1.93-bookworm`
# tag does not always match byte-for-byte (it tracks the latest 1.93.x patch).
# Copying it in before `cook` makes cook build the dependency graph with the
# same rustc used by the final `cargo build` below; without it, cook and the
# final build silently used different rustc versions and every dependency
# fingerprint (and thus the whole point of this cache layer) was invalidated.
COPY rust-toolchain.toml rust-toolchain.toml
RUN cargo chef cook --release --recipe-path recipe.json
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
