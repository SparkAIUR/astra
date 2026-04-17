# syntax=docker/dockerfile:1.7

FROM rust:1.95-bookworm AS builder
WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

ARG TARGETARCH
ARG RUSTFLAGS_EXTRA=""

RUN if [ "$TARGETARCH" = "amd64" ]; then \
      export RUSTFLAGS="-C target-cpu=ivybridge ${RUSTFLAGS_EXTRA}"; \
    else \
      export RUSTFLAGS="${RUSTFLAGS_EXTRA}"; \
    fi && \
    cargo build --release -p astrad -p astractl

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=builder /workspace/target/release/astrad /usr/local/bin/astrad
COPY --from=builder /workspace/target/release/astractl /usr/local/bin/astractl

ENV ASTRAD_CLIENT_ADDR=0.0.0.0:2379
EXPOSE 2379

ENTRYPOINT ["astrad"]
