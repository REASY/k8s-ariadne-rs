FROM rust:1.87 AS builder

WORKDIR /code

# System deps to compile Rust crates that may need native deps
RUN apt-get update -y && \
    apt-get install cmake libclang-dev -y && \
    rm -rf /var/lib/apt/lists/*

# 1) Cache dependency build
# Copy only manifests first to maximize cache hits for dependencies
COPY Cargo.toml Cargo.lock ./
COPY ariadne-app/Cargo.toml ariadne-app/Cargo.toml
COPY ariadne-core/Cargo.toml ariadne-core/Cargo.toml
COPY ariadne-tools/Cargo.toml ariadne-tools/Cargo.toml

# Create minimal src trees so cargo can resolve the workspace and build deps
RUN mkdir -p ariadne-app/src ariadne-core/src ariadne-tools/src && \
    printf "fn main() {}\n" > ariadne-app/src/main.rs && \
    printf "\n" > ariadne-core/src/lib.rs && \
    printf "\n" > ariadne-tools/src/lib.rs

# Use BuildKit cache mounts to speed up cargo downloads between builds
# This compiles the dependency graph and primes the cargo cache,
# but produces a trivial binary since sources are stubs.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --locked --release -p ariadne-app

# 2) Now copy the actual sources
RUN rm -rf ariadne-app/src ariadne-core/src ariadne-tools/src
COPY . .

# Build the real binary
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo install --locked --path ariadne-app --profile release


FROM ubuntu:25.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
      iputils-ping net-tools curl binutils python3 pkg-config \
      libdw-dev libssl-dev libsasl2-dev git unzip && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/*

ARG COMMIT_SHA
ARG BUILD_DATE
ARG VERSION

COPY --from=builder /usr/local/cargo/bin/ariadne-app /usr/local/bin/ariadne-app
CMD ["ariadne-app"]

LABEL \
    org.opencontainers.image.name="k8s-ariadne-rs" \
    org.opencontainers.image.description="Ariadne" \
    org.opencontainers.image.url="https://github.com/REASY/k8s-ariadne-rs" \
    org.opencontainers.image.source="https://github.com/REASY/k8s-ariadne-rs.git" \
    org.opencontainers.image.version="$VERSION" \
    org.opencontainers.image.licenses="MIT License" \
    org.opencontainers.image.authors="Artavazd Balaian <reasyu@gmail.com>" \
    org.opencontainers.image.base.name="ubuntu:25.04" \
    org.opencontainers.image.created="$BUILD_DATE" \
    org.opencontainers.image.revision="${COMMIT_SHA}"
