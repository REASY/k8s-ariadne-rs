FROM rust:1.89 AS builder

WORKDIR /app

# System deps to compile Rust crates that may need native deps
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends cmake libclang-dev && \
    rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked


FROM builder AS chef_planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json


FROM builder AS chef_builder
COPY --from=chef_planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --locked --release -p ariadne-app


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

# Copy the built binary from the builder's target dir
COPY --from=chef_builder /app/target/release/ariadne-app /app/ariadne-app

ENTRYPOINT ["/app/ariadne-app"]

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
