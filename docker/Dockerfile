FROM rust:1.87 AS builder

WORKDIR /code
COPY . .
RUN apt-get update -y && apt-get install cmake libclang-dev -y && rm -rf /var/lib/apt/lists/* && cargo install --path ariadne-app

FROM ubuntu:25.04

ARG COMMIT_SHA
ARG BUILD_DATE
ARG VERSION

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install iputils-ping net-tools curl -y && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*
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


