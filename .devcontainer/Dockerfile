FROM rust:1.91

# System deps to compile Rust crates that may need native deps
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends cmake libclang-dev iputils-ping net-tools curl \
    binutils python3 pkg-config libdw-dev libssl-dev libsasl2-dev git unzip && \
    rm -rf /var/lib/apt/lists/*
