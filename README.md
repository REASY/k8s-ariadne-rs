k8s-graph-rs
-----

Simple web application that discovers objects in the Kubernetes cluster (K8S). It exposes an endpoint `/v1/graph` that
returns the state of K8s as directed graph.

# High-level details
![high_level_diagram.svg](doc/high_level_diagram.svg)

# Development

The project requires the following tools configured on your developer machine:

- Cargo and Rust compiler installed, use [rustup](https://www.rust-lang.org/tools/install)

This project uses [the kube](https://docs.rs/kube/0.91.0/kube/) Rust library to interact with K8S. Make sure you have kubectl config, cluster's certificate-authority, user's certificate and key to be able to interact with the cluster. 
By default, [kube](https://docs.rs/kube/0.91.0/kube/config/index.html) resolves in the following way:
> Kubernetes configuration objects from ~/.kube/config, $KUBECONFIG, or the cluster environment.

## Build the project

```bash
cargo build
```

## Run web app

```bash
KUBE_NAMESPACE=pyroscope KUBE_CONTEXT=tools.hk-tools-2t cargo run --release

   Compiling ariadne-app v0.1.0 (/Users/abalaian/github/REASY/k8s-graph-rs/ariadne-app)
   Compiling ariadne-core v0.1.0 (/Users/abalaian/github/REASY/k8s-graph-rs/ariadne-core)
    Finished `release` profile [optimized] target(s) in 9.22s
     Running `target/release/ariadne-app`
2025-07-04T07:00:09.904994Z  INFO main ThreadId(01) ariadne_app: ariadne-app/src/main.rs:84: Cluster: Some("tools.hk-tools-2t"), namespace: pyroscope
2025-07-04T07:00:09.905196Z  INFO main ThreadId(01) ariadne_app: ariadne-app/src/main.rs:95: Created fetch_state_handle
2025-07-04T07:00:09.905307Z  INFO tokio-runtime-worker ThreadId(15) ariadne_app: ariadne-app/src/main.rs:52: Starting fetch_state
2025-07-04T07:00:10.105757Z  INFO                 main ThreadId(01) ariadne_app: ariadne-app/src/main.rs:127: Server listening for HTTP on http://127.0.0.1:18080
```

## Open the browser at http://127.0.0.1:18080/index.html

![img.png](doc/img.png)

# Build docker image
Run the command below from the root folder of the project
```console
APP_VERSION=$(cargo pkgid --manifest-path ariadne-app/Cargo.toml | cut -d '#' -f2); docker build --platform linux/amd64 --build-arg BUILD_DATE="$(date +'%Y-%m-%dT%H:%M:%S%z')" \
    --build-arg COMMIT_SHA=$(git rev-parse HEAD) \
    --build-arg VERSION=$(cargo pkgid --manifest-path ariadne-app/Cargo.toml | cut -d '#' -f2) \
    . -f docker/Dockerfile \
   -t k8s-ariadne-rs:$APP_VERSION
```