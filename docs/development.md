# Development

## Build Rust

```bash
cargo build
```

## Run web UI

```bash
cargo run --release -p ariadne-app
```

Open: [http://127.0.0.1:18080/index.html](http://127.0.0.1:18080/index.html)

## Build Docker image

```bash
APP_VERSION=$(cargo pkgid --manifest-path ariadne-app/Cargo.toml | cut -d '#' -f2)
docker build --platform linux/amd64 \
  --build-arg BUILD_DATE="$(date +'%Y-%m-%dT%H:%M:%S%z')" \
  --build-arg COMMIT_SHA=$(git rev-parse HEAD) \
  --build-arg VERSION="$APP_VERSION" \
  . -f docker/Dockerfile \
  -t k8s-ariadne-rs:$APP_VERSION
```
