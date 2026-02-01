#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
AGENT_DIR="${ROOT_DIR}/python/agent"

BRIDGE_HOST="${BRIDGE_HOST:-127.0.0.1}"
BRIDGE_PORT="${BRIDGE_PORT:-8001}"
WEBUI_HOST="${WEBUI_HOST:-127.0.0.1}"
WEBUI_PORT="${WEBUI_PORT:-3000}"

export OPENAI_API_BASE_URL="http://${BRIDGE_HOST}:${BRIDGE_PORT}/v1"
export OPENAI_API_KEY="${OPENAI_API_KEY:-dummy-key}"
export OPENAI_API_BASE_URLS="${OPENAI_API_BASE_URL}"
export OPENAI_API_KEYS="${OPENAI_API_KEY}"

cd "${AGENT_DIR}"

K8S_GRAPH_BRIDGE_INCLUDE_CYPHER=true uv run k8s-graph-openai-bridge --host "${BRIDGE_HOST}" --port "${BRIDGE_PORT}" &
BRIDGE_PID=$!

uvx --python 3.11 open-webui@latest serve --host "${WEBUI_HOST}" --port "${WEBUI_PORT}" &
WEBUI_PID=$!

cleanup() {
  kill "${BRIDGE_PID}" "${WEBUI_PID}" 2>/dev/null || true
}
trap cleanup EXIT

wait "${BRIDGE_PID}" "${WEBUI_PID}"
