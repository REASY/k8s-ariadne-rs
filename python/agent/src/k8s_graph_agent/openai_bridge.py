from __future__ import annotations

import argparse
import logging
import os
import time
import uuid
from typing import Any, Iterable, cast

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from .agent import GraphAgent, GraphMcpClient
from .config import AdkConfig, AgentConfig
from .mcp_client import StreamableHttpMcpClient
from .synthesize import (
    SimpleResponseSynthesizer,
    SreResponseSynthesizer,
    WebUiResponseSynthesizer,
)


def create_app() -> FastAPI:
    app = FastAPI(
        title="k8s-graph-agent OpenAI bridge",
        version="0.1.0",
        openapi_url="/v1/openapi.json",
        docs_url=None,
        redoc_url=None,
    )
    logger = logging.getLogger(__name__)
    _configure_cors(app)

    @app.get("/v1/models")
    def list_models() -> dict[str, Any]:
        model_id = _model_id()
        created = int(time.time())
        return {
            "object": "list",
            "data": [
                {
                    "id": model_id,
                    "object": "model",
                    "created": created,
                    "owned_by": "k8s-graph-agent",
                }
            ],
        }

    @app.post("/v1/chat/completions")
    def chat_completions(request: ChatCompletionRequest) -> dict[str, Any]:
        if request.stream:
            logger.info("streaming requested; falling back to non-stream response")

        question = _extract_question(request.messages)
        if not question:
            logger.warning("no user message found; falling back to last message")
            question = _fallback_question(request.messages)
        if not question:
            raise HTTPException(status_code=400, detail="no message content found")

        logger.info("received chat completion request")
        answer = _run_agent(question)
        content = answer.response
        model_id = request.model or _model_id()
        created = int(time.time())
        return {
            "id": f"chatcmpl-{uuid.uuid4().hex}",
            "object": "chat.completion",
            "created": created,
            "model": model_id,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        }

    return app


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OpenAI-compatible bridge for k8s-graph-agent"
    )
    parser.add_argument("--host", default=os.environ.get("K8S_GRAPH_BRIDGE_HOST", ""))
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("K8S_GRAPH_BRIDGE_PORT", "8001")),
    )
    args = parser.parse_args()

    _configure_logging()
    import uvicorn

    host = args.host or "0.0.0.0"
    uvicorn.run(
        "k8s_graph_agent.openai_bridge:create_app",
        host=host,
        port=args.port,
        factory=True,
        log_level=os.environ.get("K8S_GRAPH_BRIDGE_LOG_LEVEL", "info").lower(),
    )


class ChatMessage(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: str
    content: str | list[dict[str, Any]] | None = None


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    model: str | None = Field(default=None)
    messages: list[ChatMessage]
    stream: bool | None = Field(default=False)


def _run_agent(question: str):
    config = AgentConfig.from_env()
    mcp = StreamableHttpMcpClient(
        base_url=config.mcp_url,
        timeout_seconds=config.request_timeout_seconds,
        client_name=config.client_name,
        client_version=config.client_version,
        auth_token=config.mcp_auth_token,
    )
    try:
        graph = GraphMcpClient(mcp=mcp)
        translator = _build_translator(mcp)
        synthesizer = _build_synthesizer()
        agent = GraphAgent(graph=graph, translator=translator, synthesizer=synthesizer)
        return agent.answer(question)
    finally:
        mcp.close()


def _build_translator(mcp: StreamableHttpMcpClient):
    use_adk = os.environ.get("K8S_GRAPH_BRIDGE_USE_ADK", "true").lower() in {
        "1",
        "true",
        "yes",
    }
    if not use_adk:
        from .translate import PrefixCypherTranslator

        return PrefixCypherTranslator()

    from .adk_translate import AdkCypherTranslator

    adk_config = AdkConfig.from_env()
    return AdkCypherTranslator(mcp=mcp, config=adk_config)


def _build_synthesizer():
    style = os.environ.get("K8S_GRAPH_BRIDGE_STYLE", "ui").lower().strip()
    if style in {"simple", "basic"}:
        return SimpleResponseSynthesizer()
    if style in {"sre", "default"}:
        return SreResponseSynthesizer()
    max_rows = int(os.environ.get("K8S_GRAPH_BRIDGE_MAX_ROWS", "25"))
    max_cell_chars = int(os.environ.get("K8S_GRAPH_BRIDGE_MAX_CELL_CHARS", "120"))
    include_cypher = os.environ.get(
        "K8S_GRAPH_BRIDGE_INCLUDE_CYPHER", "false"
    ).lower() in {"1", "true", "yes"}
    cypher_fence = os.environ.get("K8S_GRAPH_BRIDGE_CYPHER_FENCE", "true").lower() in {
        "1",
        "true",
        "yes",
    }
    cypher_format = os.environ.get("K8S_GRAPH_BRIDGE_CYPHER_FORMAT", "pretty").lower()
    compact_values = os.environ.get(
        "K8S_GRAPH_BRIDGE_COMPACT_VALUES", "true"
    ).lower() in {"1", "true", "yes"}
    return WebUiResponseSynthesizer(
        max_rows=max_rows,
        include_cypher=include_cypher,
        cypher_fence=cypher_fence,
        cypher_format=cypher_format,
        max_cell_chars=max_cell_chars,
        compact_values=compact_values,
    )


def _extract_question(messages: Iterable[ChatMessage]) -> str:
    last_user: str | None = None
    for message in messages:
        if message.role != "user":
            continue
        text = _coerce_content(message.content)
        if text:
            last_user = text
    return last_user or ""


def _fallback_question(messages: Iterable[ChatMessage]) -> str:
    last_text: str | None = None
    for message in messages:
        text = _coerce_content(message.content)
        if text:
            last_text = text
    return last_text or ""


def _coerce_content(content: str | list[dict[str, Any]] | None) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts: list[str] = []
    for part in content:
        if not isinstance(part, dict):
            continue
        if isinstance(part.get("text"), str):
            parts.append(part["text"])
        elif isinstance(part.get("content"), str):
            parts.append(part["content"])
    return "\n".join(parts)


def _model_id() -> str:
    return os.environ.get("K8S_GRAPH_BRIDGE_MODEL_ID", "k8s-graph-agent")


def _configure_logging() -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return
    level_name = os.environ.get("K8S_GRAPH_BRIDGE_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
    )


def _configure_cors(app: FastAPI) -> None:
    origins = os.environ.get("K8S_GRAPH_BRIDGE_CORS_ORIGINS", "*")
    allow_origins = [origin.strip() for origin in origins.split(",") if origin.strip()]
    app.add_middleware(
        cast(Any, CORSMiddleware),
        allow_origins=allow_origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


if __name__ == "__main__":
    main()
