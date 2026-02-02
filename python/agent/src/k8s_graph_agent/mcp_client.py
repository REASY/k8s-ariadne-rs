from __future__ import annotations

from dataclasses import dataclass, field
import itertools
import json
from typing import Any, Iterable, Iterator, Protocol, cast

from .models import JsonObject, JsonValue


class McpError(Exception):
    pass


class McpProtocolError(McpError):
    pass


class JsonRpcError(McpError):
    def __init__(self, error: JsonObject) -> None:
        message = error.get("message", "JSON-RPC error")
        super().__init__(str(message))
        self.error = error


class McpClient(Protocol):
    def initialize(self) -> JsonObject: ...

    def list_tools(self) -> list[JsonObject]: ...

    def list_prompts(self) -> list[JsonObject]: ...

    def get_prompt(
        self, name: str, arguments: JsonObject | None = None
    ) -> JsonObject: ...

    def call_tool(
        self, name: str, arguments: JsonObject | None = None
    ) -> JsonObject: ...


@dataclass
class StreamableHttpMcpClient:
    base_url: str
    timeout_seconds: float
    client_name: str
    client_version: str
    auth_token: str | None = None
    protocol_version: str = "2025-03-26"
    _http: Any = field(init=False)
    _id_counter: Iterator[int] = field(init=False)
    _session_id: str | None = field(init=False, default=None)
    _initialized: bool = field(init=False, default=False)
    _server_info: JsonObject | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        import httpx

        self._http = httpx.Client(timeout=self.timeout_seconds)
        self._id_counter = itertools.count(1)

    def initialize(self) -> JsonObject:
        if self._initialized and self._server_info is not None:
            return self._server_info
        params: JsonObject = {
            "protocolVersion": self.protocol_version,
            "capabilities": {},
            "clientInfo": {"name": self.client_name, "version": self.client_version},
        }
        result = self._request("initialize", params)
        if not isinstance(result, dict):
            raise McpProtocolError("initialize returned non-object result")
        result_obj = cast(JsonObject, result)
        self._server_info = result_obj
        self._initialized = True
        self._notify_initialized()
        return result_obj

    def list_tools(self) -> list[JsonObject]:
        self._ensure_initialized()
        result = self._request("tools/list", {})
        if not isinstance(result, dict):
            raise McpProtocolError("tools/list returned non-object result")
        result_obj = cast(JsonObject, result)
        tools = result_obj.get("tools", [])
        if not isinstance(tools, list):
            raise McpProtocolError("tools/list returned invalid tools list")
        return [tool for tool in tools if isinstance(tool, dict)]

    def list_prompts(self) -> list[JsonObject]:
        self._ensure_initialized()
        result = self._request("prompts/list", {})
        if not isinstance(result, dict):
            raise McpProtocolError("prompts/list returned non-object result")
        result_obj = cast(JsonObject, result)
        prompts = result_obj.get("prompts", [])
        if not isinstance(prompts, list):
            raise McpProtocolError("prompts/list returned invalid prompts list")
        return [prompt for prompt in prompts if isinstance(prompt, dict)]

    def get_prompt(self, name: str, arguments: JsonObject | None = None) -> JsonObject:
        self._ensure_initialized()
        params: JsonObject = {"name": name}
        if arguments is not None:
            params["arguments"] = arguments
        result = self._request("prompts/get", params)
        if not isinstance(result, dict):
            raise McpProtocolError("prompts/get returned non-object result")
        return cast(JsonObject, result)

    def call_tool(self, name: str, arguments: JsonObject | None = None) -> JsonObject:
        self._ensure_initialized()
        params: JsonObject = {"name": name}
        if arguments is not None:
            params["arguments"] = arguments
        result = self._request("tools/call", params)
        if not isinstance(result, dict):
            raise McpProtocolError("tools/call returned non-object result")
        return cast(JsonObject, result)

    def close(self) -> None:
        self._http.close()

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            self.initialize()

    def _notify_initialized(self) -> None:
        message: JsonObject = {"jsonrpc": "2.0", "method": "notifications/initialized"}
        self._post_message(message)

    def _request(self, method: str, params: JsonObject | None) -> JsonValue:
        request_id = next(self._id_counter)
        message: JsonObject = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        responses = self._post_message(message)
        response = _pick_response(responses, request_id)
        if response is None:
            raise McpProtocolError(f"no response for request id {request_id}")
        if "error" in response:
            error = response.get("error")
            if isinstance(error, dict):
                raise JsonRpcError(cast(JsonObject, error))
            raise McpProtocolError("json-rpc error returned without error object")
        if "result" not in response:
            raise McpProtocolError("json-rpc response missing result")
        return response["result"]

    def _post_message(self, message: JsonObject) -> list[JsonObject]:
        headers = {"Accept": "application/json, text/event-stream"}
        if self._session_id is not None:
            headers["mcp-session-id"] = self._session_id
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        response = self._http.post(self.base_url, json=message, headers=headers)
        if response.status_code in (202, 204):
            return []
        response.raise_for_status()
        session_id = response.headers.get("mcp-session-id")
        if session_id and self._session_id is None:
            self._session_id = session_id
        content_type = response.headers.get("content-type", "")
        if content_type.startswith("text/event-stream"):
            messages = _parse_sse_messages(response.text)
            if not messages:
                raise McpProtocolError("no json-rpc messages in sse response")
            return messages
        if content_type.startswith("application/json"):
            body = response.json()
            if isinstance(body, dict):
                return [body]
            raise McpProtocolError("expected json-rpc object response")
        raise McpProtocolError(f"unexpected content type: {content_type}")


def extract_json_content(tool_result: JsonObject) -> JsonValue:
    content = tool_result.get("content", [])
    if isinstance(content, list):
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get("type") != "text":
                continue
            text = item.get("text")
            if not isinstance(text, str):
                continue
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                continue
    structured = tool_result.get("structuredContent")
    if structured is not None:
        return structured
    raise McpProtocolError("tool result did not contain json content")


def _parse_sse_messages(body: str) -> list[JsonObject]:
    messages: list[JsonObject] = []
    data_lines: list[str] = []

    def flush() -> None:
        if not data_lines:
            return
        data = "\n".join(data_lines).strip()
        data_lines.clear()
        if not data:
            return
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            return
        if isinstance(parsed, dict):
            messages.append(parsed)

    for line in body.splitlines():
        if not line:
            flush()
            continue
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    flush()
    return messages


def _pick_response(
    responses: Iterable[JsonObject], request_id: int
) -> JsonObject | None:
    for response in responses:
        if response.get("id") == request_id:
            return response
    return None
