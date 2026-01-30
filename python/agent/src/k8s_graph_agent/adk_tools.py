from __future__ import annotations

from .config import AgentConfig
from .mcp_client import StreamableHttpMcpClient, extract_json_content
from .models import JsonValue


def execute_cypher_query(query: str) -> JsonValue:
    """
    Execute a Cypher query against the MCP server and return parsed JSON rows.

    Args:
        query (str): Cypher query to run against the graph.

    Returns:
        JsonValue: Parsed JSON content returned by the MCP tool.
    """
    config = AgentConfig.from_env()
    client = StreamableHttpMcpClient(
        base_url=config.mcp_url,
        timeout_seconds=config.request_timeout_seconds,
        client_name=config.client_name,
        client_version=config.client_version,
        auth_token=config.mcp_auth_token,
    )
    try:
        tool_result = client.call_tool("execute_cypher_query", {"query": query})
        return extract_json_content(tool_result)
    finally:
        client.close()
