from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from .agent import GraphAgent, GraphMcpClient
from .config import AgentConfig, AdkConfig
from .mcp_client import StreamableHttpMcpClient
from .synthesize import SimpleResponseSynthesizer, SreResponseSynthesizer
from .translate import PrefixCypherTranslator


def main() -> None:
    parser = argparse.ArgumentParser(description="K8s graph agent (MCP client)")
    parser.add_argument(
        "question", nargs="?", help="Natural language question or cypher: ..."
    )
    parser.add_argument("--cypher", help="Execute a Cypher query directly")
    parser.add_argument("--raw", action="store_true", help="Print raw JSON result")
    parser.add_argument(
        "--rows",
        type=int,
        default=0,
        help="Print up to N result rows as a table",
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Skip the summary and only print rows/raw output",
    )
    parser.add_argument(
        "--use-adk", action="store_true", help="Use Google ADK for translation"
    )
    parser.add_argument("--adk-model", help="Override ADK model name")
    parser.add_argument(
        "--simple",
        action="store_true",
        help="Use the simple response synthesizer",
    )
    args = parser.parse_args()

    if not args.question and not args.cypher:
        parser.print_usage()
        sys.exit(2)

    config = AgentConfig.from_env()
    mcp = StreamableHttpMcpClient(
        base_url=config.mcp_url,
        timeout_seconds=config.request_timeout_seconds,
        client_name=config.client_name,
        client_version=config.client_version,
        auth_token=config.mcp_auth_token,
    )

    graph = GraphMcpClient(mcp=mcp)
    synthesizer = (
        SimpleResponseSynthesizer() if args.simple else SreResponseSynthesizer()
    )

    if args.use_adk:
        _configure_logging()
        from .adk_translate import AdkCypherTranslator

        if args.adk_model:
            os.environ["LLM_MODEL"] = args.adk_model
        adk_config = AdkConfig.from_env()
        translator = AdkCypherTranslator(mcp=mcp, config=adk_config)
    else:
        translator = PrefixCypherTranslator()

    if args.cypher:
        question = args.question or f"cypher: {args.cypher}"
        result = graph.execute_cypher(args.cypher)
        response = synthesizer.synthesize(question, args.cypher, result)
        if args.raw:
            print(json.dumps(result, ensure_ascii=True))
        elif args.no_summary:
            _print_rows(result, args.rows)
        else:
            print(response)
            _print_rows(result, args.rows)
        return

    agent = GraphAgent(graph=graph, translator=translator, synthesizer=synthesizer)
    answer = agent.answer(args.question)
    if args.raw:
        print(json.dumps(answer.result, ensure_ascii=True))
    elif args.no_summary:
        _print_rows(answer.result, args.rows)
    else:
        print(answer.response)
        _print_rows(answer.result, args.rows)


if __name__ == "__main__":
    main()


def _configure_logging() -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return
    level_name = os.environ.get("K8S_GRAPH_AGENT_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
    )


def _print_rows(result: object, limit: int) -> None:
    if limit <= 0:
        return
    if not isinstance(result, list):
        print("\nRows:")
        print(json.dumps(result, ensure_ascii=True))
        return
    rows = [row for row in result if isinstance(row, dict)]
    if not rows:
        print("\nRows: none")
        return
    rows = rows[:limit]
    columns = _collect_columns(rows)
    if not columns:
        print("\nRows:")
        for row in rows:
            print(json.dumps(row, ensure_ascii=True))
        return
    widths = _column_widths(columns, rows)
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    sep = "-+-".join("-" * widths[col] for col in columns)
    print("\nRows:")
    print(header)
    print(sep)
    for row in rows:
        line = " | ".join(
            _format_cell(row.get(col)).ljust(widths[col]) for col in columns
        )
        print(line)


def _collect_columns(rows: list[dict[str, object]]) -> list[str]:
    columns: list[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                columns.append(key)
    return columns


def _column_widths(columns: list[str], rows: list[dict[str, object]]) -> dict[str, int]:
    widths: dict[str, int] = {}
    for col in columns:
        widths[col] = len(col)
    for row in rows:
        for col in columns:
            value = _format_cell(row.get(col))
            if len(value) > widths[col]:
                widths[col] = len(value)
    return widths


def _format_cell(value: object) -> str:
    text = json.dumps(value, ensure_ascii=True)
    if len(text) > 120:
        return text[:117] + "..."
    return text
