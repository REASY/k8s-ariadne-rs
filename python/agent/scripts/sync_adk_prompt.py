from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync prompt.txt into the ADK config agent instruction"
    )
    parser.add_argument(
        "--model",
        default="gemini-2.0-flash",
        help="Model name for the config agent",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    prompt_path = repo_root / "prompt.txt"
    agent_dir = repo_root / "python" / "agent" / "adk_config" / "k8s_graph_agent"
    agent_path = agent_dir / "root_agent.yaml"

    prompt = prompt_path.read_text(encoding="utf-8").rstrip()
    instruction = _indent_block(prompt)

    agent_dir.mkdir(parents=True, exist_ok=True)
    agent_path.write_text(
        _render_agent_yaml(model=args.model, instruction=instruction),
        encoding="utf-8",
    )

    print(f"Synced {prompt_path} -> {agent_path}")


def _indent_block(text: str) -> str:
    return "\n".join(f"  {line}" for line in text.splitlines())


def _render_agent_yaml(*, model: str, instruction: str) -> str:
    return (
        "# yaml-language-server: $schema=https://raw.githubusercontent.com/google/adk-python/main/src/google/adk/agents/config_schemas/AgentConfig.json\n"
        "agent_class: LlmAgent\n"
        "name: k8s_graph_agent\n"
        f"model: {model}\n"
        "description: \"Translate SRE questions to Cypher and query the K8s graph.\"\n"
        "instruction: |\n"
        f"{instruction}\n\n"
        "tools:\n"
        "  - name: k8s_graph_agent.adk_tools.execute_cypher_query\n"
    )


if __name__ == "__main__":
    main()
