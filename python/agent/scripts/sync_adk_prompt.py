from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync generated prompt into the ADK config agent instruction"
    )
    parser.add_argument(
        "--model",
        default="gemini-2.0-flash",
        help="Model name for the config agent",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    agent_dir = repo_root / "python" / "agent" / "adk_config" / "k8s_graph_agent"
    agent_path = agent_dir / "root_agent.yaml"

    prompt = _generate_prompt(repo_root)
    instruction = _indent_block(prompt)

    agent_dir.mkdir(parents=True, exist_ok=True)
    agent_path.write_text(
        _render_agent_yaml(model=args.model, instruction=instruction),
        encoding="utf-8",
    )

    print(f"Synced prompt -> {agent_path}")


def _indent_block(text: str) -> str:
    return "\n".join(f"  {line}" for line in text.splitlines())


def _generate_prompt(repo_root: Path) -> str:
    tool_path = repo_root / "target" / "debug" / "ariadne-tools"
    if tool_path.exists():
        cmd = [str(tool_path), "--full-prompt"]
    else:
        cmd = ["cargo", "run", "-q", "-p", "ariadne-tools", "--", "--full-prompt"]
    result = subprocess.run(
        cmd,
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    prompt = result.stdout.rstrip()
    if not prompt:
        raise SystemExit("Failed to generate prompt from ariadne-tools")
    return prompt


def _render_agent_yaml(*, model: str, instruction: str) -> str:
    return (
        "# yaml-language-server: $schema=https://raw.githubusercontent.com/google/adk-python/main/src/google/adk/agents/config_schemas/AgentConfig.json\n"
        "agent_class: LlmAgent\n"
        "name: k8s_graph_agent\n"
        f"model: {model}\n"
        'description: "Translate SRE questions to Cypher and query the K8s graph."\n'
        "instruction: |\n"
        f"{instruction}\n\n"
        "tools:\n"
        "  - name: k8s_graph_agent.adk_tools.execute_cypher_query\n"
    )


if __name__ == "__main__":
    main()
