from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import Iterable


_REL_LINE_PATTERN = re.compile(
    r"\(:(?P<src>[A-Za-z_][\w]*)\)\s*-\s*\[:(?P<rel>[A-Za-z_][\w]*)\]\s*->\s*\(:(?P<dst>[A-Za-z_][\w]*)\)"
)


@dataclass(frozen=True)
class GraphSchema:
    relationships: dict[str, frozenset[tuple[str, str]]]

    @classmethod
    def from_edges(cls, edges: Iterable[tuple[str, str, str]]) -> "GraphSchema":
        mapping: dict[str, set[tuple[str, str]]] = {}
        for src, rel, dst in edges:
            mapping.setdefault(rel, set()).add((src, dst))
        frozen = {rel: frozenset(pairs) for rel, pairs in mapping.items()}
        return cls(relationships=frozen)

    def allows(self, rel_type: str, src_label: str, dst_label: str) -> bool:
        return (src_label, dst_label) in self.relationships.get(rel_type, frozenset())

    @classmethod
    def load_default(cls) -> "GraphSchema":
        env_path = os.environ.get("K8S_GRAPH_SCHEMA_PATH")
        if env_path:
            loaded = cls._load_from_adk_config(Path(env_path))
            if loaded is not None:
                return loaded
        default_path = _default_schema_path()
        loaded = cls._load_from_adk_config(default_path)
        if loaded is not None:
            return loaded
        return cls.from_edges(_fallback_edges())

    @classmethod
    def _load_from_adk_config(cls, path: Path) -> "GraphSchema" | None:
        if not path.exists():
            return None
        edges: list[tuple[str, str, str]] = []
        try:
            content = path.read_text(encoding="utf-8")
        except OSError:
            return None
        for line in content.splitlines():
            match = _REL_LINE_PATTERN.search(line)
            if match:
                edges.append(
                    (match.group("src"), match.group("rel"), match.group("dst"))
                )
        if not edges:
            return None
        return cls.from_edges(edges)


def _default_schema_path() -> Path:
    agent_root = Path(__file__).resolve().parents[2]
    return agent_root / "adk_config" / "k8s_graph_agent" / "root_agent.yaml"


def _fallback_edges() -> list[tuple[str, str, str]]:
    return [
        ("Host", "IsClaimedBy", "Ingress"),
        ("Ingress", "DefinesBackend", "IngressServiceBackend"),
        ("IngressServiceBackend", "TargetsService", "Service"),
        ("Service", "Manages", "EndpointSlice"),
        ("EndpointSlice", "ContainsEndpoint", "Endpoint"),
        ("Endpoint", "HasAddress", "EndpointAddress"),
        ("EndpointAddress", "IsAddressOf", "Pod"),
        ("EndpointAddress", "ListedIn", "EndpointSlice"),
    ]
