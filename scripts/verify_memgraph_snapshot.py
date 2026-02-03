"""
Export and hash Memgraph graph content for before/after verification.

This script connects to Memgraph via the Neo4j driver, exports nodes/edges
into canonical JSONL files, and prints stable hashes. Use it to confirm that
graph data is unchanged after refactors or ingestion changes by comparing
hash outputs across runs.
"""

#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from typing import Any, Dict, Iterable, Tuple

from neo4j import GraphDatabase


def normalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: normalize(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    if hasattr(value, "iso_format"):
        try:
            return value.iso_format()
        except Exception:
            pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return value


def hash_rows(rows: Iterable[Dict[str, Any]], out_path: str | None) -> Tuple[str, int]:
    hasher = hashlib.sha256()
    count = 0
    out = open(out_path, "w", encoding="utf-8") if out_path else None
    try:
        for row in rows:
            line = json.dumps(
                row, sort_keys=True, separators=(",", ":"), ensure_ascii=True
            )
            hasher.update(line.encode("utf-8"))
            hasher.update(b"\n")
            count += 1
            if out:
                out.write(line)
                out.write("\n")
    finally:
        if out:
            out.close()
    return hasher.hexdigest(), count


def iter_nodes(session) -> Iterable[Dict[str, Any]]:
    query = """
    MATCH (n)
    RETURN n.metadata.uid AS uid, labels(n) AS labels, n AS node
    ORDER BY uid
    """
    for record in session.run(query):
        node = record["node"]
        uid = record["uid"]
        labels = sorted(list(node.labels))
        props = normalize(dict(node))
        yield {"uid": uid, "labels": labels, "props": props}


def iter_edges(session) -> Iterable[Dict[str, Any]]:
    query = """
    MATCH (a)-[r]->(b)
    RETURN a.metadata.uid AS src, type(r) AS rel, b.metadata.uid AS dst, r AS relobj
    ORDER BY src, rel, dst
    """
    for record in session.run(query):
        relobj = record["relobj"]
        props = normalize(dict(relobj))
        yield {
            "src": record["src"],
            "rel": record["rel"],
            "dst": record["dst"],
            "props": props,
        }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify Memgraph contents by hashing canonical node/edge exports."
    )
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="memgraph")
    parser.add_argument(
        "--out", default=None, help="Directory to write nodes/edges JSONL."
    )
    args = parser.parse_args()

    out_dir = args.out
    nodes_out = edges_out = None
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        nodes_out = os.path.join(out_dir, "nodes.jsonl")
        edges_out = os.path.join(out_dir, "edges.jsonl")

    auth = (args.user, args.password)
    with GraphDatabase.driver(args.uri, auth=auth) as driver:
        driver.verify_connectivity()
        with driver.session(database=args.database) as session:
            node_hash, node_count = hash_rows(iter_nodes(session), nodes_out)
            edge_hash, edge_count = hash_rows(iter_edges(session), edges_out)

    graph_hash = hashlib.sha256(
        f"{node_hash}\n{edge_hash}\n".encode("utf-8")
    ).hexdigest()

    print(f"nodes: {node_count} hash={node_hash}")
    print(f"edges: {edge_count} hash={edge_hash}")
    print(f"graph: {graph_hash}")
    if out_dir:
        print(f"wrote: {nodes_out}")
        print(f"wrote: {edges_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

