# ariadne-cypher

Full openCypher parser and validation layer using tree-sitter.

## Setup
No external tooling is required. The `tree-sitter-cypher` grammar is pulled as a dependency.

## What this crate provides
- tree-sitter-based parser with a stable Rust API.
- `parse_cypher` entry point for syntax validation.
- AST conversion (`parse_query`) plus semantic validation (`validate_query`).
- Tests for common Cypher constructs (MATCH/WHERE/IN/WITH/UNWIND).
- openCypher TCK parser compatibility checks.

## Run openCypher TCK parser tests
Clone the openCypher repository and point `TCK_PATH` at its `tck/` directory:

```bash
git clone https://github.com/opencypher/opencypher.git
TCK_PATH=/path/to/opencypher/tck \
  cargo test -p ariadne-cypher --test tck_parser
```

Optional skip list (by tag or scenario substring):
```bash
TCK_PATH=/path/to/opencypher/tck \
TCK_SKIP=/path/to/skip.txt \
  cargo test -p ariadne-cypher --test tck_parser
```

## Next steps
- Expand semantic validation rules.
- Build out engine execution coverage for more Cypher clauses.
