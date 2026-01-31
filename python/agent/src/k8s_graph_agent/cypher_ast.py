from __future__ import annotations

from dataclasses import dataclass

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener

from antlr4_cypher import CypherLexer, CypherParser


class CypherParseError(ValueError):
    pass


@dataclass(frozen=True)
class CypherAst:
    text: str
    tree: object
    parser: CypherParser
    tokens: CommonTokenStream


class _CypherErrorListener(ErrorListener):
    def __init__(self) -> None:
        self.errors: list[str] = []

    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):  # type: ignore[override]
        self.errors.append(f"line {line}:{column} {msg}")


def parse_cypher(text: str) -> CypherAst:
    input_stream = InputStream(text)
    lexer = CypherLexer(input_stream)
    tokens = CommonTokenStream(lexer)
    parser = CypherParser(tokens)
    parser.removeErrorListeners()
    listener = _CypherErrorListener()
    parser.addErrorListener(listener)
    tree = parser.script()
    if listener.errors:
        raise CypherParseError("Cypher parse failed: " + "; ".join(listener.errors))
    return CypherAst(text=text, tree=tree, parser=parser, tokens=tokens)
