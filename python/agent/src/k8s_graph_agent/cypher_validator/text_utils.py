from __future__ import annotations


def _strip_string_literals(text: str) -> str:
    result: list[str] = []
    in_string = False
    escape = False
    for char in text:
        if in_string:
            if escape:
                escape = False
                result.append(" ")
                continue
            if char == "\\":
                escape = True
                result.append(" ")
                continue
            if char == "'":
                in_string = False
            result.append(" ")
            continue
        if char == "'":
            in_string = True
            result.append(" ")
            continue
        result.append(char)
    return "".join(result)


def _looks_like_pattern_expression(text: str) -> bool:
    return any(token in text for token in ("-[:", "<-[", "]-", "->", "<-", ")-", "-("))


def _is_word_boundary(text: str, start: int, end: int) -> bool:
    if start > 0 and (text[start - 1].isalnum() or text[start - 1] == "_"):
        return False
    if end < len(text) and (text[end].isalnum() or text[end] == "_"):
        return False
    return True
