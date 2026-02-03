from __future__ import annotations

import os
import traceback
from typing import Optional


def format_java_like(exc: BaseException, thread_name: Optional[str] = None) -> str:
    tb = traceback.TracebackException.from_exception(exc)
    exc_name = tb.exc_type_str if tb.exc_type_str else "Exception"
    header = (
        f'Exception in thread "{thread_name}" ({exc_name}): {tb}'
        if thread_name
        else f"Exception ({exc_name}): {tb}"
    )
    lines: list[str] = [header]

    frames = list(tb.stack)
    top_n, bottom_m = 10, 4
    if len(frames) > top_n + bottom_m:
        kept = frames[:top_n] + frames[-bottom_m:]
        elided = len(frames) - len(kept)
    else:
        kept = frames
        elided = 0

    for frame in kept:
        filename = os.path.basename(frame.filename)
        lines.append(f"    at {frame.name}({filename}:{frame.lineno})")
    if elided:
        lines.append(f"    ... {elided} frames elided")
    return "\n".join(lines)
