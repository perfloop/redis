#!/usr/bin/env python3
"""Inject benchmark-only HOL hooks into a disposable Redis source worktree."""

from __future__ import annotations

import argparse
from pathlib import Path

HEADER_MARKER = '#include "../tools/iothread-hol-server-instrument.h" /* PERFLOOP_HOL_INSTRUMENT */\n'
IMPLEMENTATION_MARKER = '#include "../tools/iothread-hol-server-instrument.c" /* PERFLOOP_HOL_INSTRUMENT */\n'


def replace_once(source: str, old: str, new: str, description: str) -> str:
    count = source.count(old)
    if count != 1:
        raise RuntimeError(f"expected one {description} insertion point, found {count}")
    return source.replace(old, new, 1)


def instrument(path: Path) -> None:
    source = path.read_text()
    if HEADER_MARKER in source and IMPLEMENTATION_MARKER in source:
        return
    if HEADER_MARKER in source or IMPLEMENTATION_MARKER in source:
        raise RuntimeError("found a partial HOL instrumentation injection")

    source = replace_once(
        source,
        '#include "server.h"\n',
        '#include "server.h"\n' + HEADER_MARKER,
        "iothread server-instrumentation header",
    )
    source = replace_once(
        source,
        "int processClientsFromIOThread(IOThread *t) {\n",
        "int processClientsFromIOThread(IOThread *t) {\n"
        "    perfloopHolInvocationEnter(t->id, server.stat_numcommands);\n",
        "processClientsFromIOThread entry",
    )
    source = replace_once(
        source,
        "    size_t processed = listLength(mainThreadProcessingClients[t->id]);\n"
        "    if (processed == 0) return 0;\n",
        "    size_t processed = listLength(mainThreadProcessingClients[t->id]);\n"
        "    if (processed == 0) {\n"
        "        perfloopHolInvocationExit(t->id, server.stat_numcommands, 0);\n"
        "        return 0;\n"
        "    }\n",
        "empty processClientsFromIOThread return",
    )
    source = replace_once(
        source,
        "        listUnlinkNode(mainThreadProcessingClients[t->id], node);\n",
        "        listUnlinkNode(mainThreadProcessingClients[t->id], node);\n"
        "        perfloopHolClientProcessed();\n",
        "processClientsFromIOThread client removal",
    )
    source = replace_once(
        source,
        "    sendPendingClientsToIOThreadIfNeeded(t, 0);\n\n"
        "    return processed;\n"
        "}\n",
        "    sendPendingClientsToIOThreadIfNeeded(t, 0);\n\n"
        "    size_t perfloop_hol_residual = listLength(mainThreadProcessingClients[t->id]);\n"
        "    pthread_mutex_lock(&mainThreadPendingClientsMutexes[t->id]);\n"
        "    perfloop_hol_residual += listLength(mainThreadPendingClients[t->id]);\n"
        "    pthread_mutex_unlock(&mainThreadPendingClientsMutexes[t->id]);\n"
        "    perfloopHolInvocationExit(t->id, server.stat_numcommands, perfloop_hol_residual);\n\n"
        "    return processed;\n"
        "}\n",
        "processClientsFromIOThread exit",
    )
    path.write_text(source + "\n" + IMPLEMENTATION_MARKER)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="path to src/iothread.c in a disposable build worktree")
    args = parser.parse_args()
    instrument(args.source)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
