"""Erase one name and time it: the call, the wait for every copy, and when each layer reports itself scrubbed."""

import threading
import time
from dataclasses import dataclass

LAYERS = ("local", "bucket", "lake")


class ErasureError(RuntimeError):
    pass


@dataclass(frozen=True)
class ErasureTimes:
    name: str
    tuples: int
    erase_ns: int
    wait_ns: int
    layers_ns: dict[str, int]
    poll_ns: int


def erase(graph, name, tuples, timeout_s, poll_ns, clock=time.perf_counter_ns):
    unconfigured = [layer for layer in LAYERS if not graph.stats()["erasure"][layer]["configured"]]
    if unconfigured:
        raise ErasureError(f"layers not configured, so never scrubbed: {', '.join(unconfigured)}; nothing was erased")
    cleared = {}
    stopping = threading.Event()
    started = clock()
    token = graph.erase(name)
    erased = clock()

    def observe():
        layers = graph.stats()["erasure"]
        for layer in LAYERS:
            if layer not in cleared and not layers[layer]["pending"]:
                cleared[layer] = clock() - started

    def watch():
        while not stopping.is_set() and len(cleared) < len(LAYERS):
            observe()
            stopping.wait(poll_ns / 1e9)

    watcher = threading.Thread(target=watch, name="act2-erasure-layers", daemon=True)
    watcher.start()
    try:
        graph.wait_erased(token, timeout=timeout_s)
        waited = clock()
    finally:
        stopping.set()
        watcher.join()
    observe()
    return token, ErasureTimes(name, tuples, erased - started, waited - started, dict(cleared), poll_ns)
