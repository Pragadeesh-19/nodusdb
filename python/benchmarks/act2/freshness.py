"""Each write's freshness: from its acknowledgement until an outside reader sees a table version that holds it."""

import math
import threading
import time
from dataclasses import dataclass

from act2 import lake

NANOS_PER_MILLI = 1_000_000


class FreshnessError(RuntimeError):
    pass


@dataclass(frozen=True)
class Observation:
    seen_ns: int
    version: lake.TableVersion


@dataclass(frozen=True)
class Spread:
    count: int
    min_ns: int
    p50_ns: int
    p99_ns: int
    max_ns: int

    @classmethod
    def of(cls, samples):
        if not samples:
            raise FreshnessError("no samples to summarize")
        ordered = sorted(samples)
        return cls(len(ordered), ordered[0], _nearest_rank(ordered, 0.50), _nearest_rank(ordered, 0.99), ordered[-1])

    def milliseconds(self):
        return {"count": self.count, "min_ms": self.min_ns / NANOS_PER_MILLI, "p50_ms": self.p50_ns / NANOS_PER_MILLI,
                "p99_ms": self.p99_ns / NANOS_PER_MILLI, "max_ms": self.max_ns / NANOS_PER_MILLI}


def freshness_ns(acknowledged, observations):
    ordered = sorted(observations, key=lambda observation: observation.seen_ns)
    for earlier, later in zip(ordered, ordered[1:]):
        if later.version.projected_lsn < earlier.version.projected_lsn:
            raise FreshnessError(f"the table went back from LSN {earlier.version.projected_lsn} to "
                                 f"{later.version.projected_lsn}")
    samples = []
    cursor = 0
    for lsn, acknowledged_ns in sorted(acknowledged):
        while cursor < len(ordered) and ordered[cursor].version.projected_lsn < lsn:
            cursor += 1
        if cursor == len(ordered):
            raise FreshnessError(f"no table version a reader saw holds the write at LSN {lsn}")
        samples.append(max(0, ordered[cursor].seen_ns - acknowledged_ns))
    return samples


class Watcher:
    def __init__(self, objects, poll_ns, clock=time.perf_counter_ns):
        self._objects = objects
        self._poll_ns = poll_ns
        self._clock = clock
        self._observations = []
        self._read_ns = []
        self._failure = None
        self._stopping = threading.Event()
        self._changed = threading.Condition()
        self._thread = threading.Thread(target=self._run, name="act2-lake-reader", daemon=True)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stopping.set()
        self._thread.join()
        self._raise_failure()

    def observations(self):
        with self._changed:
            return list(self._observations)

    def reads(self):
        with self._changed:
            return list(self._read_ns)

    def await_lsn(self, lsn, timeout_s):
        deadline = time.monotonic() + timeout_s
        with self._changed:
            while not self._observations or self._observations[-1].version.projected_lsn < lsn:
                self._raise_failure()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    newest = self._observations[-1].version.projected_lsn if self._observations else None
                    raise FreshnessError(f"after {timeout_s} s no table version holds LSN {lsn}; the newest holds "
                                         f"{newest}")
                self._changed.wait(remaining)
            return self._observations[-1]

    def _run(self):
        try:
            while not self._stopping.is_set():
                started = self._clock()
                version = lake.newest_version(self._objects)
                seen = self._clock()
                self._record(version, seen, seen - started)
                self._stopping.wait(max(0, self._poll_ns - (self._clock() - started)) / 1e9)
        except Exception as failure:
            with self._changed:
                self._failure = failure
                self._changed.notify_all()

    def _record(self, version, seen_ns, read_ns):
        with self._changed:
            self._read_ns.append(read_ns)
            if version is not None and (not self._observations or self._observations[-1].version.number
                                        != version.number):
                self._observations.append(Observation(seen_ns, version))
                self._changed.notify_all()

    def _raise_failure(self):
        if self._failure is not None:
            raise FreshnessError(f"the lake reader failed: {self._failure}") from self._failure


def _nearest_rank(ordered, quantile):
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]
