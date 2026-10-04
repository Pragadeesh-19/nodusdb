import hashlib
import json
import os
import statistics
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

CACHE_DIRECTORY = Path(os.environ.get("NODUS_BENCH_CACHE", Path(tempfile.gettempdir()) / "nodusdb-bench"))


def optional_import(module_name):
    try:
        module = __import__(module_name)
    except ImportError as error:
        print(f"WARNING: {module_name} is unavailable and its engines will be skipped: {error}",
              file=sys.stderr, flush=True)
        return None
    return module


def ensure_download(url, destination, sha256=None):
    destination = Path(destination)
    if destination.exists():
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    print(f"downloading {url}", file=sys.stderr, flush=True)
    urllib.request.urlretrieve(url, temporary)
    if sha256 is not None:
        digest = file_sha256(temporary)
        if digest != sha256:
            temporary.unlink()
            raise RuntimeError(f"checksum mismatch for {url}: {digest}")
    temporary.replace(destination)
    return destination


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def directory_bytes(path):
    path = Path(path)
    if path.is_file():
        return path.stat().st_size
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def current_rss_mb():
    import psutil

    return psutil.Process().memory_info().rss / (1 << 20)


def peak_rss_mb():
    import psutil

    info = psutil.Process().memory_info()
    peak = getattr(info, "peak_wset", None)
    if peak is None:
        peak = getattr(info, "peak", None)
    return max(info.rss, peak or 0) / (1 << 20)


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
    return ordered[index]


def latency_summary(microseconds):
    if not microseconds:
        return {"count": 0, "median_us": None, "p99_us": None, "mean_us": None}
    return {
        "count": len(microseconds),
        "median_us": statistics.median(microseconds),
        "p99_us": percentile(microseconds, 0.99),
        "mean_us": statistics.fmean(microseconds),
    }


class Stopwatch:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.seconds = time.perf_counter() - self.start
        return False


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def markdown_table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for row in rows:
        lines.append("| " + " | ".join("n/a" if value is None else str(value) for value in row) + " |")
    return "\n".join(lines)
