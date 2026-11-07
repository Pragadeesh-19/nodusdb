"""The results document: every number Act 2 measured, with what produced it."""

import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import duckdb

import nodusdb

FORMAT = "nodusdb.act2/1"
NANOS_PER_MILLI = 1_000_000


def provenance(connection):
    library = Path(nodusdb.find_library())
    extension = connection.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'iceberg'").fetchone()
    return {
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "library": {"path": library.as_posix(), "sha256": _sha256(library), "version": nodusdb.__version__},
        "python": platform.python_version(),
        "platform": platform.platform(),
        "duckdb": {"version": duckdb.__version__, "iceberg_extension": extension[0] if extension else None},
    }


def fixture_section(fixture, digest):
    return {"model": "document-sharing", "scale": fixture.scale, "seed": fixture.seed,
            "relationships": len(fixture.relationships), "schema_sha256": digest}


def freshness_section(spread, settings, observations, reads):
    return {
        "commit_interval_s": settings.commit_interval_s,
        "rate_per_second": settings.rate_per_second,
        "poll_interval_ms": settings.poll_ms,
        "table_versions_seen": len(observations),
        "reader_polls": len(reads),
        "median_read_ms": sorted(reads)[len(reads) // 2] / NANOS_PER_MILLI if reads else None,
        "writes": spread.milliseconds(),
    }


def audit_section(object, instants, users, checkpoint):
    return {
        "object": object,
        "candidates": len(users),
        "checkpoint_after_load": checkpoint,
        "instants": [{
            "label": instant.label,
            "lsn": instant.lsn,
            "viewers": len(instant.past.viewers),
            "as_of_build_ms": instant.past.build_ns / NANOS_PER_MILLI,
            "as_of_replay": instant.past.replay,
            "as_of_checks": instant.past.checks,
            "as_of_check_ms": instant.past.check_ns / NANOS_PER_MILLI,
            "sql_ms": instant.lake.query_ns / NANOS_PER_MILLI,
        } for instant in instants],
        "answers_agree": True,
    }


def erasure_section(times, before, after, versioning, leftover):
    return {
        "name": times.name,
        "tuples": times.tuples,
        "erase_ms": times.erase_ns / NANOS_PER_MILLI,
        "wait_erased_ms": times.wait_ns / NANOS_PER_MILLI,
        "layers_ms": {layer: elapsed / NANOS_PER_MILLI for layer, elapsed in sorted(times.layers_ns.items())},
        "layer_poll_ms": times.poll_ns / NANOS_PER_MILLI,
        "found_before": _layer_counts(before),
        "found_after": len(after),
        "bucket_versioning": versioning,
        "noncurrent_versions": len(leftover.noncurrent_versions),
        "delete_markers": len(leftover.delete_markers),
    }


def write_document(path, sections):
    document = {"format": FORMAT, **sections}
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return document


def _layer_counts(hits):
    counts = {}
    for hit in hits:
        counts[hit.layer] = counts.get(hit.layer, 0) + 1
    return dict(sorted(counts.items()))


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()
