"""Act 2: the revocation race's writes land in the lake, are audited there, and are then erased from every copy.

Run it from python/benchmarks against an S3 bucket that has never had versioning:

    python -m act2 --endpoint http://127.0.0.1:8333 --path-style --bucket nodus-test --out act2.json

S3 credentials come from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. The run stops at the first gate that fails
and names it: a fixture that is not AuthzBench's, a bucket that kept versions, a prefix that is not empty, a write
the lake never shows, two audit answers that disagree, a layer the scanner cannot see, or a copy that still holds
the erased name."""

import argparse
import os
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import boto3  # noqa: E402
import duckdb  # noqa: E402
from botocore.config import Config  # noqa: E402

from act2 import audit, bucket, erasure, fixture, freshness, lake, results, scan  # noqa: E402
from nodusdb import Graph, Shipping, environment_credentials, generate_signing_key  # noqa: E402

NANOS_PER_MILLI = 1_000_000
NANOS_PER_SECOND = 1_000_000_000
KEY_ID = 1
ERASURE_POLL_MS = 10
CONFIRM_SQL = ("SELECT DISTINCT txn_lsn FROM iceberg_scan($metadata) "
               "WHERE event = 'add' AND txn_lsn > $low AND txn_lsn <= $high")
UNLINKED_SQL = ("SELECT count(*) FROM iceberg_scan($metadata) "
                "WHERE event = 'remove' AND txn_lsn > $after AND (subject_type = 'erased' OR object_type = 'erased')")


class RunError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    endpoint: str
    region: str
    bucket: str
    prefix: str
    commit_interval_s: int
    rate_per_second: int
    poll_ms: int
    timeout_s: float
    path_style: bool


def run(settings, fixture_directory, work, out):
    data = fixture.read_fixture(fixture_directory)
    connection = _duckdb(settings)
    client = _s3_client(settings)
    versioning = bucket.require_never_versioned(client, settings.bucket)
    objects = bucket.S3Objects(client, settings.bucket, settings.prefix)
    bucket.require_empty(objects)
    sections = {"provenance": results.provenance(connection),
                "fixture": results.fixture_section(data, fixture.schema_digest(data.schema)),
                "bucket": {"endpoint": settings.endpoint, "bucket": settings.bucket, "prefix": settings.prefix}}
    with Graph(path=work / "graph", sync_mode="sync", shipping=_shipping(settings, work)) as graph:
        graph.apply_schema(data.schema)
        sections["freshness"], checkpoint, before, revoke, covering = _land_in_the_lake(
            graph, data, objects, connection, settings)
        sections["audit"] = _audit(graph, data, connection, objects.location(covering.version.key), checkpoint,
                                   before, revoke)
        sections["erasure"] = _erase(graph, data, objects, connection, client, settings, work / "graph",
                                     versioning)
    return results.write_document(out, sections)


def _land_in_the_lake(graph, data, objects, connection, settings):
    watcher = freshness.Watcher(objects, settings.poll_ms * NANOS_PER_MILLI).start()
    try:
        acknowledged = _write_at_rate(graph, data.relationships, settings.rate_per_second)
        watcher.await_lsn(max(lsn for lsn, _ in acknowledged), settings.timeout_s)
        checkpoint = _checkpoint(graph, settings.timeout_s)
        before = graph.token
        revoke = graph.remove_tuple(data.revoke.object, data.revoke.relation, data.revoke.subject,
                                    data.revoke.subject_relation)
        covering = watcher.await_lsn(revoke.lsn, settings.timeout_s)
    finally:
        watcher.stop()
    observations = watcher.observations()
    spread = freshness.Spread.of(freshness.freshness_ns(acknowledged, observations))
    _confirm_rows(connection, objects, observations, acknowledged)
    return (results.freshness_section(spread, settings, observations, watcher.reads()), checkpoint, before, revoke,
            covering)


def _audit(graph, data, connection, location, checkpoint, before, revoke):
    users = data.users()
    instants = [audit.audit(graph, connection, location, "before the revoke", before, data.probe.object, users),
                audit.audit(graph, connection, location, "after the revoke", revoke, data.probe.object, users)]
    audit.require_flip(*instants, data.probe.subject)
    return results.audit_section(data.probe.object, instants, users, checkpoint)


def _erase(graph, data, objects, connection, client, settings, graph_directory, versioning):
    name, grants = data.busiest_user_after_the_revoke()
    _checkpoint(graph, settings.timeout_s)
    found_before = scan.scan(name, graph_directory, objects)
    scan.require_every_layer(found_before, name)
    erased_after = graph.token.lsn
    _, times = erasure.erase(graph, name, grants, settings.timeout_s, ERASURE_POLL_MS * NANOS_PER_MILLI)
    _confirm_unlinked(connection, objects, erased_after, grants)
    found_after = scan.scan(name, graph_directory, objects)
    scan.require_nowhere(found_after, name)
    leftover = bucket.require_no_leftovers(client, settings.bucket, settings.prefix)
    return results.erasure_section(times, found_before, found_after, versioning, leftover)


def _checkpoint(graph, timeout_s, clock=time.perf_counter_ns):
    started = clock()
    graph.checkpoint()
    checkpointed = clock()
    lsn = graph.token.lsn
    _await_snapshot_reference(graph, lsn, timeout_s)
    return {"lsn": lsn, "checkpoint_ms": (checkpointed - started) / NANOS_PER_MILLI,
            "until_referenced_ms": (clock() - started) / NANOS_PER_MILLI}


def _confirm_unlinked(connection, objects, after_lsn, expected):
    location = objects.location(lake.newest_version(objects).key)
    unlinked = connection.execute(UNLINKED_SQL, {"metadata": location, "after": after_lsn}).fetchone()[0]
    if unlinked != expected:
        raise RunError(f"the erasure should remove {expected} grants, but the lake shows {unlinked} removed")


def _write_at_rate(graph, relationships, rate_per_second, clock=time.perf_counter_ns):
    acknowledged = []
    interval_ns = NANOS_PER_SECOND // rate_per_second
    first_due = clock()
    for index, relationship in enumerate(relationships):
        remaining = first_due + index * interval_ns - clock()
        if remaining > 0:
            time.sleep(remaining / NANOS_PER_SECOND)
        token = graph.add_tuple(relationship.object, relationship.relation, relationship.subject,
                                relationship.subject_relation)
        acknowledged.append((token.lsn, clock()))
    return acknowledged


def _confirm_rows(connection, objects, observations, acknowledged):
    written = sorted(lsn for lsn, _ in acknowledged)
    low = 0
    for observation in observations:
        high = observation.version.projected_lsn
        expected = {lsn for lsn in written if low < lsn <= high}
        if expected:
            found = {row[0] for row in connection.execute(
                CONFIRM_SQL, {"metadata": objects.location(observation.version.key), "low": low, "high": high})
                .fetchall()}
            missing = sorted(expected - found)
            if missing:
                raise RunError(f"table version {observation.version.number} claims LSN {high} but DuckDB finds no row "
                               f"for {len(missing)} writes, first at LSN {missing[0]}")
        low = high
    if low < written[-1]:
        raise RunError(f"no observed table version reaches the last write at LSN {written[-1]}")


def _await_snapshot_reference(graph, lsn, timeout_s):
    deadline = time.monotonic() + timeout_s
    while graph.stats()["shipping"]["snapshot_references"]["lsn"] < lsn:
        if time.monotonic() > deadline:
            raise RunError(f"after {timeout_s} s the bucket references no snapshot at or after LSN {lsn}")
        time.sleep(0.05)


def _duckdb(settings):
    connection = duckdb.connect()
    for extension in ("iceberg", "httpfs"):
        connection.execute(f"INSTALL {extension}")
        connection.execute(f"LOAD {extension}")
    endpoint = urlparse(settings.endpoint)
    secret = {"KEY_ID": os.environ["AWS_ACCESS_KEY_ID"], "SECRET": os.environ["AWS_SECRET_ACCESS_KEY"],
              "REGION": settings.region, "ENDPOINT": endpoint.netloc,
              "URL_STYLE": "path" if settings.path_style else "vhost"}
    options = ", ".join(f"{key} {_sql_text(value)}" for key, value in secret.items())
    connection.execute(f"CREATE SECRET act2 (TYPE s3, {options}, USE_SSL {str(endpoint.scheme == 'https').lower()})")
    return connection


def _s3_client(settings):
    return boto3.client("s3", endpoint_url=settings.endpoint, region_name=settings.region,
                        config=Config(s3={"addressing_style": "path" if settings.path_style else "virtual"}))


def _shipping(settings, work):
    private_key, public_key = work / "signing.pem", work / "signing.pub"
    generate_signing_key(private_key, public_key)
    return Shipping.s3(settings.bucket, settings.region, prefix=settings.prefix, endpoint=settings.endpoint,
                       path_style=settings.path_style, key_file=private_key, key_id=KEY_ID, public_key_file=public_key,
                       credentials=environment_credentials(), iceberg=True,
                       iceberg_commit_interval_s=settings.commit_interval_s)


def _sql_text(value):
    return "'" + str(value).replace("'", "''") + "'"


def _arguments(argv):
    parser = argparse.ArgumentParser(prog="act2", description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--endpoint", required=True, help="S3 endpoint, e.g. http://127.0.0.1:8333")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--prefix", default=None, help="an empty prefix; a fresh one is made when omitted")
    parser.add_argument("--path-style", action="store_true", help="address the bucket in the path, as SeaweedFS needs")
    parser.add_argument("--commit-interval", type=int, default=60, help="Iceberg commit interval in seconds")
    parser.add_argument("--rate", type=int, default=10, help="writes per second")
    parser.add_argument("--poll-ms", type=int, default=50, help="how often the lake reader looks, in milliseconds")
    parser.add_argument("--timeout", type=float, default=None, help="longest wait for any step, in seconds")
    parser.add_argument("--fixture", type=Path, default=fixture.DIRECTORY)
    parser.add_argument("--work", type=Path, default=None, help="kept after the run when given")
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv=None):
    arguments = _arguments(argv)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    settings = Settings(
        endpoint=arguments.endpoint, region=arguments.region, bucket=arguments.bucket,
        prefix=arguments.prefix or f"act2/{stamp}-{uuid.uuid4().hex[:8]}",
        commit_interval_s=arguments.commit_interval, rate_per_second=arguments.rate, poll_ms=arguments.poll_ms,
        timeout_s=arguments.timeout or max(120.0, 3.0 * arguments.commit_interval), path_style=arguments.path_style)
    if arguments.work is not None:
        arguments.work.mkdir(parents=True, exist_ok=True)
        document = run(settings, arguments.fixture, arguments.work, arguments.out)
    else:
        with tempfile.TemporaryDirectory(prefix="act2-") as work:
            document = run(settings, arguments.fixture, Path(work), arguments.out)
    writes = document["freshness"]["writes"]
    print(f"freshness at {settings.commit_interval_s} s: p50 {writes['p50_ms']:.0f} ms, p99 {writes['p99_ms']:.0f} ms, "
          f"max {writes['max_ms']:.0f} ms over {writes['count']} writes")
    for instant in document["audit"]["instants"]:
        print(f"audit {instant['label']}: {instant['viewers']} viewers; as_of {instant['as_of_build_ms']:.1f} ms "
              f"build + {instant['as_of_check_ms']:.1f} ms for {instant['as_of_checks']} checks; "
              f"SQL {instant['sql_ms']:.1f} ms")
    erased = document["erasure"]
    print(f"erase {erased['name']} ({erased['tuples']} grants) {erased['erase_ms']:.1f} ms, wait_erased "
          f"{erased['wait_erased_ms']:.0f} ms, layers {erased['layers_ms']}; found before {erased['found_before']}, "
          f"after {erased['found_after']}")


if __name__ == "__main__":
    main()
