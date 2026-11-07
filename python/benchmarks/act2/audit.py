"""Who could view an object at a commit, answered through as_of and through SQL over the lake; the two must agree."""

import time
from dataclasses import dataclass
from pathlib import Path

SQL = Path(__file__).with_name("audit.sql").read_text(encoding="utf-8")
PERMISSION = "view"


class AuditError(RuntimeError):
    pass


REPLAY_FIELDS = ("snapshot_lsn", "replayed_objects", "replayed_records")


@dataclass(frozen=True)
class PastAnswer:
    viewers: frozenset[str]
    build_ns: int
    check_ns: int
    checks: int
    replay: dict[str, int]


@dataclass(frozen=True)
class LakeAnswer:
    viewers: frozenset[str]
    query_ns: int


@dataclass(frozen=True)
class Instant:
    label: str
    lsn: int
    past: PastAnswer
    lake: LakeAnswer


def through_as_of(graph, token, object, users, clock=time.perf_counter_ns):
    started = clock()
    with graph.as_of(token) as view:
        built = clock()
        viewers = frozenset(user for user in users if view.check(object, PERMISSION, user))
        checked = clock()
        replay = {field: view.stats()["as_of"][field] for field in REPLAY_FIELDS}
    return PastAnswer(viewers, built - started, checked - built, len(users), replay)


def through_sql(connection, metadata_location, lsn, object, clock=time.perf_counter_ns):
    object_type, object_id = object.split(":", 1)
    parameters = {"metadata": metadata_location, "lsn": lsn, "object_type": object_type, "object_id": object_id}
    started = clock()
    rows = connection.execute(SQL, parameters).fetchall()
    return LakeAnswer(frozenset(row[0] for row in rows), clock() - started)


def audit(graph, connection, metadata_location, label, token, object, users):
    instant = Instant(label, token.lsn, through_as_of(graph, token, object, users),
                      through_sql(connection, metadata_location, token.lsn, object))
    require_agreement(instant)
    return instant


def require_agreement(instant):
    past, lake = instant.past.viewers, instant.lake.viewers
    if past != lake:
        raise AuditError(f"{instant.label} at LSN {instant.lsn}: as_of finds {len(past)} viewers and SQL finds "
                         f"{len(lake)}; only as_of: {sorted(past - lake)[:5]}, only SQL: {sorted(lake - past)[:5]}")


def require_flip(before, after, revoked):
    if revoked not in before.past.viewers:
        raise AuditError(f"{revoked} could not view before the revoke, at LSN {before.lsn}")
    if revoked in after.past.viewers:
        raise AuditError(f"{revoked} could still view after the revoke, at LSN {after.lsn}")
