"""The writes Act 2 makes: an AuthzBench dataset and the NodusDB schema AuthzBench generated for its model."""

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

FORMAT = "authzbench.dataset/1"
MODEL = "document-sharing"
SCHEMA_FILE = "model.nodus"
SCHEMA_SHA256 = "da2bc59be17a73ed6a1c72fdb667e44fda11dbd259b62539fa0720e3aa092042"
DIRECTORY = Path(__file__).resolve().parent / "fixture"

_NAME = r"[a-z][a-z0-9_]*:[A-Za-z0-9_\-]+"
_WORD = r"[a-z][a-z0-9_]*"
_RELATIONSHIP = re.compile(
    rf"^(?P<object>{_NAME})#(?P<relation>{_WORD})@(?P<subject>{_NAME})(?:#(?P<subject_relation>{_WORD}))?$")
_QUERY = re.compile(rf"^(?P<object>{_NAME})#(?P<permission>{_WORD})@(?P<subject>{_NAME})$")
_ANSWERS = ("allow", "deny")


class FixtureError(ValueError):
    pass


@dataclass(frozen=True)
class Relationship:
    object: str
    relation: str
    subject: str
    subject_relation: str | None = None

    @classmethod
    def parse(cls, text):
        match = _RELATIONSHIP.match(text)
        if match is None:
            raise FixtureError(f"not a relationship: {text!r}")
        return cls(match["object"], match["relation"], match["subject"], match["subject_relation"])

    def names(self):
        return self.object, self.subject


@dataclass(frozen=True)
class Query:
    object: str
    permission: str
    subject: str

    @classmethod
    def parse(cls, text):
        match = _QUERY.match(text)
        if match is None:
            raise FixtureError(f"not a check: {text!r}")
        return cls(match["object"], match["permission"], match["subject"])


@dataclass(frozen=True)
class Fixture:
    scale: str
    seed: int
    schema: str
    relationships: tuple[Relationship, ...]
    revoke: Relationship
    probe: Query

    def users(self):
        return sorted({relationship.subject for relationship in self.relationships
                       if relationship.subject.startswith("user:")})

    def busiest_user_after_the_revoke(self):
        grants = {}
        for relationship in set(self.relationships) - {self.revoke}:
            for name in relationship.names():
                if name.startswith("user:"):
                    grants[name] = grants.get(name, 0) + 1
        return min(grants.items(), key=lambda item: (-item[1], item[0]))


def read_fixture(directory=DIRECTORY):
    directory = Path(directory)
    manifest = json.loads((directory / "dataset.json").read_text(encoding="utf-8"))
    if manifest.get("format") != FORMAT:
        raise FixtureError(f"{directory} holds format {manifest.get('format')!r}, not {FORMAT}")
    if manifest.get("model") != MODEL:
        raise FixtureError(f"{directory} holds model {manifest.get('model')!r}; the audit SQL is written for {MODEL}")
    relationships = tuple(Relationship.parse(line) for line in _lines(directory / "relationships.txt"))
    checks = [_check(line) for line in _lines(directory / "checks.txt")]
    _require_count("relationships", manifest["relationships"], len(relationships))
    _require_count("checks", manifest["checks"], len(checks))
    return Fixture(
        scale=manifest["scale"],
        seed=manifest["seed"],
        schema=_schema(directory / SCHEMA_FILE),
        relationships=relationships,
        revoke=Relationship.parse(manifest["revocation"]["revoke"]),
        probe=Query.parse(manifest["revocation"]["probe"]),
    )


def schema_digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _schema(path):
    text = path.read_text(encoding="utf-8")
    digest = schema_digest(text)
    if digest != SCHEMA_SHA256:
        raise FixtureError(f"{path} has SHA-256 {digest}; the audit SQL is written for the schema with {SCHEMA_SHA256}")
    return text


def _check(line):
    query, _, answer = line.rpartition(" ")
    if answer not in _ANSWERS:
        raise FixtureError(f"not a check with an answer: {line!r}")
    return Query.parse(query)


def _lines(path):
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line]


def _require_count(kind, promised, found):
    if promised != found:
        raise FixtureError(f"the manifest promises {promised} {kind} but the file holds {found}")
