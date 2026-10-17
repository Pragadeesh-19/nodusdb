import datetime
import typing

from ._tokens import Token

KINDS = {1: "add", 2: "remove", 3: "schema", 4: "epoch", 5: "erase"}
NAMES_PER_CHANGE = 5
NUMBERS_PER_CHANGE = 5
DEFAULT_POLL_TIMEOUT = 1.0
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


class Change(typing.NamedTuple):
    """One committed change.

    kind is "add", "remove", "schema", "epoch" or "erase". token is the commit that holds the change, the same
    token its write returned, and lsn is the change's own position inside that commit. An erased name reads as its
    pseudonym. A change from a transaction of several writes can name a tuple that was already present or absent."""

    kind: str
    object: typing.Union[str, int, None]
    relation: typing.Optional[str]
    subject: typing.Union[str, int, None]
    subject_relation: typing.Optional[str]
    detail: typing.Optional[str]
    schema_version: typing.Optional[int]
    lsn: int
    token: Token
    commit_time: typing.Optional[datetime.datetime]


class ChangeBatch(typing.NamedTuple):
    """Whole transactions committed after a cursor, and the cursor to read from next."""

    changes: typing.Tuple[Change, ...]
    next: Token


class AccessChange(typing.NamedTuple):
    """A registered check whose answer changed. token is the cursor the new answer was checked at."""

    object: str
    permission: str
    subject: str
    allowed: bool
    token: Token


def decode(count, utf8, lengths, numbers, kinds, next_token, integer_keys):
    changes = []
    offset = 0
    for index in range(count):
        names = []
        for field in range(NAMES_PER_CHANGE):
            length = lengths[NAMES_PER_CHANGE * index + field]
            names.append(utf8[offset:offset + length].decode("utf-8"))
            offset += length
        at = NUMBERS_PER_CHANGE * index
        lsn, epoch, commit_lsn, commit_micros, schema_version = (numbers[at + i] for i in range(NUMBERS_PER_CHANGE))
        changes.append(Change(
            kind=KINDS[kinds[index]],
            object=_node(names[0], integer_keys),
            relation=names[1] or None,
            subject=_node(names[2], integer_keys),
            subject_relation=names[3] or None,
            detail=names[4] or None,
            schema_version=schema_version or None,
            lsn=lsn,
            token=Token(epoch, commit_lsn),
            commit_time=_commit_time(commit_micros),
        ))
    return ChangeBatch(tuple(changes), Token(*next_token))


def _node(name, integer_keys):
    if not name:
        return None
    return int(name) if integer_keys else name


def _commit_time(micros):
    if micros <= 0:
        return None
    return _EPOCH + datetime.timedelta(microseconds=micros)


class AccessWatch:
    """Report when the answer to a registered check changes.

    The checks are (object, permission, subject) triples. Each is answered once when the watch starts; that answer is
    the baseline and is not reported. After every batch of committed changes, every triple is checked again at the
    batch's token and the ones whose answer changed are returned. A change that is undone inside one batch is not
    reported. Each poll costs one check per registered triple."""

    def __init__(self, graph, checks, *, after=None):
        triples = []
        for check in checks:
            if not isinstance(check, (tuple, list)) or len(check) != 3:
                raise TypeError("each check is an (object, permission, subject) triple")
            triples.append(tuple(check))
        if not triples:
            raise ValueError("an AccessWatch needs at least one check")
        if len(set(triples)) != len(triples):
            raise ValueError("each check is registered once")
        self._graph = graph
        self._cursor = graph.token if after is None else Token(*after)
        self._allowed = {triple: graph.check(*triple, at_least=self._cursor) for triple in triples}

    @property
    def token(self):
        """The cursor the current answers were checked at."""
        return self._cursor

    def allowed(self, object, permission, subject):
        """The last answer for a registered check."""
        return self._allowed[(object, permission, subject)]

    def poll(self, timeout=DEFAULT_POLL_TIMEOUT):
        """Wait, for at most timeout seconds, for committed changes, and return the checks whose answer changed."""
        batch = self._graph.changes(self._cursor, timeout=timeout)
        if batch.next == self._cursor:
            return []
        self._cursor = batch.next
        if not batch.changes:
            return []
        changed = []
        for triple, before in self._allowed.items():
            now = self._graph.check(*triple, at_least=self._cursor)
            if now != before:
                self._allowed[triple] = now
                changed.append(AccessChange(*triple, allowed=now, token=self._cursor))
        return changed
