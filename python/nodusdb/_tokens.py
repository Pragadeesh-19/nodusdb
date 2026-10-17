import typing


class Token(typing.NamedTuple):
    """The position of a write: the epoch of the writer tenure and the log sequence number."""

    epoch: int
    lsn: int
