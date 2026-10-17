import datetime

from ._tokens import Token

AT_TOKEN = 0
AT_TIME = 1
POSITION_FIELDS = 3
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_MICROSECOND = datetime.timedelta(microseconds=1)


def target(when):
    """The native form of an as_of target: a token, or an aware datetime."""
    if isinstance(when, datetime.datetime):
        if when.tzinfo is None or when.utcoffset() is None:
            raise ValueError("as_of needs an aware datetime, such as datetime.now(timezone.utc)")
        micros = (when - _EPOCH) // _MICROSECOND
        if micros < 0:
            raise ValueError("as_of cannot go before 1970")
        return AT_TIME, 0, micros
    if isinstance(when, (str, bytes)):
        raise TypeError("as_of takes a Token or an aware datetime")
    try:
        epoch, lsn = int(when[0]), int(when[1])
    except (TypeError, IndexError, ValueError) as error:
        raise TypeError("as_of takes a Token or an aware datetime") from error
    if epoch < 0 or lsn < 0:
        raise ValueError("a token has a non-negative epoch and lsn")
    return AT_TOKEN, epoch, lsn


def position(values):
    """The token and commit time a past view stands on."""
    token = Token(values[0], values[1])
    micros = values[2]
    return token, (_EPOCH + datetime.timedelta(microseconds=micros)) if micros > 0 else None
