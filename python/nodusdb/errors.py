class NodusError(RuntimeError):
    code = -1


class NodusMemoryError(NodusError, MemoryError):
    """The graph's native memory limit would be exceeded. Existing data is unchanged."""

    code = -3


class NodusStaleReadError(NodusError):
    """The graph has not yet applied the write the token names."""

    code = -4


class NodusCheckDepthError(NodusError):
    """A check reached its depth limit before it could decide."""

    code = -5


class NodusSchemaError(NodusError):
    """The schema or a tuple violates the schema. Nothing changed."""

    code = -6


class NodusLogBacklogError(NodusError):
    """The unshipped log reached its cap."""

    code = -7


class NodusWriterFencedError(NodusError):
    """A newer writer exists for this graph."""

    code = -8


class NodusCorruptLogError(NodusError):
    """The log is damaged inside the part known to be durable."""

    code = -9


class NodusUnsupportedError(NodusError):
    """The operation is not available in this mode or version."""

    code = -10


class NodusUpgradeRequiredError(NodusError):
    """The directory was written by an earlier version; run upgrade() first."""

    code = -11


class NodusIndeterminateError(NodusError):
    """A log force failed, so the write may or may not have survived; reopen the graph to find out."""

    code = -12


class NodusTokenLostError(NodusError):
    """The write the token names was acknowledged but never became durable."""

    code = -13


class NodusShipTimeoutError(NodusError):
    """The write is applied and locally durable but had not reached the object store when the wait ended."""

    code = -14
    token = None


class NodusChainTrustError(NodusError):
    """The object store holds chain data that fails verification: a bad signature, an unknown signer, a broken link, a rollback or a fork."""

    code = -15


_BY_CODE = {
    error.code: error
    for error in (
        NodusMemoryError, NodusStaleReadError, NodusCheckDepthError, NodusSchemaError, NodusLogBacklogError,
        NodusWriterFencedError, NodusCorruptLogError, NodusUnsupportedError, NodusUpgradeRequiredError,
        NodusIndeterminateError, NodusTokenLostError, NodusShipTimeoutError, NodusChainTrustError,
    )
}


def error_for(code, message):
    return _BY_CODE.get(code, NodusError)(message)
