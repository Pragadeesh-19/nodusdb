package io.nodusdb.kernel.memory;

import io.nodusdb.error.ErrorCode;
import io.nodusdb.error.NodusException;

public final class MemoryLimitExceededException extends NodusException {

    private static final long serialVersionUID = 1L;

    MemoryLimitExceededException(long limitBytes, long usedBytes, long requestedBytes) {
        super(ErrorCode.MEMORY_LIMIT, "native memory limit of " + limitBytes + " bytes exceeded: " + usedBytes
                + " bytes in use, " + requestedBytes + " more requested");
    }
}
