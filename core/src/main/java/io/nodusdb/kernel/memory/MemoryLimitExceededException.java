package io.nodusdb.kernel.memory;

public final class MemoryLimitExceededException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    MemoryLimitExceededException(long limitBytes, long usedBytes, long requestedBytes) {
        super("native memory limit of " + limitBytes + " bytes exceeded: " + usedBytes
                + " bytes in use, " + requestedBytes + " more requested");
    }
}
