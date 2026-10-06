package io.nodusdb.kernel;

/*
 * Counts the native bytes a kernel has allocated and refuses a request that would pass the
 * limit. Native chunks are never returned, so the count only grows. One budget is shared by
 * every structure of a kernel, so a single limit covers all of them. Only the writer thread
 * charges; the count is volatile so another thread can read it.
 */
final class MemoryBudget {

    private final long limitBytes;
    private volatile long usedBytes;

    private MemoryBudget(long limitBytes) {
        this.limitBytes = limitBytes;
    }

    static MemoryBudget unlimited() {
        return new MemoryBudget(Long.MAX_VALUE);
    }

    static MemoryBudget limitedTo(long limitBytes) {
        if (limitBytes < 1) {
            throw new IllegalArgumentException("memory limit must be positive: " + limitBytes);
        }
        return new MemoryBudget(limitBytes);
    }

    boolean isLimited() {
        return limitBytes != Long.MAX_VALUE;
    }

    long limit() {
        return limitBytes;
    }

    long used() {
        return usedBytes;
    }

    boolean canCharge(long bytes) {
        return bytes <= limitBytes - usedBytes;
    }

    void require(long bytes) {
        long used = usedBytes;
        if (bytes > limitBytes - used) {
            throw new MemoryLimitExceededException(limitBytes, used, bytes);
        }
    }

    void charge(long bytes) {
        require(bytes);
        usedBytes += bytes;
    }
}
