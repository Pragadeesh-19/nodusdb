package io.nodusdb.kernel.memory;

public final class MemoryBudget {

    private final long limitBytes;
    private volatile long usedBytes;

    private MemoryBudget(long limitBytes) {
        this.limitBytes = limitBytes;
    }

    public static MemoryBudget unlimited() {
        return new MemoryBudget(Long.MAX_VALUE);
    }

    public static MemoryBudget limitedTo(long limitBytes) {
        if (limitBytes < 1) {
            throw new IllegalArgumentException("memory limit must be positive: " + limitBytes);
        }
        return new MemoryBudget(limitBytes);
    }

    public boolean isLimited() {
        return limitBytes != Long.MAX_VALUE;
    }

    public long limit() {
        return limitBytes;
    }

    public long used() {
        return usedBytes;
    }

    public boolean canCharge(long bytes) {
        return bytes <= limitBytes - usedBytes;
    }

    public void require(long bytes) {
        long used = usedBytes;
        if (bytes > limitBytes - used) {
            throw new MemoryLimitExceededException(limitBytes, used, bytes);
        }
    }

    public void charge(long bytes) {
        require(bytes);
        usedBytes += bytes;
    }
}
