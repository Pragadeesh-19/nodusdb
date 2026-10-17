package io.nodusdb.kernel.memory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

public final class MemoryBudget {

    private static final VarHandle USED;

    static {
        try {
            USED = MethodHandles.lookup().findVarHandle(MemoryBudget.class, "usedBytes", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long limitBytes;
    private final MemoryBudget pool;
    private volatile long usedBytes;

    private MemoryBudget(long limitBytes, MemoryBudget pool) {
        this.limitBytes = limitBytes;
        this.pool = pool;
    }

    public static MemoryBudget unlimited() {
        return new MemoryBudget(Long.MAX_VALUE, null);
    }

    public static MemoryBudget limitedTo(long limitBytes) {
        return new MemoryBudget(requirePositive(limitBytes), null);
    }

    public static MemoryBudget drawingOn(MemoryBudget pool, long limitBytes) {
        Objects.requireNonNull(pool, "pool");
        return new MemoryBudget(limitBytes == Long.MAX_VALUE ? limitBytes : requirePositive(limitBytes), pool);
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
        return bytes <= limitBytes - usedBytes && (pool == null || pool.canCharge(bytes));
    }

    public void require(long bytes) {
        long used = usedBytes;
        if (bytes > limitBytes - used) {
            throw new MemoryLimitExceededException(limitBytes, used, bytes);
        }
        if (pool != null) {
            pool.require(bytes);
        }
    }

    public void charge(long bytes) {
        while (true) {
            long used = usedBytes;
            if (bytes > limitBytes - used) {
                throw new MemoryLimitExceededException(limitBytes, used, bytes);
            }
            if (USED.compareAndSet(this, used, used + bytes)) {
                break;
            }
        }
        if (pool == null) {
            return;
        }
        try {
            pool.charge(bytes);
        } catch (MemoryLimitExceededException refused) {
            USED.getAndAdd(this, -bytes);
            throw refused;
        }
    }

    public void releaseAll() {
        long released = (long) USED.getAndSet(this, 0L);
        if (pool != null && released > 0) {
            pool.release(released);
        }
    }

    private void release(long bytes) {
        USED.getAndAdd(this, -bytes);
    }

    private static long requirePositive(long limitBytes) {
        if (limitBytes < 1) {
            throw new IllegalArgumentException("memory limit must be positive: " + limitBytes);
        }
        return limitBytes;
    }
}
