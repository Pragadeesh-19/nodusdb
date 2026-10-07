package io.nodusdb.kernel.concurrency;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class WriteSequence {

    private static final VarHandle SEQUENCE;

    static {
        try {
            SEQUENCE = MethodHandles.lookup().findVarHandle(WriteSequence.class, "sequence", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private long sequence;

    public void beginWrite() {
        SEQUENCE.set(this, (long) SEQUENCE.get(this) + 1);
        VarHandle.releaseFence();
    }

    public void endWrite() {
        VarHandle.releaseFence();
        SEQUENCE.setRelease(this, (long) SEQUENCE.get(this) + 1);
    }

    public long beginRead() {
        long started;
        while (((started = (long) SEQUENCE.getAcquire(this)) & 1L) != 0) {
            Thread.onSpinWait();
        }
        return started;
    }

    public boolean validate(long started) {
        VarHandle.acquireFence();
        return (long) SEQUENCE.get(this) == started;
    }
}
