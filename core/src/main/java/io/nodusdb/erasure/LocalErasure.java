package io.nodusdb.erasure;

import io.nodusdb.kernel.GraphKernel;

final class LocalErasure implements ErasureLayer {

    static final String NAME = "local";

    private final GraphKernel kernel;

    LocalErasure(GraphKernel kernel) {
        this.kernel = kernel;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public long coveredLsn() {
        return Math.min(kernel.checkpointLsn(), kernel.oldestLogLsn() - 1);
    }

    @Override
    public boolean covers(long requiredLsn, int erasedSymbols) {
        return coveredLsn() >= requiredLsn;
    }

    @Override
    public void advance(long targetLsn, boolean due) {
        if (kernel.checkpointLsn() >= targetLsn) {
            kernel.trimLog();
        } else if (due) {
            kernel.checkpoint();
        }
    }
}
