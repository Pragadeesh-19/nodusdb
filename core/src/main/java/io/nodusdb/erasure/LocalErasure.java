package io.nodusdb.erasure;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.ShipWatermark;

final class LocalErasure implements ErasureLayer {

    static final String NAME = "local";

    private final GraphKernel kernel;
    private long rolledFor;

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
        if (kernel.checkpointLsn() < targetLsn) {
            if (due) {
                kernel.checkpoint();
            }
            return;
        }
        ShipWatermark shipping = kernel.shipWatermark();
        if (shipping.configured() && shipping.shippedLsn() < targetLsn) {
            return;
        }
        kernel.trimLog();
        if (!covers(targetLsn, 0) && rolledFor < targetLsn) {
            kernel.checkpoint();
            rolledFor = targetLsn;
        }
    }
}
