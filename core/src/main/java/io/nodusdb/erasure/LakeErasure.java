package io.nodusdb.erasure;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.ship.ShipState;

import java.util.Objects;

public final class LakeErasure implements ErasureLayer {

    static final String NAME = "lake";

    private final GraphKernel kernel;
    private final ShipState state;

    public LakeErasure(GraphKernel kernel, ShipState state) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.state = Objects.requireNonNull(state, "state");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public long coveredLsn() {
        return state.lakeCoverage().eraseLsn();
    }

    @Override
    public boolean covers(long requiredLsn, int erasedSymbols) {
        ShipState.LakeCoverage covered = state.lakeCoverage();
        return covered.erasedCount() >= erasedSymbols || covered.eraseLsn() >= requiredLsn;
    }

    @Override
    public void advance(long targetLsn, boolean due) {
        int erased = kernel.erasedSymbols();
        state.requestLakeErasure(new ShipState.LakeRequest(kernel.newestEraseLsn(), erased));
    }
}
