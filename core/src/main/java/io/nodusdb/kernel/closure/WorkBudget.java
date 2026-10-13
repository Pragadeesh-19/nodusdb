package io.nodusdb.kernel.closure;

final class WorkBudget {

    private long remaining;

    void reset(long units) {
        remaining = units;
    }

    boolean exhausted() {
        return remaining <= 0;
    }

    boolean spend(long units) {
        remaining -= units;
        return remaining >= 0;
    }
}
