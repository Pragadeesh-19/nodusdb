package io.nodusdb.kernel.closure;

public record ClosureLimits(boolean enabled, int workBudget, long minimumCap, int capPerTuple) {

    public static final String SWITCH = "NODUS_REACHABILITY_INDEX";
    public static final ClosureLimits DEFAULT = new ClosureLimits(true, 4_096, 1L << 20, 4);

    private static final String OFF = "off";

    public ClosureLimits {
        if (workBudget < 1) {
            throw new IllegalArgumentException("the work budget must be at least 1: " + workBudget);
        }
        if (minimumCap < 0) {
            throw new IllegalArgumentException("the minimum entry cap must not be negative: " + minimumCap);
        }
        if (capPerTuple < 0) {
            throw new IllegalArgumentException("the entries per tuple must not be negative: " + capPerTuple);
        }
    }

    public static ClosureLimits fromEnvironment() {
        return forSwitch(System.getenv(SWITCH));
    }

    static ClosureLimits forSwitch(String value) {
        return value != null && OFF.equalsIgnoreCase(value.trim()) ? DEFAULT.disabled() : DEFAULT;
    }

    public ClosureLimits disabled() {
        return new ClosureLimits(false, workBudget, minimumCap, capPerTuple);
    }

    public ClosureLimits withWorkBudget(int units) {
        return new ClosureLimits(enabled, units, minimumCap, capPerTuple);
    }

    public ClosureLimits withCap(long minimum, int perTuple) {
        return new ClosureLimits(enabled, workBudget, minimum, perTuple);
    }

    public long capFor(long tuples) {
        long scaled = tuples > Long.MAX_VALUE / Math.max(1, capPerTuple) ? Long.MAX_VALUE : tuples * capPerTuple;
        return Math.max(minimumCap, scaled);
    }
}
