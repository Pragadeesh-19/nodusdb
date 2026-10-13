package io.nodusdb.kernel.closure;

public record ClosureStats(boolean enabled, boolean building, long dirtyNodes, long entries, long entryCap,
                           long incrementalUpdates, long dirtyMarks, long rebuiltNodes, long refusals,
                           long fullPasses, String failure) {
}
