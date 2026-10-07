package io.nodusdb.log;

import java.util.List;

public record RecoveryResult(long lastLsn, long lastCommitMicros, List<Long> segmentBases, long tailOffset,
                             long discardedRecords, long truncatedBytes) {

    public RecoveryResult {
        segmentBases = List.copyOf(segmentBases);
    }

    public RecoveryResult withCommitMicrosFloor(long floor) {
        return new RecoveryResult(lastLsn, Math.max(lastCommitMicros, floor), segmentBases, tailOffset,
                discardedRecords, truncatedBytes);
    }

    public boolean hasSegments() {
        return !segmentBases.isEmpty();
    }

    public long tailBase() {
        return segmentBases.get(segmentBases.size() - 1);
    }
}
