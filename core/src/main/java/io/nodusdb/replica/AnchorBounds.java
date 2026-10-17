package io.nodusdb.replica;

public record AnchorBounds(long minSnapshotLsn, long maxSnapshotLsn, long maxCommitMicros) {

    public static final AnchorBounds NONE = new AnchorBounds(0, Long.MAX_VALUE, Long.MAX_VALUE);

    public AnchorBounds {
        if (minSnapshotLsn < 0 || maxSnapshotLsn < minSnapshotLsn || maxCommitMicros < 0) {
            throw new IllegalArgumentException("anchor bounds need 0 <= min <= max and a non-negative time: "
                    + minSnapshotLsn + ", " + maxSnapshotLsn + ", " + maxCommitMicros);
        }
    }

    boolean admits(long snapshotLsn) {
        return snapshotLsn >= minSnapshotLsn && snapshotLsn <= maxSnapshotLsn;
    }

    boolean boundsTime() {
        return maxCommitMicros != Long.MAX_VALUE;
    }
}
