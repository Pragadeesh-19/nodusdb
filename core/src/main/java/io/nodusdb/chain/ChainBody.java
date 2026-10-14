package io.nodusdb.chain;

import java.util.Objects;

public sealed interface ChainBody permits ChainBody.Records, ChainBody.SnapshotRef, ChainBody.Redaction {

    ChainKind kind();

    record Records(long lsnFirst, long lsnLast, byte[] records) implements ChainBody {

        public Records {
            Objects.requireNonNull(records, "records");
            if (lsnFirst < 1 || lsnLast < lsnFirst) {
                throw new IllegalArgumentException("a record range needs 1 <= first <= last, got " + lsnFirst + ".."
                        + lsnLast);
            }
        }

        @Override
        public ChainKind kind() {
            return ChainKind.RECORDS;
        }
    }

    record SnapshotRef(String path, ChainHash sha256, long lsn) implements ChainBody {

        public SnapshotRef {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(sha256, "sha256");
            if (lsn < 0) {
                throw new IllegalArgumentException("a snapshot LSN must not be negative: " + lsn);
            }
        }

        @Override
        public ChainKind kind() {
            return ChainKind.SNAPSHOT_REF;
        }
    }

    record Redaction(long eraseLsn, int erasedCount, long firstKeptSeq, long snapshotLsn, long lastLsn)
            implements ChainBody {

        public Redaction {
            if (eraseLsn < 1 || erasedCount < 1 || firstKeptSeq < 1 || snapshotLsn < eraseLsn || lastLsn < 0) {
                throw new IllegalArgumentException("a redaction needs a positive erase LSN, count and kept object, "
                        + "and a snapshot at or after the erasure");
            }
        }

        @Override
        public ChainKind kind() {
            return ChainKind.REDACTION;
        }
    }
}
