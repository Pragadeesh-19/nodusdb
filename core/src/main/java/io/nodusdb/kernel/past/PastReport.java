package io.nodusdb.kernel.past;

import io.nodusdb.kernel.Token;

public record PastReport(Token position, long commitMicros, long snapshotLsn, long replayedObjects,
                         long replayedRecords, long buildMillis, long memoryBytes) {
}
