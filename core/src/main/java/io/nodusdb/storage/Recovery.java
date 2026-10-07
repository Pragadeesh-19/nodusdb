package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;

public record Recovery(GraphKernel kernel, long recordsReplayed, long discardedRecords, long truncatedBytes) {
}
