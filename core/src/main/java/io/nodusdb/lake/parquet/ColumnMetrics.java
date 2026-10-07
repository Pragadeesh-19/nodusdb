package io.nodusdb.lake.parquet;

public record ColumnMetrics(int fieldId, String name, long valueCount, long nullCount, long compressedBytes,
                            byte[] lowerBound, byte[] upperBound) {

    public boolean hasBounds() {
        return lowerBound != null && upperBound != null;
    }
}
