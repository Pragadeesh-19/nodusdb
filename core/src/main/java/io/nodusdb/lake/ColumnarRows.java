package io.nodusdb.lake;

/*
 * Column-oriented input for LakeTable.upsertColumns. Each variable-width column follows Arrow's
 * layout: value i of a column spans bytes [varCharStart(column, i), varCharStart(column, i + 1)).
 * Offsets are absolute positions in the column's data, so a slice of a larger array needs no copy.
 */
public interface ColumnarRows {

    int rowCount();

    int longColumnCount();

    int intColumnCount();

    int varCharColumnCount();

    long key(int row);

    long longValue(int column, int row);

    int intValue(int column, int row);

    int varCharStart(int column, int row);

    void copyVarChar(int column, int start, int length, byte[] destination, int destinationOffset);
}
