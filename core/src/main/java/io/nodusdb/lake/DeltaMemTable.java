package io.nodusdb.lake;

import io.nodusdb.kernel.LongIntIndex;

import java.util.Arrays;
import java.util.Objects;

/*
 * Columnar write absorber keyed by a 64-bit key hash. Rows 0..rowCount-1 are always dense.
 *
 *   keyHashes          [k0 k1 k2 ... k(n-1) | free ]
 *   longColumns[c]     [v0 v1 v2 ... v(n-1) | free ]   doubles are stored as raw bits
 *   intColumns[c]      [i0 i1 i2 ... i(n-1) | free ]
 *   varCharOffsets[c]  [o0 o1 o2 ... o(n-1) | free ]  -> position in varCharSlab
 *   varCharLengths[c]  [l0 l1 l2 ... l(n-1) | free ]
 *   varCharSlab        [ live bytes ... | dead bytes | free ]
 *   index              keyHash -> row, the only structure that maps keys to rows
 *
 * Delete moves the last row into the vacated slot across every column array, repoints the
 * moved key in the index, then removes the deleted key with backward-shift deletion.
 * Overwritten and deleted variable-width bytes stay in the slab until the next rebuild, which
 * copies live bytes into a spare buffer when the slab runs out of room.
 */
public final class DeltaMemTable {

    public static final int ABSENT = -1;

    public static final int MAX_ROWS = 1 << 29;

    static final int MAX_SLAB_BYTES = 1 << 30;

    private static final byte[] EMPTY_SLAB = new byte[0];

    public record Schema(int longColumns, int intColumns, int varCharColumns) {

        public Schema {
            if (longColumns < 0 || intColumns < 0 || varCharColumns < 0) {
                throw new IllegalArgumentException("column counts must be non-negative");
            }
        }
    }

    private final Schema schema;
    private final LongIntIndex index;
    private final long[][] longColumns;
    private final int[][] intColumns;
    private final int[][] varCharOffsets;
    private final int[][] varCharLengths;
    private long[] keyHashes;
    private byte[] varCharSlab;
    private byte[] spareSlab = EMPTY_SLAB;
    private int slabUsed;
    private long liveVarCharBytes;
    private int rowCount;

    public DeltaMemTable(Schema schema, int initialCapacity, int initialSlabBytes) {
        this.schema = Objects.requireNonNull(schema, "schema");
        if (initialCapacity < 1 || initialCapacity > MAX_ROWS || Integer.bitCount(initialCapacity) != 1) {
            throw new IllegalArgumentException("initial capacity must be a power of two in [1, 2^29]: " + initialCapacity);
        }
        if (initialSlabBytes < 1 || initialSlabBytes > MAX_SLAB_BYTES || Integer.bitCount(initialSlabBytes) != 1) {
            throw new IllegalArgumentException(
                    "initial slab size must be a power of two in [1, 2^30]: " + initialSlabBytes);
        }
        this.index = new LongIntIndex(initialCapacity << 1);
        this.keyHashes = new long[initialCapacity];
        this.longColumns = new long[schema.longColumns()][];
        this.intColumns = new int[schema.intColumns()][];
        this.varCharOffsets = new int[schema.varCharColumns()][];
        this.varCharLengths = new int[schema.varCharColumns()][];
        for (int c = 0; c < schema.longColumns(); c++) {
            longColumns[c] = new long[initialCapacity];
        }
        for (int c = 0; c < schema.intColumns(); c++) {
            intColumns[c] = new int[initialCapacity];
        }
        for (int c = 0; c < schema.varCharColumns(); c++) {
            varCharOffsets[c] = new int[initialCapacity];
            varCharLengths[c] = new int[initialCapacity];
        }
        this.varCharSlab = new byte[initialSlabBytes];
    }

    public boolean upsert(long keyHash, long[] longValues, int[] intValues,
                          byte[] varCharValues, int[] varCharValueLengths) {
        Objects.requireNonNull(longValues, "longValues");
        Objects.requireNonNull(intValues, "intValues");
        Objects.requireNonNull(varCharValues, "varCharValues");
        Objects.requireNonNull(varCharValueLengths, "varCharValueLengths");
        checkArity(longValues.length, schema.longColumns(), "longValues");
        checkArity(intValues.length, schema.intColumns(), "intValues");
        checkArity(varCharValueLengths.length, schema.varCharColumns(), "varCharValueLengths");
        int totalBytes = totalLength(varCharValueLengths, varCharValues.length);

        int row = index.get(keyHash);
        if (row != ABSENT) {
            overwrite(row, longValues, intValues, varCharValues, varCharValueLengths, totalBytes);
            return false;
        }
        insert(keyHash, longValues, intValues, varCharValues, varCharValueLengths, totalBytes);
        return true;
    }

    public boolean delete(long keyHash) {
        int row = index.get(keyHash);
        if (row == ABSENT) {
            return false;
        }
        int lastRow = rowCount - 1;
        for (int c = 0; c < varCharLengths.length; c++) {
            liveVarCharBytes -= varCharLengths[c][row];
        }
        if (row != lastRow) {
            moveRow(lastRow, row);
        }
        index.remove(keyHash);
        rowCount = lastRow;
        return true;
    }

    public int getRow(long keyHash) {
        return index.get(keyHash);
    }

    public int size() {
        return rowCount;
    }

    public long keyHashAt(int row) {
        checkRow(row);
        return keyHashes[row];
    }

    public long longAt(int column, int row) {
        checkRow(row);
        return longColumns[column][row];
    }

    public double doubleAt(int column, int row) {
        return Double.longBitsToDouble(longAt(column, row));
    }

    public int intAt(int column, int row) {
        checkRow(row);
        return intColumns[column][row];
    }

    public int varCharLength(int column, int row) {
        checkRow(row);
        return varCharLengths[column][row];
    }

    public int copyVarChar(int column, int row, byte[] destination, int destinationOffset) {
        checkRow(row);
        int length = varCharLengths[column][row];
        System.arraycopy(varCharSlab, varCharOffsets[column][row], destination, destinationOffset, length);
        return length;
    }

    int slabCapacity() {
        return varCharSlab.length;
    }

    public void assertInvariant() {
        if (index.size() != rowCount) {
            throw new IllegalStateException("index size " + index.size() + " != rowCount " + rowCount);
        }
        if (slabUsed > varCharSlab.length) {
            throw new IllegalStateException("slabUsed " + slabUsed + " exceeds slab " + varCharSlab.length);
        }
        long liveBytes = 0;
        for (int row = 0; row < rowCount; row++) {
            if (index.get(keyHashes[row]) != row) {
                throw new IllegalStateException(
                        "keyHashes[" + row + "]=" + keyHashes[row] + " is indexed at " + index.get(keyHashes[row]));
            }
            for (int c = 0; c < varCharLengths.length; c++) {
                int offset = varCharOffsets[c][row];
                int length = varCharLengths[c][row];
                if (offset < 0 || length < 0 || (long) offset + length > slabUsed) {
                    throw new IllegalStateException(
                            "var column " + c + " row " + row + " spans [" + offset + ", +" + length
                                    + ") outside used slab " + slabUsed);
                }
                liveBytes += length;
            }
        }
        if (liveBytes != liveVarCharBytes) {
            throw new IllegalStateException("live bytes " + liveVarCharBytes + " != summed lengths " + liveBytes);
        }
    }

    private void insert(long keyHash, long[] longValues, int[] intValues,
                        byte[] varCharValues, int[] varCharValueLengths, int totalBytes) {
        if (rowCount == MAX_ROWS) {
            throw new IllegalStateException("row capacity limit reached: " + MAX_ROWS);
        }
        ensureSlabRoom(totalBytes);
        if (rowCount == keyHashes.length) {
            growRows();
        }
        int row = rowCount;
        keyHashes[row] = keyHash;
        writeFixedWidth(row, longValues, intValues);
        writeVarChars(row, varCharValues, varCharValueLengths);
        index.put(keyHash, row);
        rowCount++;
    }

    private void overwrite(int row, long[] longValues, int[] intValues,
                           byte[] varCharValues, int[] varCharValueLengths, int totalBytes) {
        ensureSlabRoom(totalBytes);
        for (int c = 0; c < varCharLengths.length; c++) {
            liveVarCharBytes -= varCharLengths[c][row];
        }
        writeFixedWidth(row, longValues, intValues);
        writeVarChars(row, varCharValues, varCharValueLengths);
    }

    private void writeFixedWidth(int row, long[] longValues, int[] intValues) {
        for (int c = 0; c < longColumns.length; c++) {
            longColumns[c][row] = longValues[c];
        }
        for (int c = 0; c < intColumns.length; c++) {
            intColumns[c][row] = intValues[c];
        }
    }

    private void writeVarChars(int row, byte[] varCharValues, int[] varCharValueLengths) {
        int source = 0;
        for (int c = 0; c < varCharLengths.length; c++) {
            int length = varCharValueLengths[c];
            System.arraycopy(varCharValues, source, varCharSlab, slabUsed, length);
            varCharOffsets[c][row] = slabUsed;
            varCharLengths[c][row] = length;
            slabUsed += length;
            liveVarCharBytes += length;
            source += length;
        }
    }

    private void moveRow(int from, int to) {
        long movedKey = keyHashes[from];
        keyHashes[to] = movedKey;
        for (long[] column : longColumns) {
            column[to] = column[from];
        }
        for (int[] column : intColumns) {
            column[to] = column[from];
        }
        for (int c = 0; c < varCharLengths.length; c++) {
            varCharOffsets[c][to] = varCharOffsets[c][from];
            varCharLengths[c][to] = varCharLengths[c][from];
        }
        index.put(movedKey, to);
    }

    private void ensureSlabRoom(int extra) {
        if ((long) slabUsed + extra > varCharSlab.length) {
            rebuildSlab(extra);
        }
    }

    private void rebuildSlab(int extra) {
        long required = liveVarCharBytes + extra;
        if (required > MAX_SLAB_BYTES) {
            throw new IllegalStateException("variable-width slab limit reached: " + MAX_SLAB_BYTES);
        }
        long target = Math.min(2 * required, MAX_SLAB_BYTES);
        int capacity = 1;
        while (capacity < target) {
            capacity <<= 1;
        }
        byte[] destination = spareSlab.length >= capacity ? spareSlab : new byte[capacity];
        int cursor = 0;
        for (int row = 0; row < rowCount; row++) {
            for (int c = 0; c < varCharLengths.length; c++) {
                int length = varCharLengths[c][row];
                System.arraycopy(varCharSlab, varCharOffsets[c][row], destination, cursor, length);
                varCharOffsets[c][row] = cursor;
                cursor += length;
            }
        }
        spareSlab = varCharSlab;
        varCharSlab = destination;
        slabUsed = cursor;
    }

    private void growRows() {
        int newCapacity = keyHashes.length << 1;
        keyHashes = Arrays.copyOf(keyHashes, newCapacity);
        for (int c = 0; c < longColumns.length; c++) {
            longColumns[c] = Arrays.copyOf(longColumns[c], newCapacity);
        }
        for (int c = 0; c < intColumns.length; c++) {
            intColumns[c] = Arrays.copyOf(intColumns[c], newCapacity);
        }
        for (int c = 0; c < varCharLengths.length; c++) {
            varCharOffsets[c] = Arrays.copyOf(varCharOffsets[c], newCapacity);
            varCharLengths[c] = Arrays.copyOf(varCharLengths[c], newCapacity);
        }
    }

    private void checkRow(int row) {
        Objects.checkIndex(row, rowCount);
    }

    private static void checkArity(int actual, int expected, String name) {
        if (actual != expected) {
            throw new IllegalArgumentException(name + " has " + actual + " entries, schema expects " + expected);
        }
    }

    private static int totalLength(int[] lengths, int available) {
        long total = 0;
        for (int length : lengths) {
            if (length < 0) {
                throw new IllegalArgumentException("var-char length must be non-negative: " + length);
            }
            total += length;
        }
        if (total > available) {
            throw new IllegalArgumentException(
                    "var-char lengths sum to " + total + " but only " + available + " bytes were supplied");
        }
        return (int) total;
    }
}
