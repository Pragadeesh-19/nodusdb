package io.nodusdb.lake;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/*
 * Columnar write absorber keyed by a 64-bit key hash. Rows 0..rowCount-1 are always dense.
 *
 *   keyHashes          [k0 k1 k2 ... k(n-1) | free ]     native, 64-byte aligned
 *   rowKinds           [INSERT/TOMBSTONE per row | free ]
 *   longColumns[c]     [v0 v1 v2 ... v(n-1) | free ]     doubles are stored as raw bits
 *   intColumns[c]      [i0 i1 i2 ... i(n-1) | free ]
 *   varCharOffsets[c]  [o0 o1 o2 ... o(n-1) | free ]     -> position in varCharSlab
 *   varCharLengths[c]  [l0 l1 l2 ... l(n-1) | free ]
 *   varCharSlab        [ live bytes ... | dead bytes | free ]
 *   index              keyHash -> row, the only structure that maps keys to rows
 *
 * upsert writes an INSERT row. tombstone marks a key as deleted without removing it, so that
 * the deletion can be written to a delete file if an older committed row exists. delete removes
 * the row outright, with swap-and-pop across every column.
 */
public final class DeltaMemTable implements AutoCloseable {

    public static final int ABSENT = -1;

    public static final byte INSERT = 0;

    public static final byte TOMBSTONE = 1;

    public static final int MAX_ROWS = 1 << 29;

    static final int MAX_SLAB_BYTES = 1 << 30;

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final ValueLayout.OfInt INT_UNALIGNED = ValueLayout.JAVA_INT_UNALIGNED;

    public record Schema(int longColumns, int intColumns, int varCharColumns) {

        public Schema {
            if (longColumns < 0 || intColumns < 0 || varCharColumns < 0) {
                throw new IllegalArgumentException("column counts must be non-negative");
            }
        }
    }

    private final Schema schema;
    private final NativeKeyIndex index;
    private final NativeColumn keyHashes;
    private final NativeColumn rowKinds;
    private final NativeColumn[] longColumns;
    private final NativeColumn[] intColumns;
    private final NativeColumn[] varCharOffsets;
    private final NativeColumn[] varCharLengths;
    private int rowCapacity;
    private NativeColumn slab;
    private NativeColumn spareSlab;
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
        this.rowCapacity = initialCapacity;
        this.index = new NativeKeyIndex(initialCapacity << 1);
        this.keyHashes = new NativeColumn((long) initialCapacity * Long.BYTES);
        this.rowKinds = new NativeColumn(initialCapacity);
        this.longColumns = new NativeColumn[schema.longColumns()];
        this.intColumns = new NativeColumn[schema.intColumns()];
        this.varCharOffsets = new NativeColumn[schema.varCharColumns()];
        this.varCharLengths = new NativeColumn[schema.varCharColumns()];
        for (int c = 0; c < schema.longColumns(); c++) {
            longColumns[c] = new NativeColumn((long) initialCapacity * Long.BYTES);
        }
        for (int c = 0; c < schema.intColumns(); c++) {
            intColumns[c] = new NativeColumn((long) initialCapacity * Integer.BYTES);
        }
        for (int c = 0; c < schema.varCharColumns(); c++) {
            varCharOffsets[c] = new NativeColumn((long) initialCapacity * Integer.BYTES);
            varCharLengths[c] = new NativeColumn((long) initialCapacity * Integer.BYTES);
        }
        this.slab = new NativeColumn(initialSlabBytes);
    }

    public boolean upsert(long keyHash, long[] longValues, int[] intValues,
                          byte[] varCharValues, int[] varCharValueLengths) {
        return upsert(keyHash, MemorySegment.ofArray(longValues), MemorySegment.ofArray(intValues),
                MemorySegment.ofArray(varCharValues), MemorySegment.ofArray(varCharValueLengths));
    }

    public boolean upsert(long keyHash, MemorySegment longValues, MemorySegment intValues,
                          MemorySegment varCharValues, MemorySegment lengths) {
        Objects.requireNonNull(longValues, "longValues");
        Objects.requireNonNull(intValues, "intValues");
        Objects.requireNonNull(varCharValues, "varCharValues");
        Objects.requireNonNull(lengths, "lengths");
        requireBytes(longValues, schema.longColumns() * (long) Long.BYTES, "longValues");
        requireBytes(intValues, schema.intColumns() * (long) Integer.BYTES, "intValues");
        requireBytes(lengths, schema.varCharColumns() * (long) Integer.BYTES, "lengths");
        int totalBytes = totalLength(lengths, schema.varCharColumns(), varCharValues.byteSize());

        int row = index.find(keyHashes.segment(), keyHash);
        if (row != ABSENT) {
            overwrite(row, longValues, intValues, varCharValues, lengths, totalBytes);
            return false;
        }
        insert(keyHash, longValues, intValues, varCharValues, lengths, totalBytes);
        return true;
    }

    public boolean delete(long keyHash) {
        int row = index.find(keyHashes.segment(), keyHash);
        if (row == ABSENT) {
            return false;
        }
        int lastRow = rowCount - 1;
        for (NativeColumn column : varCharLengths) {
            liveVarCharBytes -= column.segment().getAtIndex(INT, row);
        }
        index.remove(keyHashes.segment(), keyHash);
        if (row != lastRow) {
            moveRow(lastRow, row);
        }
        rowCount = lastRow;
        return true;
    }

    public void tombstone(long keyHash) {
        int row = index.find(keyHashes.segment(), keyHash);
        if (row == ABSENT) {
            appendTombstone(keyHash);
        } else if (kindAt(row) == INSERT) {
            clearVarChars(row);
            rowKinds.segment().set(BYTE, row, TOMBSTONE);
        }
    }

    public void clear() {
        index.clear();
        rowCount = 0;
        slabUsed = 0;
        liveVarCharBytes = 0;
    }

    public int getRow(long keyHash) {
        return index.find(keyHashes.segment(), keyHash);
    }

    public int size() {
        return rowCount;
    }

    public int slabBytesUsed() {
        return slabUsed;
    }

    public long keyHashAt(int row) {
        checkRow(row);
        return keyHashes.segment().getAtIndex(LONG, row);
    }

    public byte kindAt(int row) {
        checkRow(row);
        return rowKinds.segment().get(BYTE, row);
    }

    public long longAt(int column, int row) {
        checkRow(row);
        return longColumns[column].segment().getAtIndex(LONG, row);
    }

    public double doubleAt(int column, int row) {
        return Double.longBitsToDouble(longAt(column, row));
    }

    public int intAt(int column, int row) {
        checkRow(row);
        return intColumns[column].segment().getAtIndex(INT, row);
    }

    public int varCharLength(int column, int row) {
        checkRow(row);
        return varCharLengths[column].segment().getAtIndex(INT, row);
    }

    public int copyVarChar(int column, int row, byte[] destination, int destinationOffset) {
        checkRow(row);
        int length = varCharLengths[column].segment().getAtIndex(INT, row);
        MemorySegment.copy(slab.segment(), BYTE, varCharOffsets[column].segment().getAtIndex(INT, row),
                destination, destinationOffset, length);
        return length;
    }

    int slabCapacity() {
        return Math.toIntExact(slab.capacity());
    }

    MemorySegment keyHashColumn() {
        return keyHashes.segment();
    }

    MemorySegment kindColumn() {
        return rowKinds.segment();
    }

    MemorySegment longColumn(int column) {
        return longColumns[column].segment();
    }

    MemorySegment intColumn(int column) {
        return intColumns[column].segment();
    }

    MemorySegment varCharOffsetColumn(int column) {
        return varCharOffsets[column].segment();
    }

    MemorySegment varCharLengthColumn(int column) {
        return varCharLengths[column].segment();
    }

    MemorySegment varCharSlab() {
        return slab.segment();
    }

    @Override
    public void close() {
        index.close();
        keyHashes.close();
        rowKinds.close();
        closeAll(longColumns);
        closeAll(intColumns);
        closeAll(varCharOffsets);
        closeAll(varCharLengths);
        slab.close();
        if (spareSlab != null) {
            spareSlab.close();
        }
    }

    public void assertInvariant() {
        if (index.size() != rowCount) {
            throw new IllegalStateException("index size " + index.size() + " != rowCount " + rowCount);
        }
        if (slabUsed > slab.capacity()) {
            throw new IllegalStateException("slabUsed " + slabUsed + " exceeds slab " + slab.capacity());
        }
        long liveBytes = 0;
        for (int row = 0; row < rowCount; row++) {
            long key = keyHashAt(row);
            if (index.find(keyHashes.segment(), key) != row) {
                throw new IllegalStateException(
                        "keyHashes[" + row + "]=" + key + " is indexed at " + index.find(keyHashes.segment(), key));
            }
            byte kind = kindAt(row);
            if (kind != INSERT && kind != TOMBSTONE) {
                throw new IllegalStateException("row " + row + " has unknown kind " + kind);
            }
            for (int c = 0; c < varCharLengths.length; c++) {
                int offset = varCharOffsets[c].segment().getAtIndex(INT, row);
                int length = varCharLengths[c].segment().getAtIndex(INT, row);
                if (offset < 0 || length < 0 || (long) offset + length > slabUsed) {
                    throw new IllegalStateException(
                            "var column " + c + " row " + row + " spans [" + offset + ", +" + length
                                    + ") outside used slab " + slabUsed);
                }
                if (kind == TOMBSTONE && length != 0) {
                    throw new IllegalStateException("tombstone row " + row + " carries " + length + " var-char bytes");
                }
                liveBytes += length;
            }
        }
        if (liveBytes != liveVarCharBytes) {
            throw new IllegalStateException("live bytes " + liveVarCharBytes + " != summed lengths " + liveBytes);
        }
    }

    private void insert(long keyHash, MemorySegment longValues, MemorySegment intValues,
                        MemorySegment varCharValues, MemorySegment lengths, int totalBytes) {
        checkRowCapacity();
        ensureSlabRoom(totalBytes);
        if (rowCount == rowCapacity) {
            growRows();
        }
        int row = rowCount;
        keyHashes.segment().setAtIndex(LONG, row, keyHash);
        rowKinds.segment().set(BYTE, row, INSERT);
        writeFixedWidth(row, longValues, intValues);
        writeVarChars(row, varCharValues, lengths);
        index.insert(keyHashes.segment(), keyHash, row);
        rowCount++;
    }

    private void appendTombstone(long keyHash) {
        checkRowCapacity();
        if (rowCount == rowCapacity) {
            growRows();
        }
        int row = rowCount;
        keyHashes.segment().setAtIndex(LONG, row, keyHash);
        rowKinds.segment().set(BYTE, row, TOMBSTONE);
        for (NativeColumn column : varCharOffsets) {
            column.segment().setAtIndex(INT, row, 0);
        }
        for (NativeColumn column : varCharLengths) {
            column.segment().setAtIndex(INT, row, 0);
        }
        index.insert(keyHashes.segment(), keyHash, row);
        rowCount++;
    }

    private void overwrite(int row, MemorySegment longValues, MemorySegment intValues,
                           MemorySegment varCharValues, MemorySegment lengths, int totalBytes) {
        ensureSlabRoom(totalBytes);
        clearVarChars(row);
        rowKinds.segment().set(BYTE, row, INSERT);
        writeFixedWidth(row, longValues, intValues);
        writeVarChars(row, varCharValues, lengths);
    }

    private void clearVarChars(int row) {
        for (int c = 0; c < varCharLengths.length; c++) {
            liveVarCharBytes -= varCharLengths[c].segment().getAtIndex(INT, row);
            varCharLengths[c].segment().setAtIndex(INT, row, 0);
            varCharOffsets[c].segment().setAtIndex(INT, row, 0);
        }
    }

    private void writeFixedWidth(int row, MemorySegment longValues, MemorySegment intValues) {
        for (int c = 0; c < longColumns.length; c++) {
            MemorySegment.copy(longValues, c * (long) Long.BYTES, longColumns[c].segment(),
                    row * (long) Long.BYTES, Long.BYTES);
        }
        for (int c = 0; c < intColumns.length; c++) {
            MemorySegment.copy(intValues, c * (long) Integer.BYTES, intColumns[c].segment(),
                    row * (long) Integer.BYTES, Integer.BYTES);
        }
    }

    private void writeVarChars(int row, MemorySegment varCharValues, MemorySegment lengths) {
        long source = 0;
        for (int c = 0; c < varCharLengths.length; c++) {
            int length = lengths.get(INT_UNALIGNED, c * (long) Integer.BYTES);
            MemorySegment.copy(varCharValues, source, slab.segment(), slabUsed, length);
            varCharOffsets[c].segment().setAtIndex(INT, row, slabUsed);
            varCharLengths[c].segment().setAtIndex(INT, row, length);
            slabUsed += length;
            liveVarCharBytes += length;
            source += length;
        }
    }

    private void moveRow(int from, int to) {
        long movedKey = keyHashes.segment().getAtIndex(LONG, from);
        keyHashes.segment().setAtIndex(LONG, to, movedKey);
        rowKinds.segment().set(BYTE, to, rowKinds.segment().get(BYTE, from));
        for (NativeColumn column : longColumns) {
            column.segment().setAtIndex(LONG, to, column.segment().getAtIndex(LONG, from));
        }
        for (NativeColumn column : intColumns) {
            column.segment().setAtIndex(INT, to, column.segment().getAtIndex(INT, from));
        }
        for (int c = 0; c < varCharLengths.length; c++) {
            varCharOffsets[c].segment().setAtIndex(INT, to, varCharOffsets[c].segment().getAtIndex(INT, from));
            varCharLengths[c].segment().setAtIndex(INT, to, varCharLengths[c].segment().getAtIndex(INT, from));
        }
        index.relocate(keyHashes.segment(), movedKey, to);
    }

    private void ensureSlabRoom(int extra) {
        if ((long) slabUsed + extra > slab.capacity()) {
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
        NativeColumn destination = spareSlab != null && spareSlab.capacity() >= capacity
                ? spareSlab
                : new NativeColumn(capacity);
        if (destination != spareSlab && spareSlab != null) {
            spareSlab.close();
        }
        MemorySegment source = slab.segment();
        MemorySegment copyTarget = destination.segment();
        int cursor = 0;
        for (int row = 0; row < rowCount; row++) {
            for (int c = 0; c < varCharLengths.length; c++) {
                int length = varCharLengths[c].segment().getAtIndex(INT, row);
                MemorySegment.copy(source, varCharOffsets[c].segment().getAtIndex(INT, row),
                        copyTarget, cursor, length);
                varCharOffsets[c].segment().setAtIndex(INT, row, cursor);
                cursor += length;
            }
        }
        spareSlab = slab;
        slab = destination;
        slabUsed = cursor;
    }

    private void checkRowCapacity() {
        if (rowCount == MAX_ROWS) {
            throw new IllegalStateException("row capacity limit reached: " + MAX_ROWS);
        }
    }

    private void growRows() {
        long rows = 2L * rowCapacity;
        keyHashes.ensureCapacity(rows * Long.BYTES);
        rowKinds.ensureCapacity(rows);
        for (NativeColumn column : longColumns) {
            column.ensureCapacity(rows * Long.BYTES);
        }
        for (NativeColumn column : intColumns) {
            column.ensureCapacity(rows * Integer.BYTES);
        }
        for (NativeColumn column : varCharOffsets) {
            column.ensureCapacity(rows * Integer.BYTES);
        }
        for (NativeColumn column : varCharLengths) {
            column.ensureCapacity(rows * Integer.BYTES);
        }
        rowCapacity = Math.toIntExact(rows);
    }

    private void checkRow(int row) {
        Objects.checkIndex(row, rowCount);
    }

    private static void closeAll(NativeColumn[] columns) {
        for (NativeColumn column : columns) {
            column.close();
        }
    }

    private static void requireBytes(MemorySegment values, long bytes, String name) {
        if (values.byteSize() != bytes) {
            throw new IllegalArgumentException(name + " spans " + values.byteSize() + " bytes, schema needs " + bytes);
        }
    }

    private static int totalLength(MemorySegment lengths, int columns, long available) {
        long total = 0;
        for (int c = 0; c < columns; c++) {
            int length = lengths.get(INT_UNALIGNED, c * (long) Integer.BYTES);
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
