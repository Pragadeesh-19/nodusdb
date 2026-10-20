package io.nodusdb.lake.buffer;

import io.nodusdb.kernel.memory.NativeMemory;
import io.nodusdb.lake.memory.NativeColumn;
import io.nodusdb.lake.memory.NativeKeyIndex;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

public final class DeltaMemTable implements AutoCloseable {

    public static final int ABSENT = -1;

    public static final byte INSERT = 0;

    public static final byte TOMBSTONE = 1;

    public static final int MAX_ROWS = 1 << 29;

    static final int MAX_SLAB_BYTES = 1 << 30;

    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

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
    private int tombstones;

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

        int row = index.find(keyHashes, keyHash);
        boolean fresh = row == ABSENT;
        if (fresh) {
            row = beginInsert(keyHash, totalBytes);
        } else {
            beginOverwrite(row, totalBytes);
        }
        writeFixedWidth(row, longValues, intValues);
        writeVarChars(row, varCharValues, lengths);
        if (fresh) {
            finishInsert(keyHash, row);
        }
        return fresh;
    }

    public boolean upsertRow(ColumnarCursor batch, int sourceRow) {
        long keyHash = batch.keyAt(sourceRow);
        long varBytes = 0;
        for (int c = 0; c < varCharLengths.length; c++) {
            varBytes += batch.offsetAt(c, sourceRow + 1) - batch.offsetAt(c, sourceRow);
        }
        int totalBytes = Math.toIntExact(varBytes);
        int row = index.find(keyHashes, keyHash);
        boolean fresh = row == ABSENT;
        if (fresh) {
            row = beginInsert(keyHash, totalBytes);
        } else {
            beginOverwrite(row, totalBytes);
        }
        for (int c = 0; c < longColumns.length; c++) {
            longColumns[c].setLongAtIndex(row, batch.longAt(c, sourceRow));
        }
        for (int c = 0; c < intColumns.length; c++) {
            intColumns[c].setIntAtIndex(row, batch.intAt(c, sourceRow));
        }
        for (int c = 0; c < varCharLengths.length; c++) {
            int start = batch.offsetAt(c, sourceRow);
            int length = batch.offsetAt(c, sourceRow + 1) - start;
            NativeColumn.copyBytes(batch.dataColumns[c], start, slab.segment(), slabUsed, length);
            varCharOffsets[c].setIntAtIndex(row, slabUsed);
            varCharLengths[c].setIntAtIndex(row, length);
            slabUsed += length;
            liveVarCharBytes += length;
        }
        if (fresh) {
            finishInsert(keyHash, row);
        }
        return fresh;
    }

    public boolean delete(long keyHash) {
        int row = index.find(keyHashes, keyHash);
        if (row == ABSENT) {
            return false;
        }
        int lastRow = rowCount - 1;
        if (kindAt(row) == TOMBSTONE) {
            tombstones--;
        }
        for (NativeColumn column : varCharLengths) {
            liveVarCharBytes -= column.getIntAtIndex(row);
        }
        index.remove(keyHashes, keyHash);
        if (row != lastRow) {
            moveRow(lastRow, row);
        }
        rowCount = lastRow;
        return true;
    }

    public void tombstone(long keyHash) {
        int row = index.find(keyHashes, keyHash);
        if (row == ABSENT) {
            appendTombstone(keyHash);
            tombstones++;
        } else if (kindAt(row) == INSERT) {
            clearVarChars(row);
            rowKinds.setByte(row, TOMBSTONE);
            tombstones++;
        }
    }

    public void clear() {
        index.clear();
        rowCount = 0;
        slabUsed = 0;
        liveVarCharBytes = 0;
        tombstones = 0;
    }

    public int getRow(long keyHash) {
        return index.find(keyHashes, keyHash);
    }

    public int size() {
        return rowCount;
    }

    public int tombstoneCount() {
        return tombstones;
    }

    public int slabBytesUsed() {
        return slabUsed;
    }

    public long keyHashAt(int row) {
        checkRow(row);
        return keyHashes.getLongAtIndex(row);
    }

    public byte kindAt(int row) {
        checkRow(row);
        return rowKinds.getByte(row);
    }

    public long longAt(int column, int row) {
        checkRow(row);
        return longColumns[column].getLongAtIndex(row);
    }

    public double doubleAt(int column, int row) {
        return Double.longBitsToDouble(longAt(column, row));
    }

    public int intAt(int column, int row) {
        checkRow(row);
        return intColumns[column].getIntAtIndex(row);
    }

    public int varCharLength(int column, int row) {
        checkRow(row);
        return varCharLengths[column].getIntAtIndex(row);
    }

    public int copyVarChar(int column, int row, byte[] destination, int destinationOffset) {
        checkRow(row);
        int length = varCharLengths[column].getIntAtIndex(row);
        MemorySegment.copy(slab.segment(), BYTE, varCharOffsets[column].getIntAtIndex(row),
                destination, destinationOffset, length);
        return length;
    }

    int slabCapacity() {
        return Math.toIntExact(slab.capacity());
    }

    public MemorySegment keyHashColumn() {
        return keyHashes.segment();
    }

    public MemorySegment kindColumn() {
        return rowKinds.segment();
    }

    public MemorySegment longColumn(int column) {
        return longColumns[column].segment();
    }

    public MemorySegment intColumn(int column) {
        return intColumns[column].segment();
    }

    public MemorySegment varCharOffsetColumn(int column) {
        return varCharOffsets[column].segment();
    }

    public MemorySegment varCharLengthColumn(int column) {
        return varCharLengths[column].segment();
    }

    public MemorySegment varCharSlab() {
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
        int tombstoneRows = 0;
        for (int row = 0; row < rowCount; row++) {
            long key = keyHashAt(row);
            if (index.find(keyHashes, key) != row) {
                throw new IllegalStateException(
                        "keyHashes[" + row + "]=" + key + " is indexed at " + index.find(keyHashes, key));
            }
            byte kind = kindAt(row);
            if (kind != INSERT && kind != TOMBSTONE) {
                throw new IllegalStateException("row " + row + " has unknown kind " + kind);
            }
            if (kind == TOMBSTONE) {
                tombstoneRows++;
            }
            for (int c = 0; c < varCharLengths.length; c++) {
                int offset = varCharOffsets[c].getIntAtIndex(row);
                int length = varCharLengths[c].getIntAtIndex(row);
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
        if (tombstoneRows != tombstones) {
            throw new IllegalStateException(
                    "tombstone count " + tombstones + " != " + tombstoneRows + " tombstone rows");
        }
    }

    private int beginInsert(long keyHash, int totalBytes) {
        checkRowCapacity();
        ensureSlabRoom(totalBytes);
        if (rowCount == rowCapacity) {
            growRows();
        }
        int row = rowCount;
        keyHashes.setLongAtIndex(row, keyHash);
        rowKinds.setByte(row, INSERT);
        return row;
    }

    private void finishInsert(long keyHash, int row) {
        index.insert(keyHashes, keyHash, row);
        rowCount++;
    }

    private void appendTombstone(long keyHash) {
        checkRowCapacity();
        if (rowCount == rowCapacity) {
            growRows();
        }
        int row = rowCount;
        keyHashes.setLongAtIndex(row, keyHash);
        rowKinds.setByte(row, TOMBSTONE);
        for (NativeColumn column : varCharOffsets) {
            column.setIntAtIndex(row, 0);
        }
        for (NativeColumn column : varCharLengths) {
            column.setIntAtIndex(row, 0);
        }
        index.insert(keyHashes, keyHash, row);
        rowCount++;
    }

    private void beginOverwrite(int row, int totalBytes) {
        ensureSlabRoom(totalBytes);
        clearVarChars(row);
        if (kindAt(row) == TOMBSTONE) {
            tombstones--;
        }
        rowKinds.setByte(row, INSERT);
    }

    private void clearVarChars(int row) {
        for (int c = 0; c < varCharLengths.length; c++) {
            liveVarCharBytes -= varCharLengths[c].getIntAtIndex(row);
            varCharLengths[c].setIntAtIndex(row, 0);
            varCharOffsets[c].setIntAtIndex(row, 0);
        }
    }

    private void writeFixedWidth(int row, MemorySegment longValues, MemorySegment intValues) {
        long longAddress = NativeMemory.readableAddress(longValues);
        long longSize = longValues.byteSize();
        for (int c = 0; c < longColumns.length; c++) {
            long value = NativeMemory.getLongUnaligned(longValues, longAddress, longSize, (long) c << 3);
            longColumns[c].setLongAtIndex(row, value);
        }
        long intAddress = NativeMemory.readableAddress(intValues);
        long intSize = intValues.byteSize();
        for (int c = 0; c < intColumns.length; c++) {
            int value = NativeMemory.getIntUnaligned(intValues, intAddress, intSize, (long) c << 2);
            intColumns[c].setIntAtIndex(row, value);
        }
    }

    private void writeVarChars(int row, MemorySegment varCharValues, MemorySegment lengths) {
        long source = 0;
        long lengthsAddress = NativeMemory.readableAddress(lengths);
        long lengthsSize = lengths.byteSize();
        for (int c = 0; c < varCharLengths.length; c++) {
            int length = NativeMemory.getIntUnaligned(lengths, lengthsAddress, lengthsSize, c * (long) Integer.BYTES);
            NativeColumn.copyBytes(varCharValues, source, slab.segment(), slabUsed, length);
            varCharOffsets[c].setIntAtIndex(row, slabUsed);
            varCharLengths[c].setIntAtIndex(row, length);
            slabUsed += length;
            liveVarCharBytes += length;
            source += length;
        }
    }

    private void moveRow(int from, int to) {
        long movedKey = keyHashes.getLongAtIndex(from);
        keyHashes.setLongAtIndex(to, movedKey);
        rowKinds.setByte(to, rowKinds.getByte(from));
        for (NativeColumn column : longColumns) {
            column.setLongAtIndex(to, column.getLongAtIndex(from));
        }
        for (NativeColumn column : intColumns) {
            column.setIntAtIndex(to, column.getIntAtIndex(from));
        }
        for (int c = 0; c < varCharLengths.length; c++) {
            varCharOffsets[c].setIntAtIndex(to, varCharOffsets[c].getIntAtIndex(from));
            varCharLengths[c].setIntAtIndex(to, varCharLengths[c].getIntAtIndex(from));
        }
        index.relocate(keyHashes, movedKey, to);
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
                int length = varCharLengths[c].getIntAtIndex(row);
                MemorySegment.copy(source, varCharOffsets[c].getIntAtIndex(row),
                        copyTarget, cursor, length);
                varCharOffsets[c].setIntAtIndex(row, cursor);
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
        long lengthsAddress = NativeMemory.readableAddress(lengths);
        long lengthsSize = lengths.byteSize();
        for (int c = 0; c < columns; c++) {
            int length = NativeMemory.getIntUnaligned(lengths, lengthsAddress, lengthsSize, c * (long) Integer.BYTES);
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
