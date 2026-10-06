package io.nodusdb.capi;

import io.nodusdb.lake.Aggregate;
import io.nodusdb.lake.ColumnarRows;
import io.nodusdb.lake.DeltaMemTable;
import io.nodusdb.lake.LakeRow;
import io.nodusdb.lake.LakeSchema;
import io.nodusdb.lake.LakeTable;
import io.nodusdb.lake.ParquetCodec;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CDoublePointer;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.word.WordFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class LakeCApi {

    private static final int ERROR = -1;
    private static final int GET_ABSENT = 0;
    private static final int GET_FOUND = 1;
    private static final int GET_BUFFER_TOO_SMALL = 2;
    private static final int AGGREGATE_EMPTY = 0;
    private static final int AGGREGATE_VALUE = 1;
    private static final int CODEC_UNCOMPRESSED = 0;
    private static final int CODEC_SNAPPY = 1;
    private static final long LONG_BYTES = Long.BYTES;
    private static final long INT_BYTES = Integer.BYTES;
    private static final ValueLayout.OfLong LONG_UNALIGNED = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final ValueLayout.OfInt INT_UNALIGNED = ValueLayout.JAVA_INT_UNALIGNED;
    private static final HandleTable<LakeTable> TABLES = new HandleTable<>();

    private LakeCApi() {
    }

    @CEntryPoint(name = "nodus_lake_open")
    public static VoidPointer open(IsolateThread thread, CCharPointer schema, int schemaLength,
                                   CCharPointer path, int pathLength, int maxRows, int maxSlabBytes,
                                   long flushIntervalMillis, int codec) {
        try {
            LakeSchema parsed = LakeSchema.parse(text(schema, schemaLength));
            LakeTable.Config defaults = LakeTable.Config.DEFAULT;
            LakeTable.Config config = new LakeTable.Config(maxRows, maxSlabBytes, defaults.initialCapacity(),
                    defaults.initialSlabBytes(), flushIntervalMillis, codecOf(codec));
            LakeTable table = LakeTable.open(Path.of(text(path, pathLength)), parsed, config);
            return WordFactory.pointer(TABLES.open(table));
        } catch (IOException | RuntimeException e) {
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "nodus_lake_close")
    public static boolean close(IsolateThread thread, VoidPointer handle) {
        try {
            long key = handle.rawValue();
            TABLES.get(key).close();
            TABLES.close(key);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @CEntryPoint(name = "nodus_lake_upsert")
    public static boolean upsert(IsolateThread thread, VoidPointer handle, long key, CLongPointer longs,
                                 CIntPointer ints, CCharPointer varBytes, int varByteCount,
                                 CIntPointer varLengths) {
        try {
            LakeTable table = TABLES.get(handle.rawValue());
            DeltaMemTable.Schema shape = table.schema().memtableSchema();
            table.upsert(key,
                    segment(longs.rawValue(), shape.longColumns() * LONG_BYTES),
                    segment(ints.rawValue(), shape.intColumns() * INT_BYTES),
                    segment(varBytes.rawValue(), varByteCount),
                    segment(varLengths.rawValue(), shape.varCharColumns() * INT_BYTES));
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @CEntryPoint(name = "nodus_lake_upsert_batch")
    public static long upsertBatch(IsolateThread thread, VoidPointer handle, CLongPointer keys, long rows,
                                   CLongPointer longs, CIntPointer ints, CCharPointer varBytes,
                                   long varByteCount, CIntPointer varLengths) {
        try {
            LakeTable table = TABLES.get(handle.rawValue());
            DeltaMemTable.Schema shape = table.schema().memtableSchema();
            int rowCount = Math.toIntExact(rows);
            MemorySegment keyValues = segment(keys.rawValue(), rows * LONG_BYTES);
            MemorySegment longValues = segment(longs.rawValue(), rows * shape.longColumns() * LONG_BYTES);
            MemorySegment intValues = segment(ints.rawValue(), rows * shape.intColumns() * INT_BYTES);
            MemorySegment lengthValues = segment(varLengths.rawValue(), rows * shape.varCharColumns() * INT_BYTES);
            MemorySegment allBytes = segment(varBytes.rawValue(), varByteCount);
            requireLengthsFit(lengthValues, rowCount, shape.varCharColumns(), allBytes.byteSize());

            long longWidth = shape.longColumns() * LONG_BYTES;
            long intWidth = shape.intColumns() * INT_BYTES;
            long lengthWidth = shape.varCharColumns() * INT_BYTES;
            long cursor = 0;
            for (int row = 0; row < rowCount; row++) {
                long lengthOffset = row * lengthWidth;
                int rowBytes = rowLength(lengthValues, lengthOffset, shape.varCharColumns());
                table.upsert(keyValues.getAtIndex(LONG_UNALIGNED, row),
                        longValues.asSlice(row * longWidth, longWidth),
                        intValues.asSlice(row * intWidth, intWidth),
                        allBytes.asSlice(cursor, rowBytes),
                        lengthValues.asSlice(lengthOffset, lengthWidth));
                cursor += rowBytes;
            }
            return rowCount;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_lake_delete")
    public static boolean delete(IsolateThread thread, VoidPointer handle, long key) {
        try {
            TABLES.get(handle.rawValue()).delete(key);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @CEntryPoint(name = "nodus_lake_flush")
    public static boolean flush(IsolateThread thread, VoidPointer handle) {
        try {
            TABLES.get(handle.rawValue()).flush();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @CEntryPoint(name = "nodus_lake_get")
    public static int get(IsolateThread thread, VoidPointer handle, long key, CLongPointer outLongs,
                          CIntPointer outInts, CCharPointer outVarBytes, long outVarCapacity,
                          CIntPointer outVarLengths) {
        try {
            Optional<LakeRow> found = TABLES.get(handle.rawValue()).get(key);
            if (found.isEmpty()) {
                return GET_ABSENT;
            }
            LakeRow row = found.get();
            long needed = 0;
            for (byte[] value : row.varCharValues()) {
                needed += value.length;
            }
            if (needed > outVarCapacity) {
                return GET_BUFFER_TOO_SMALL;
            }
            for (int i = 0; i < row.longValues().length; i++) {
                outLongs.write(i, row.longValues()[i]);
            }
            for (int i = 0; i < row.intValues().length; i++) {
                outInts.write(i, row.intValues()[i]);
            }
            int cursor = 0;
            for (int column = 0; column < row.varCharValues().length; column++) {
                byte[] value = row.varCharValues()[column];
                outVarLengths.write(column, value.length);
                for (byte b : value) {
                    outVarBytes.write(cursor++, b);
                }
            }
            return GET_FOUND;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_lake_upsert_columns")
    public static long upsertColumns(IsolateThread thread, VoidPointer handle, long rows, CLongPointer keys,
                                     int longCount, CLongPointer longAddresses,
                                     int intCount, CLongPointer intAddresses,
                                     int varCharCount, CLongPointer offsetAddresses, CLongPointer dataAddresses) {
        try {
            LakeTable table = TABLES.get(handle.rawValue());
            int rowCount = Math.toIntExact(rows);
            List<MemorySegment> longColumns = new ArrayList<>(longCount);
            for (int c = 0; c < longCount; c++) {
                longColumns.add(segment(longAddresses.read(c), rows * LONG_BYTES));
            }
            List<MemorySegment> intColumns = new ArrayList<>(intCount);
            for (int c = 0; c < intCount; c++) {
                intColumns.add(segment(intAddresses.read(c), rows * INT_BYTES));
            }
            List<MemorySegment> offsets = new ArrayList<>(varCharCount);
            List<MemorySegment> data = new ArrayList<>(varCharCount);
            for (int c = 0; c < varCharCount; c++) {
                MemorySegment columnOffsets = segment(offsetAddresses.read(c), (rows + 1) * INT_BYTES);
                offsets.add(columnOffsets);
                int end = columnOffsets.get(INT_UNALIGNED, rows * INT_BYTES);
                data.add(segment(dataAddresses.read(c), end));
            }
            return table.upsertColumns(new ColumnarRows(rowCount, segment(keys.rawValue(), rows * LONG_BYTES),
                    longColumns, intColumns, offsets, data));
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_lake_sum")
    public static int sum(IsolateThread thread, VoidPointer handle, int field, CDoublePointer out) {
        try {
            out.write(TABLES.get(handle.rawValue()).aggregate(field).sum());
            return AGGREGATE_VALUE;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    @CEntryPoint(name = "nodus_lake_avg")
    public static int average(IsolateThread thread, VoidPointer handle, int field, CDoublePointer out) {
        try {
            Aggregate aggregate = TABLES.get(handle.rawValue()).aggregate(field);
            if (aggregate.count() == 0) {
                return AGGREGATE_EMPTY;
            }
            out.write(aggregate.sum() / aggregate.count());
            return AGGREGATE_VALUE;
        } catch (RuntimeException e) {
            return ERROR;
        }
    }

    private static ParquetCodec codecOf(int id) {
        return switch (id) {
            case CODEC_UNCOMPRESSED -> ParquetCodec.UNCOMPRESSED;
            case CODEC_SNAPPY -> ParquetCodec.SNAPPY;
            default -> throw new IllegalArgumentException("unknown parquet codec " + id);
        };
    }

    private static void requireLengthsFit(MemorySegment lengths, int rows, int columns, long available) {
        long total = 0;
        for (long i = 0; i < (long) rows * columns; i++) {
            int length = lengths.getAtIndex(INT_UNALIGNED, i);
            if (length < 0) {
                throw new IllegalArgumentException("negative var-char length: " + length);
            }
            total += length;
        }
        if (total > available) {
            throw new IllegalArgumentException("var-char lengths exceed supplied bytes");
        }
    }

    private static int rowLength(MemorySegment lengths, long offset, int columns) {
        int total = 0;
        for (int c = 0; c < columns; c++) {
            total += lengths.get(INT_UNALIGNED, offset + c * INT_BYTES);
        }
        return total;
    }

    @SuppressWarnings("restricted")
    private static MemorySegment segment(long address, long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("negative buffer size: " + bytes);
        }
        if (address == 0 && bytes > 0) {
            throw new IllegalArgumentException("null pointer for " + bytes + " bytes");
        }
        return MemorySegment.ofAddress(address).reinterpret(bytes);
    }

    private static String text(CCharPointer pointer, int count) {
        return new String(segment(pointer.rawValue(), count).toArray(ValueLayout.JAVA_BYTE),
                StandardCharsets.UTF_8);
    }
}
