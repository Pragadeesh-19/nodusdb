package io.nodusdb.capi;

import io.nodusdb.lake.DeltaMemTable;
import io.nodusdb.lake.LakeRow;
import io.nodusdb.lake.LakeSchema;
import io.nodusdb.lake.LakeTable;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.word.WordFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

public final class LakeCApi {

    private static final int ERROR = -1;
    private static final int GET_ABSENT = 0;
    private static final int GET_FOUND = 1;
    private static final int GET_BUFFER_TOO_SMALL = 2;
    private static final HandleTable<LakeTable> TABLES = new HandleTable<>();

    private LakeCApi() {
    }

    @CEntryPoint(name = "nodus_lake_open")
    public static VoidPointer open(IsolateThread thread, CCharPointer schema, int schemaLength,
                                   CCharPointer path, int pathLength, int maxRows, int maxSlabBytes,
                                   long flushIntervalMillis) {
        try {
            LakeSchema parsed = LakeSchema.parse(text(schema, schemaLength));
            LakeTable.Config defaults = LakeTable.Config.DEFAULT;
            LakeTable.Config config = new LakeTable.Config(maxRows, maxSlabBytes, defaults.initialCapacity(),
                    defaults.initialSlabBytes(), flushIntervalMillis);
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
            table.upsert(key, longArray(longs, shape.longColumns()), intArray(ints, shape.intColumns()),
                    bytes(varBytes, varByteCount), intArray(varLengths, shape.varCharColumns()));
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
            long[] keyValues = longArray(keys, rowCount);
            long[] longValues = longArray(longs, Math.multiplyExact(rowCount, shape.longColumns()));
            int[] intValues = intArray(ints, Math.multiplyExact(rowCount, shape.intColumns()));
            int[] lengthValues = intArray(varLengths, Math.multiplyExact(rowCount, shape.varCharColumns()));
            byte[] allBytes = bytes(varBytes, Math.toIntExact(varByteCount));
            requireLengthsFit(lengthValues, allBytes.length);

            long[] rowLongs = new long[shape.longColumns()];
            int[] rowInts = new int[shape.intColumns()];
            int[] rowLengths = new int[shape.varCharColumns()];
            int cursor = 0;
            for (int row = 0; row < rowCount; row++) {
                System.arraycopy(longValues, row * shape.longColumns(), rowLongs, 0, rowLongs.length);
                System.arraycopy(intValues, row * shape.intColumns(), rowInts, 0, rowInts.length);
                System.arraycopy(lengthValues, row * shape.varCharColumns(), rowLengths, 0, rowLengths.length);
                int rowBytes = 0;
                for (int length : rowLengths) {
                    rowBytes += length;
                }
                byte[] rowVarBytes = Arrays.copyOfRange(allBytes, cursor, cursor + rowBytes);
                cursor += rowBytes;
                table.upsert(keyValues[row], rowLongs, rowInts, rowVarBytes, rowLengths);
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

    private static void requireLengthsFit(int[] lengths, int available) {
        long total = 0;
        for (int length : lengths) {
            if (length < 0) {
                throw new IllegalArgumentException("negative var-char length: " + length);
            }
            total += length;
        }
        if (total > available) {
            throw new IllegalArgumentException("var-char lengths exceed supplied bytes");
        }
    }

    private static long[] longArray(CLongPointer pointer, long count) {
        int size = Math.toIntExact(count);
        long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = pointer.read(i);
        }
        return values;
    }

    private static int[] intArray(CIntPointer pointer, long count) {
        int size = Math.toIntExact(count);
        int[] values = new int[size];
        for (int i = 0; i < size; i++) {
            values[i] = pointer.read(i);
        }
        return values;
    }

    private static byte[] bytes(CCharPointer pointer, long count) {
        int size = Math.toIntExact(count);
        byte[] values = new byte[size];
        for (int i = 0; i < size; i++) {
            values[i] = pointer.read(i);
        }
        return values;
    }

    private static String text(CCharPointer pointer, int count) {
        return new String(bytes(pointer, count), StandardCharsets.UTF_8);
    }
}
