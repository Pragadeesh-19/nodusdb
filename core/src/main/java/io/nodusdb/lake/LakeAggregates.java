package io.nodusdb.lake;

import io.nodusdb.lake.buffer.DeltaMemTable;
import io.nodusdb.lake.model.Aggregate;
import io.nodusdb.lake.model.LakeSchema;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class LakeAggregates {

    static final int CHUNK = 4096;

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final ThreadLocal<Scratch> SCRATCH = ThreadLocal.withInitial(Scratch::new);

    private LakeAggregates() {
    }

    private static final class Scratch {
        final long[] longs = new long[CHUNK];
        final int[] ints = new int[CHUNK];
        final byte[] kinds = new byte[CHUNK];
    }

    static Aggregate of(DeltaMemTable table, LakeSchema.Type type, int slot) {
        int rows = table.size();
        MemorySegment kinds = table.kindColumn();
        boolean hasTombstones = table.tombstoneCount() > 0;
        long live = rows - (long) table.tombstoneCount();
        return switch (type) {
            case DOUBLE -> new Aggregate(live, hasTombstones
                    ? sumDoublesLive(table.longColumn(slot), kinds, rows)
                    : sumDoubles(table.longColumn(slot), rows));
            case INT64 -> new Aggregate(live, (double) (hasTombstones
                    ? sumLongsLive(table.longColumn(slot), kinds, rows)
                    : sumLongs(table.longColumn(slot), rows)));
            case INT32 -> new Aggregate(live, (double) (hasTombstones
                    ? sumIntsLive(table.intColumn(slot), kinds, rows)
                    : sumInts(table.intColumn(slot), rows)));
            case UTF8 -> throw new IllegalArgumentException("sum and average need a numeric column");
        };
    }

    static double sumDoubles(MemorySegment bits, int rows) {
        long[] chunk = SCRATCH.get().longs;
        double s0 = 0;
        double s1 = 0;
        double s2 = 0;
        double s3 = 0;
        for (int base = 0; base < rows; base += CHUNK) {
            int count = Math.min(CHUNK, rows - base);
            MemorySegment.copy(bits, LONG, (long) base * Long.BYTES, chunk, 0, count);
            int i = 0;
            for (; i + 3 < count; i += 4) {
                s0 += Double.longBitsToDouble(chunk[i]);
                s1 += Double.longBitsToDouble(chunk[i + 1]);
                s2 += Double.longBitsToDouble(chunk[i + 2]);
                s3 += Double.longBitsToDouble(chunk[i + 3]);
            }
            for (; i < count; i++) {
                s0 += Double.longBitsToDouble(chunk[i]);
            }
        }
        return (s0 + s1) + (s2 + s3);
    }

    static double sumDoublesLive(MemorySegment bits, MemorySegment kinds, int rows) {
        Scratch scratch = SCRATCH.get();
        double sum = 0;
        for (int base = 0; base < rows; base += CHUNK) {
            int count = Math.min(CHUNK, rows - base);
            MemorySegment.copy(bits, LONG, (long) base * Long.BYTES, scratch.longs, 0, count);
            MemorySegment.copy(kinds, BYTE, base, scratch.kinds, 0, count);
            for (int i = 0; i < count; i++) {
                if (scratch.kinds[i] == DeltaMemTable.INSERT) {
                    sum += Double.longBitsToDouble(scratch.longs[i]);
                }
            }
        }
        return sum;
    }

    static long sumLongs(MemorySegment values, int rows) {
        long[] chunk = SCRATCH.get().longs;
        long s0 = 0;
        long s1 = 0;
        long s2 = 0;
        long s3 = 0;
        for (int base = 0; base < rows; base += CHUNK) {
            int count = Math.min(CHUNK, rows - base);
            MemorySegment.copy(values, LONG, (long) base * Long.BYTES, chunk, 0, count);
            int i = 0;
            for (; i + 3 < count; i += 4) {
                s0 = Math.addExact(s0, chunk[i]);
                s1 = Math.addExact(s1, chunk[i + 1]);
                s2 = Math.addExact(s2, chunk[i + 2]);
                s3 = Math.addExact(s3, chunk[i + 3]);
            }
            for (; i < count; i++) {
                s0 = Math.addExact(s0, chunk[i]);
            }
        }
        return Math.addExact(Math.addExact(s0, s1), Math.addExact(s2, s3));
    }

    static long sumLongsLive(MemorySegment values, MemorySegment kinds, int rows) {
        Scratch scratch = SCRATCH.get();
        long sum = 0;
        for (int base = 0; base < rows; base += CHUNK) {
            int count = Math.min(CHUNK, rows - base);
            MemorySegment.copy(values, LONG, (long) base * Long.BYTES, scratch.longs, 0, count);
            MemorySegment.copy(kinds, BYTE, base, scratch.kinds, 0, count);
            for (int i = 0; i < count; i++) {
                if (scratch.kinds[i] == DeltaMemTable.INSERT) {
                    sum = Math.addExact(sum, scratch.longs[i]);
                }
            }
        }
        return sum;
    }

    static long sumInts(MemorySegment values, int rows) {
        int[] chunk = SCRATCH.get().ints;
        long s0 = 0;
        long s1 = 0;
        long s2 = 0;
        long s3 = 0;
        for (int base = 0; base < rows; base += CHUNK) {
            int count = Math.min(CHUNK, rows - base);
            MemorySegment.copy(values, INT, (long) base * Integer.BYTES, chunk, 0, count);
            int i = 0;
            for (; i + 3 < count; i += 4) {
                s0 += chunk[i];
                s1 += chunk[i + 1];
                s2 += chunk[i + 2];
                s3 += chunk[i + 3];
            }
            for (; i < count; i++) {
                s0 += chunk[i];
            }
        }
        return (s0 + s1) + (s2 + s3);
    }

    static long sumIntsLive(MemorySegment values, MemorySegment kinds, int rows) {
        Scratch scratch = SCRATCH.get();
        long sum = 0;
        for (int base = 0; base < rows; base += CHUNK) {
            int count = Math.min(CHUNK, rows - base);
            MemorySegment.copy(values, INT, (long) base * Integer.BYTES, scratch.ints, 0, count);
            MemorySegment.copy(kinds, BYTE, base, scratch.kinds, 0, count);
            for (int i = 0; i < count; i++) {
                if (scratch.kinds[i] == DeltaMemTable.INSERT) {
                    sum += scratch.ints[i];
                }
            }
        }
        return sum;
    }
}
