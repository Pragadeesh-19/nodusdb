package io.nodusdb.lake;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

final class LakeAggregates {

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

    private LakeAggregates() {
    }

    static Aggregate of(DeltaMemTable table, LakeSchema.Type type, int slot) {
        int rows = table.size();
        MemorySegment kinds = table.kindColumn();
        boolean hasTombstones = containsTombstone(kinds, rows);
        long live = hasTombstones ? countLive(kinds, rows) : rows;
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
        double s0 = 0;
        double s1 = 0;
        double s2 = 0;
        double s3 = 0;
        int i = 0;
        for (; i + 3 < rows; i += 4) {
            s0 += Double.longBitsToDouble(bits.getAtIndex(LONG, i));
            s1 += Double.longBitsToDouble(bits.getAtIndex(LONG, i + 1));
            s2 += Double.longBitsToDouble(bits.getAtIndex(LONG, i + 2));
            s3 += Double.longBitsToDouble(bits.getAtIndex(LONG, i + 3));
        }
        for (; i < rows; i++) {
            s0 += Double.longBitsToDouble(bits.getAtIndex(LONG, i));
        }
        return (s0 + s1) + (s2 + s3);
    }

    static double sumDoublesLive(MemorySegment bits, MemorySegment kinds, int rows) {
        double sum = 0;
        for (int i = 0; i < rows; i++) {
            if (kinds.getAtIndex(BYTE, i) == DeltaMemTable.INSERT) {
                sum += Double.longBitsToDouble(bits.getAtIndex(LONG, i));
            }
        }
        return sum;
    }

    static long sumLongs(MemorySegment values, int rows) {
        long s0 = 0;
        long s1 = 0;
        long s2 = 0;
        long s3 = 0;
        int i = 0;
        for (; i + 3 < rows; i += 4) {
            s0 = Math.addExact(s0, values.getAtIndex(LONG, i));
            s1 = Math.addExact(s1, values.getAtIndex(LONG, i + 1));
            s2 = Math.addExact(s2, values.getAtIndex(LONG, i + 2));
            s3 = Math.addExact(s3, values.getAtIndex(LONG, i + 3));
        }
        for (; i < rows; i++) {
            s0 = Math.addExact(s0, values.getAtIndex(LONG, i));
        }
        return Math.addExact(Math.addExact(s0, s1), Math.addExact(s2, s3));
    }

    static long sumLongsLive(MemorySegment values, MemorySegment kinds, int rows) {
        long sum = 0;
        for (int i = 0; i < rows; i++) {
            if (kinds.getAtIndex(BYTE, i) == DeltaMemTable.INSERT) {
                sum = Math.addExact(sum, values.getAtIndex(LONG, i));
            }
        }
        return sum;
    }

    static long sumInts(MemorySegment values, int rows) {
        long s0 = 0;
        long s1 = 0;
        long s2 = 0;
        long s3 = 0;
        int i = 0;
        for (; i + 3 < rows; i += 4) {
            s0 += values.getAtIndex(INT, i);
            s1 += values.getAtIndex(INT, i + 1);
            s2 += values.getAtIndex(INT, i + 2);
            s3 += values.getAtIndex(INT, i + 3);
        }
        for (; i < rows; i++) {
            s0 += values.getAtIndex(INT, i);
        }
        return (s0 + s1) + (s2 + s3);
    }

    static long sumIntsLive(MemorySegment values, MemorySegment kinds, int rows) {
        long sum = 0;
        for (int i = 0; i < rows; i++) {
            if (kinds.getAtIndex(BYTE, i) == DeltaMemTable.INSERT) {
                sum += values.getAtIndex(INT, i);
            }
        }
        return sum;
    }

    private static boolean containsTombstone(MemorySegment kinds, int rows) {
        for (int i = 0; i < rows; i++) {
            if (kinds.getAtIndex(BYTE, i) == DeltaMemTable.TOMBSTONE) {
                return true;
            }
        }
        return false;
    }

    private static long countLive(MemorySegment kinds, int rows) {
        long live = 0;
        for (int i = 0; i < rows; i++) {
            if (kinds.getAtIndex(BYTE, i) == DeltaMemTable.INSERT) {
                live++;
            }
        }
        return live;
    }
}
