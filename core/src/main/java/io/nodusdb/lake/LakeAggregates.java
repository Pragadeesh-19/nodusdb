package io.nodusdb.lake;

final class LakeAggregates {

    private LakeAggregates() {
    }

    static Aggregate of(DeltaMemTable table, LakeSchema.Type type, int slot) {
        int rows = table.size();
        byte[] kinds = table.kindColumn();
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

    static double sumDoubles(long[] bits, int rows) {
        double s0 = 0;
        double s1 = 0;
        double s2 = 0;
        double s3 = 0;
        int i = 0;
        for (; i + 3 < rows; i += 4) {
            s0 += Double.longBitsToDouble(bits[i]);
            s1 += Double.longBitsToDouble(bits[i + 1]);
            s2 += Double.longBitsToDouble(bits[i + 2]);
            s3 += Double.longBitsToDouble(bits[i + 3]);
        }
        for (; i < rows; i++) {
            s0 += Double.longBitsToDouble(bits[i]);
        }
        return (s0 + s1) + (s2 + s3);
    }

    static double sumDoublesLive(long[] bits, byte[] kinds, int rows) {
        double sum = 0;
        for (int i = 0; i < rows; i++) {
            if (kinds[i] == DeltaMemTable.INSERT) {
                sum += Double.longBitsToDouble(bits[i]);
            }
        }
        return sum;
    }

    static long sumLongs(long[] values, int rows) {
        long s0 = 0;
        long s1 = 0;
        long s2 = 0;
        long s3 = 0;
        int i = 0;
        for (; i + 3 < rows; i += 4) {
            s0 = Math.addExact(s0, values[i]);
            s1 = Math.addExact(s1, values[i + 1]);
            s2 = Math.addExact(s2, values[i + 2]);
            s3 = Math.addExact(s3, values[i + 3]);
        }
        for (; i < rows; i++) {
            s0 = Math.addExact(s0, values[i]);
        }
        return Math.addExact(Math.addExact(s0, s1), Math.addExact(s2, s3));
    }

    static long sumLongsLive(long[] values, byte[] kinds, int rows) {
        long sum = 0;
        for (int i = 0; i < rows; i++) {
            if (kinds[i] == DeltaMemTable.INSERT) {
                sum = Math.addExact(sum, values[i]);
            }
        }
        return sum;
    }

    static long sumInts(int[] values, int rows) {
        long s0 = 0;
        long s1 = 0;
        long s2 = 0;
        long s3 = 0;
        int i = 0;
        for (; i + 3 < rows; i += 4) {
            s0 += values[i];
            s1 += values[i + 1];
            s2 += values[i + 2];
            s3 += values[i + 3];
        }
        for (; i < rows; i++) {
            s0 += values[i];
        }
        return (s0 + s1) + (s2 + s3);
    }

    static long sumIntsLive(int[] values, byte[] kinds, int rows) {
        long sum = 0;
        for (int i = 0; i < rows; i++) {
            if (kinds[i] == DeltaMemTable.INSERT) {
                sum += values[i];
            }
        }
        return sum;
    }

    private static boolean containsTombstone(byte[] kinds, int rows) {
        for (int i = 0; i < rows; i++) {
            if (kinds[i] == DeltaMemTable.TOMBSTONE) {
                return true;
            }
        }
        return false;
    }

    private static long countLive(byte[] kinds, int rows) {
        long live = 0;
        for (int i = 0; i < rows; i++) {
            if (kinds[i] == DeltaMemTable.INSERT) {
                live++;
            }
        }
        return live;
    }
}
