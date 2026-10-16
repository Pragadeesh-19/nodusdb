package io.nodusdb.kernel;

import java.util.Arrays;

public final class ErasedSymbols {

    static final ErasedSymbols NONE = new ErasedSymbols(new int[0], new long[0]);

    private final int[] symbolIds;
    private final long[] eraseLsns;

    ErasedSymbols(int[] symbolIds, long[] eraseLsns) {
        this.symbolIds = symbolIds;
        this.eraseLsns = eraseLsns;
    }

    public int count() {
        return symbolIds.length;
    }

    public int symbolId(int index) {
        return symbolIds[index];
    }

    public long eraseLsn(int index) {
        return eraseLsns[index];
    }

    static final class Log {

        private static final int INITIAL = 8;

        private int[] ids = new int[INITIAL];
        private long[] lsns = new long[INITIAL];
        private int size;
        private int[] unplaced = new int[INITIAL];
        private int unplacedCount;
        private volatile long newest;

        synchronized void applied(int symbolId) {
            if (unplacedCount == unplaced.length) {
                unplaced = Arrays.copyOf(unplaced, unplaced.length * 2);
            }
            unplaced[unplacedCount++] = symbolId;
        }

        boolean hasUnplaced() {
            return unplacedCount > 0;
        }

        synchronized void placed(long lsn) {
            for (int i = 0; i < unplacedCount; i++) {
                add(unplaced[i], lsn);
            }
            unplacedCount = 0;
        }

        synchronized void restored(int symbolId, long lsn) {
            add(symbolId, lsn);
        }

        long newest() {
            return newest;
        }

        synchronized ErasedSymbols snapshot() {
            return size == 0 ? NONE : new ErasedSymbols(Arrays.copyOf(ids, size), Arrays.copyOf(lsns, size));
        }

        private void add(int symbolId, long lsn) {
            if (size == ids.length) {
                ids = Arrays.copyOf(ids, size * 2);
                lsns = Arrays.copyOf(lsns, size * 2);
            }
            ids[size] = symbolId;
            lsns[size] = lsn;
            size++;
            newest = Math.max(newest, lsn);
        }
    }
}
