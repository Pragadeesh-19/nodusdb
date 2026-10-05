package io.nodusdb.kernel;

/*
 * Probe arithmetic shared by the open-addressing tables in the kernel. Tables are
 * power-of-two sized and indexed by mask = size - 1.
 *
 * Deletion uses backward shift (Knuth, TAOCP 6.4, Algorithm R). Walking the cluster
 * behind a hole, an entry may move into the hole only when the hole lies on that
 * entry's probe path from its home slot. canMoveInto encodes that test with wrap-around
 * arithmetic. A test that ignores wrap-around moves entries too early or too late
 * across the end of the table.
 */
public final class OpenAddressing {

    private OpenAddressing() {
    }

    public static int home(long key, int mask) {
        return (int) mix(key) & mask;
    }

    public static boolean canMoveInto(int hole, int cursor, int home, int mask) {
        return ((hole - home) & mask) <= ((cursor - home) & mask);
    }

    private static long mix(long key) {
        long h = key;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }
}
