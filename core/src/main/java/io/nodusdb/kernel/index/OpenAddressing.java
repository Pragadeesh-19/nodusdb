package io.nodusdb.kernel.index;

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
