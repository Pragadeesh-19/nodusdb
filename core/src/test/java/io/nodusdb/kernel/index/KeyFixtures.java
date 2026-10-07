package io.nodusdb.kernel.index;

public final class KeyFixtures {

    private KeyFixtures() {
    }

    public static long nthKeyWithHome(int home, int mask, int n) {
        int remaining = n;
        for (long key = 0; ; key++) {
            if (OpenAddressing.home(key, mask) == home) {
                if (remaining == 0) {
                    return key;
                }
                remaining--;
            }
        }
    }
}
