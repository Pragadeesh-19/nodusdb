package io.nodusdb.lake;

import io.nodusdb.kernel.OpenAddressing;
import org.junit.jupiter.api.Test;

import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NativeKeyIndexTest {

    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final int ROW_LIMIT = 256;

    @Test
    void emptyIndexFindsNothing() {
        try (NativeKeyIndex index = new NativeKeyIndex(16); NativeColumn keys = new NativeColumn(64)) {
            assertEquals(NativeKeyIndex.ABSENT, index.find(keys.segment(), 42L));
            assertEquals(0, index.size());
        }
    }

    @Test
    void insertedKeysAreFoundAtTheirRows() {
        try (NativeKeyIndex index = new NativeKeyIndex(16); NativeColumn keys = new NativeColumn(8L * ROW_LIMIT)) {
            for (int row = 0; row < 5; row++) {
                keys.segment().setAtIndex(LONG, row, 100L + row);
                index.insert(keys.segment(), 100L + row, row);
            }

            for (int row = 0; row < 5; row++) {
                assertEquals(row, index.find(keys.segment(), 100L + row));
            }
            assertEquals(5, index.size());
        }
    }

    @Test
    void removingAKeyBeforeMovingTheLastRowKeepsTheMovedKeyReachable() {
        try (NativeKeyIndex index = new NativeKeyIndex(16); NativeColumn keys = new NativeColumn(8L * ROW_LIMIT)) {
            for (int row = 0; row < 4; row++) {
                keys.segment().setAtIndex(LONG, row, 7L * (row + 1));
                index.insert(keys.segment(), 7L * (row + 1), row);
            }
            index.remove(keys.segment(), 14L);
            keys.segment().setAtIndex(LONG, 1, 28L);
            index.relocate(keys.segment(), 28L, 1);

            assertEquals(NativeKeyIndex.ABSENT, index.find(keys.segment(), 14L));
            assertEquals(1, index.find(keys.segment(), 28L));
            assertEquals(0, index.find(keys.segment(), 7L));
            assertEquals(2, index.find(keys.segment(), 21L));
        }
    }

    @Test
    void removingAbsentKeyIsRejected() {
        try (NativeKeyIndex index = new NativeKeyIndex(16); NativeColumn keys = new NativeColumn(64)) {
            assertThrows(IllegalStateException.class, () -> index.remove(keys.segment(), 9L));
            assertThrows(IllegalStateException.class, () -> index.relocate(keys.segment(), 9L, 0));
        }
    }

    @Test
    void clearEmptiesTheIndex() {
        try (NativeKeyIndex index = new NativeKeyIndex(16); NativeColumn keys = new NativeColumn(64)) {
            keys.segment().setAtIndex(LONG, 0, 5L);
            index.insert(keys.segment(), 5L, 0);
            index.clear();

            assertEquals(0, index.size());
            assertEquals(NativeKeyIndex.ABSENT, index.find(keys.segment(), 5L));
        }
    }

    @Test
    void growthRehashesEveryLiveKey() {
        int count = 3_000;
        try (NativeKeyIndex index = new NativeKeyIndex(16); NativeColumn keys = new NativeColumn(8L * count)) {
            for (int row = 0; row < count; row++) {
                keys.segment().setAtIndex(LONG, row, 1_000_003L * row + 17);
                index.insert(keys.segment(), 1_000_003L * row + 17, row);
            }

            assertEquals(count, index.size());
            assertEquals(0, index.slotCount() % 2);
            for (int row = 0; row < count; row++) {
                assertEquals(row, index.find(keys.segment(), 1_000_003L * row + 17));
            }
        }
    }

    @Test
    void wrapAroundClustersSurviveChurnAtFixedCapacity() {
        churn(1L, 15, 8, 16, 30_000);
    }

    @Test
    void mixedKeysChurnThroughGrowthMatchesAMapOracle() {
        churn(2L, -1, ROW_LIMIT, 0, 30_000);
    }

    private static void churn(long seed, int collidingHome, int rowLimit, int fixedSlots, int ops) {
        Random random = new Random(seed);
        List<Long> pool = collidingHome < 0 ? randomKeys(random, 60) : collidingKeys(collidingHome, 15, 24);
        Map<Long, Integer> model = new HashMap<>();
        try (NativeKeyIndex index = new NativeKeyIndex(16); NativeColumn keys = new NativeColumn(8L * ROW_LIMIT)) {
            int rows = 0;
            for (int op = 0; op < ops; op++) {
                long key = pool.get(random.nextInt(pool.size()));
                if (random.nextBoolean()) {
                    if (!model.containsKey(key) && rows < rowLimit) {
                        keys.segment().setAtIndex(LONG, rows, key);
                        index.insert(keys.segment(), key, rows);
                        model.put(key, rows);
                        rows++;
                    }
                } else if (model.containsKey(key)) {
                    int row = model.remove(key);
                    index.remove(keys.segment(), key);
                    int last = rows - 1;
                    if (row != last) {
                        long moved = keys.segment().getAtIndex(LONG, last);
                        keys.segment().setAtIndex(LONG, row, moved);
                        index.relocate(keys.segment(), moved, row);
                        model.put(moved, row);
                    }
                    rows--;
                }
                assertEquals(model.size(), index.size(), "size at op " + op + " seed " + seed);
                if (fixedSlots > 0) {
                    assertEquals(fixedSlots, index.slotCount(), "slot count at op " + op);
                }
                for (long candidate : pool) {
                    int expected = model.getOrDefault(candidate, NativeKeyIndex.ABSENT);
                    assertEquals(expected, index.find(keys.segment(), candidate),
                            "key " + candidate + " at op " + op + " seed " + seed);
                }
            }
        }
    }

    private static List<Long> randomKeys(Random random, int count) {
        Set<Long> keys = new LinkedHashSet<>();
        while (keys.size() < count) {
            keys.add(random.nextLong());
        }
        return new ArrayList<>(keys);
    }

    private static List<Long> collidingKeys(int home, int mask, int count) {
        List<Long> keys = new ArrayList<>();
        for (long key = 0; keys.size() < count; key++) {
            if (OpenAddressing.home(key, mask) == home) {
                keys.add(key);
            }
        }
        return keys;
    }
}
