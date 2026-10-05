package io.nodusdb.kernel;

/*
 * Set operations over one pool block. For capacity C, a power of two, the payload is
 * 2C words:
 *
 *   words [0, C)     dense keys, positions 0 .. degree-1 are live
 *   words [C, 2C)    position table, 2C ints packed two per word, each holding position + 1
 *                    so that zeroed memory is an empty table
 *
 * A table slot holds a position into dense. A lookup reads the position and compares
 * dense[position] with the key, so every key is stored once. Deletion shifts later
 * probe-chain entries back, so the table never holds tombstones. The set itself holds
 * no state: the degree lives with the caller, and the handle names the block.
 *
 * Capacity is padded up to MIN_CAPACITY, so the 2C-word payload fills whole cache
 * lines and both dense and table start on a line when C is a multiple of 8. Callers
 * read the capacity back with capacityOf rather than assuming the requested value.
 */
final class NativeSparseSet {

    static final int ABSENT = -1;
    static final int MIN_CAPACITY = NativeBlockPool.LINE_WORDS / 2;

    private static final int EMPTY = -1;

    private final NativeBlockPool pool;

    NativeSparseSet(NativeBlockPool pool) {
        this.pool = pool;
    }

    int allocate(int capacity) {
        checkCapacity(capacity);
        int padded = Math.max(capacity, MIN_CAPACITY);
        return pool.allocate(Integer.numberOfTrailingZeros(padded) + 1);
    }

    int capacityOf(int handle) {
        int log = pool.logWordsOf(handle);
        return log < 0 ? 0 : 1 << (log - 1);
    }

    int grow(int handle, int degree) {
        int capacity = capacityOf(handle);
        int grown = allocate(2 * capacity);
        for (int i = 0; i < degree; i++) {
            long key = pool.get(handle, i);
            pool.set(grown, i, key);
            insertAbsent(grown, 2 * capacity, key, i);
        }
        pool.release(handle);
        return grown;
    }

    void release(int handle) {
        pool.release(handle);
    }

    int indexOf(int handle, int degree, long key) {
        int capacity = capacityOf(handle);
        int slot = slotOf(handle, capacity, degree, key);
        return slot == ABSENT ? ABSENT : tableAt(handle, capacity, slot);
    }

    boolean contains(int handle, int degree, long key) {
        return indexOf(handle, degree, key) != ABSENT;
    }

    long peek(int handle, int degree, int i) {
        return i >= 0 && i < degree && i < capacityOf(handle) ? pool.get(handle, i) : NodeIds.NONE;
    }

    void copyKeys(int handle, int degree, long[] destination) {
        for (int i = 0; i < degree; i++) {
            destination[i] = pool.get(handle, i);
        }
    }

    void append(int handle, int size, long key) {
        int capacity = capacityOf(handle);
        if (size < 0 || size >= capacity) {
            throw new IllegalStateException("no room for key at size " + size + " in capacity " + capacity);
        }
        pool.set(handle, size, key);
        insertAbsent(handle, capacity, key, size);
    }

    void fill(int handle, long[] keys, int count) {
        int capacity = capacityOf(handle);
        if (count > capacity) {
            throw new IllegalStateException("fill of " + count + " keys exceeds capacity " + capacity);
        }
        for (int i = 0; i < count; i++) {
            pool.set(handle, i, keys[i]);
        }
        for (int i = 0; i < count; i++) {
            insertAbsent(handle, capacity, keys[i], i);
        }
    }

    void remove(int handle, int degree, long key) {
        int capacity = capacityOf(handle);
        int slot = slotOf(handle, capacity, degree, key);
        if (slot == ABSENT) {
            throw new IllegalStateException("key is not in the set: " + key);
        }
        int position = tableAt(handle, capacity, slot);
        removeSlot(handle, capacity, slot);
        int last = degree - 1;
        if (position < last) {
            long moved = pool.get(handle, last);
            int movedSlot = slotOf(handle, capacity, degree, moved);
            setTable(handle, capacity, movedSlot, position);
            pool.set(handle, position, moved);
        }
    }

    void verify(int handle, int degree) {
        for (int i = 0; i < degree; i++) {
            if (indexOf(handle, degree, pool.get(handle, i)) != i) {
                throw new IllegalStateException("dense[" + i + "] is not indexed at " + i);
            }
        }
    }

    private int slotOf(int handle, int capacity, int degree, long key) {
        if (capacity == 0 || degree > capacity) {
            return ABSENT;
        }
        int mask = 2 * capacity - 1;
        int slot = OpenAddressing.home(key, mask);
        for (int probes = 0; probes <= mask; probes++) {
            int position = tableAt(handle, capacity, slot);
            if (position == EMPTY) {
                return ABSENT;
            }
            if (position < degree && pool.get(handle, position) == key) {
                return slot;
            }
            slot = (slot + 1) & mask;
        }
        return ABSENT;
    }

    private void insertAbsent(int handle, int capacity, long key, int position) {
        int mask = 2 * capacity - 1;
        int slot = OpenAddressing.home(key, mask);
        while (tableAt(handle, capacity, slot) != EMPTY) {
            slot = (slot + 1) & mask;
        }
        setTable(handle, capacity, slot, position);
    }

    private void removeSlot(int handle, int capacity, int hole) {
        int mask = 2 * capacity - 1;
        int cursor = (hole + 1) & mask;
        int vacant = hole;
        while (tableAt(handle, capacity, cursor) != EMPTY) {
            int position = tableAt(handle, capacity, cursor);
            int home = OpenAddressing.home(pool.get(handle, position), mask);
            if (OpenAddressing.canMoveInto(vacant, cursor, home, mask)) {
                setTable(handle, capacity, vacant, position);
                vacant = cursor;
            }
            cursor = (cursor + 1) & mask;
        }
        setTable(handle, capacity, vacant, EMPTY);
    }

    private int tableAt(int handle, int capacity, int slot) {
        long word = pool.get(handle, capacity + (slot >>> 1));
        return (int) (word >>> ((slot & 1) << 5)) - 1;
    }

    private void setTable(int handle, int capacity, int slot, int position) {
        int word = capacity + (slot >>> 1);
        int shift = (slot & 1) << 5;
        long mask = 0xFFFF_FFFFL << shift;
        long current = pool.get(handle, word);
        pool.set(handle, word, (current & ~mask) | ((position + 1L & 0xFFFF_FFFFL) << shift));
    }

    private static void checkCapacity(int capacity) {
        if (capacity < 1 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("capacity must be a positive power of two: " + capacity);
        }
    }
}
