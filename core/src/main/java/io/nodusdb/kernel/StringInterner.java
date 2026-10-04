package io.nodusdb.kernel;

import java.util.Arrays;
import java.util.Objects;

/*
 * Append-only map between UTF-8 strings and dense ids 0 .. size-1.
 *
 *   slab      [ "user:alice" | "role:admin" | ... ]             all bytes, one array
 *   starts    [ 0 | 10 | ... ]   lengths [ 10 | 10 | ... ]      indexed by id
 *   hashes    [ h0 | h1 | ... ]                                 indexed by id
 *   buckets   [ 0 | 3 | 0 | 1 | ... ]                           open addressing, stores id + 1
 *
 * Ids follow first-seen order and never change. The bucket table grows once it is half full, so a
 * probe stays short. Growth rebuilds the buckets from the stored hashes, so no string is rehashed.
 */
public final class StringInterner {

    private static final int INITIAL_BUCKETS = 1 << 10;
    private static final int INITIAL_RECORDS = 1 << 10;
    private static final int INITIAL_SLAB = 1 << 16;
    private static final int MAX_ARRAY = Integer.MAX_VALUE - 8;
    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;
    private static final long MIX_FIRST = 0xff51afd7ed558ccdL;
    private static final long MIX_SECOND = 0xc4ceb9fe1a85ec53L;

    private byte[] slab = new byte[INITIAL_SLAB];
    private int slabUsed;
    private int[] starts = new int[INITIAL_RECORDS];
    private int[] lengths = new int[INITIAL_RECORDS];
    private long[] hashes = new long[INITIAL_RECORDS];
    private int[] buckets = new int[INITIAL_BUCKETS];
    private int count;

    public synchronized long intern(byte[] utf8, int offset, int length) {
        Objects.requireNonNull(utf8, "utf8");
        Objects.checkFromIndexSize(offset, length, utf8.length);
        long hash = hash(utf8, offset, length);
        int slot = probe(hash, utf8, offset, length);
        if (buckets[slot] != 0) {
            return buckets[slot] - 1;
        }
        int id = append(utf8, offset, length, hash);
        buckets[slot] = id + 1;
        if (2L * count > buckets.length) {
            rebuildBuckets();
        }
        return id;
    }

    public synchronized long lookup(byte[] utf8, int offset, int length) {
        Objects.requireNonNull(utf8, "utf8");
        Objects.checkFromIndexSize(offset, length, utf8.length);
        int slot = probe(hash(utf8, offset, length), utf8, offset, length);
        return buckets[slot] == 0 ? -1 : buckets[slot] - 1;
    }

    public synchronized byte[] resolve(long id) {
        checkKnown(id);
        int index = (int) id;
        return Arrays.copyOfRange(slab, starts[index], starts[index] + lengths[index]);
    }

    public synchronized long size() {
        return count;
    }

    private int probe(long hash, byte[] utf8, int offset, int length) {
        int mask = buckets.length - 1;
        int slot = (int) hash & mask;
        while (buckets[slot] != 0) {
            int id = buckets[slot] - 1;
            if (hashes[id] == hash && lengths[id] == length
                    && Arrays.equals(slab, starts[id], starts[id] + length, utf8, offset, offset + length)) {
                return slot;
            }
            slot = (slot + 1) & mask;
        }
        return slot;
    }

    private int append(byte[] utf8, int offset, int length, long hash) {
        int id = count;
        NodeIds.checkValid(id);
        if (length > MAX_ARRAY - slabUsed) {
            throw new IllegalStateException("string storage limit reached: " + MAX_ARRAY + " bytes");
        }
        if (slabUsed + length > slab.length) {
            slab = Arrays.copyOf(slab, capacityFor(slab.length, slabUsed + length));
        }
        if (id == starts.length) {
            int capacity = capacityFor(starts.length, id + 1);
            starts = Arrays.copyOf(starts, capacity);
            lengths = Arrays.copyOf(lengths, capacity);
            hashes = Arrays.copyOf(hashes, capacity);
        }
        starts[id] = slabUsed;
        lengths[id] = length;
        hashes[id] = hash;
        System.arraycopy(utf8, offset, slab, slabUsed, length);
        slabUsed += length;
        count++;
        return id;
    }

    private void rebuildBuckets() {
        int[] grown = new int[buckets.length << 1];
        int mask = grown.length - 1;
        for (int id = 0; id < count; id++) {
            int slot = (int) hashes[id] & mask;
            while (grown[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            grown[slot] = id + 1;
        }
        buckets = grown;
    }

    private void checkKnown(long id) {
        if (id < 0 || id >= count) {
            throw new IllegalArgumentException("unknown string id: " + id);
        }
    }

    private static int capacityFor(int current, int required) {
        if (required > MAX_ARRAY) {
            throw new IllegalStateException("string storage limit reached: " + MAX_ARRAY + " bytes");
        }
        return (int) Math.min(Math.max(2L * current, required), MAX_ARRAY);
    }

    private static long hash(byte[] utf8, int offset, int length) {
        long h = FNV_OFFSET;
        for (int i = offset; i < offset + length; i++) {
            h ^= utf8[i] & 0xFF;
            h *= FNV_PRIME;
        }
        h ^= h >>> 33;
        h *= MIX_FIRST;
        h ^= h >>> 33;
        h *= MIX_SECOND;
        h ^= h >>> 33;
        return h;
    }
}
