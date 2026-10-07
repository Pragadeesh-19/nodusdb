package io.nodusdb.kernel.symbols;

import io.nodusdb.kernel.NodeIds;
import io.nodusdb.kernel.concurrency.WriteSequence;
import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.kernel.memory.NativeLongArray;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Objects;

public final class SymbolTable {

    public static final int MAX_SYMBOLS = (int) Math.min(NodeIds.MAX_NODE_ID + 1, (Integer.MAX_VALUE - 8) / 2);

    private static final int SLAB_CHUNK_BYTES = 1 << 20;
    private static final int INITIAL_SYMBOLS = 1 << 10;
    private static final int INITIAL_BUCKETS = 1 << 11;
    private static final long HASH_MASK = 0xFFFF_FFFFL;
    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;
    private static final long MIX_FIRST = 0xff51afd7ed558ccdL;
    private static final long MIX_SECOND = 0xc4ceb9fe1a85ec53L;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

    private final Arena arena;
    private final MemoryBudget budget;
    private final WriteSequence sequence = new WriteSequence();
    private NativeLongArray entries;
    private NativeLongArray buckets;
    private int bucketCount;
    private MemorySegment[] slab = new MemorySegment[0];
    private int slabUsed;
    private volatile int count;

    public SymbolTable(Arena arena, MemoryBudget budget) {
        this.arena = Objects.requireNonNull(arena, "arena");
        this.budget = Objects.requireNonNull(budget, "budget");
    }

    public int size() {
        return count;
    }

    public int lookup(byte[] utf8, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, utf8.length);
        long hash = hash(utf8, offset, length);
        while (true) {
            long started = sequence.beginRead();
            int id = find(hash, utf8, offset, length);
            if (sequence.validate(started)) {
                return id;
            }
        }
    }

    public byte[] resolve(int id) {
        if (id < 0 || id >= count) {
            throw new IllegalArgumentException("unknown symbol id: " + id);
        }
        int length = lengthOf(id);
        byte[] bytes = new byte[length];
        long location = entries.get(2 * id);
        MemorySegment.copy(slab[(int) (location >>> Integer.SIZE)], BYTE, (int) location,
                bytes, 0, length);
        return bytes;
    }

    public void append(int id, byte[] utf8, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, utf8.length);
        if (id != count) {
            throw new IllegalStateException("symbol id " + id + " is not the next id " + count);
        }
        long hash = hash(utf8, offset, length);
        int existing = find(hash, utf8, offset, length);
        if (existing >= 0) {
            throw new IllegalStateException("symbol is already defined as id " + existing);
        }
        reserve(1, length);
        long location = copyIntoSlab(utf8, offset, length);
        entries.set(2 * id, location);
        entries.set(2 * id + 1, (hash & HASH_MASK) | ((long) length << Integer.SIZE));
        insert(hash, id);
        count = id + 1;
    }

    public long bytesToReserve(int additionalSymbols, long additionalBytes) {
        return plan(additionalSymbols, additionalBytes).totalBytes();
    }

    public void reserve(int additionalSymbols, long additionalBytes) {
        Reservation plan = plan(additionalSymbols, additionalBytes);
        budget.require(plan.totalBytes());
        if (entries == null) {
            entries = new NativeLongArray(arena, budget, 2 * INITIAL_SYMBOLS, 0L);
            buckets = new NativeLongArray(arena, budget, INITIAL_BUCKETS, 0L);
            bucketCount = INITIAL_BUCKETS;
        }
        entries.ensureCapacity(plan.entryCount());
        if (plan.slabBytes() > 0) {
            addSlabChunk((int) plan.slabBytes());
        }
        if (plan.buckets() != bucketCount) {
            rebuildBuckets(plan.buckets());
        }
    }

    private record Reservation(int entryCount, int buckets, long entryBytes, long bucketBytes, long slabBytes) {

        long totalBytes() {
            return entryBytes + bucketBytes + slabBytes;
        }
    }

    private Reservation plan(int additionalSymbols, long additionalBytes) {
        long target = (long) count + additionalSymbols;
        if (target > MAX_SYMBOLS) {
            throw new IllegalStateException("symbol table is full: " + MAX_SYMBOLS + " symbols");
        }
        boolean initialized = entries != null;
        int currentBuckets = initialized ? bucketCount : INITIAL_BUCKETS;
        int neededBuckets = currentBuckets;
        while (2L * target > neededBuckets) {
            neededBuckets <<= 1;
        }
        long initialBytes = initialized ? 0 : (long) (2 * INITIAL_SYMBOLS + INITIAL_BUCKETS) * Long.BYTES;
        long bucketBytes = neededBuckets != currentBuckets ? (long) neededBuckets * Long.BYTES : 0;
        boolean needsChunk = slab.length == 0 || (additionalBytes > 0 && slabRemaining() < additionalBytes);
        long slabBytes = needsChunk ? Math.max(SLAB_CHUNK_BYTES, additionalBytes) : 0;
        int entryCount = (int) (2 * target);
        long entryBytes = initialized ? entries.bytesToReach(entryCount) : 0;
        return new Reservation(entryCount, neededBuckets, initialBytes + entryBytes, bucketBytes, slabBytes);
    }

    private int find(long hash, byte[] utf8, int offset, int length) {
        int published = count;
        NativeLongArray table = buckets;
        if (table == null) {
            return -1;
        }
        int mask = bucketCount - 1;
        int slot = (int) hash & mask;
        for (int probes = 0; probes <= mask; probes++) {
            long stored = table.get(slot);
            if (stored == 0) {
                return -1;
            }
            int id = (int) stored - 1;
            if (id < published && matches(id, hash, utf8, offset, length)) {
                return id;
            }
            slot = (slot + 1) & mask;
        }
        return -1;
    }

    private boolean matches(int id, long hash, byte[] utf8, int offset, int length) {
        long meta = entries.get(2 * id + 1);
        if ((meta & HASH_MASK) != (hash & HASH_MASK) || (int) (meta >>> Integer.SIZE) != length) {
            return false;
        }
        long location = entries.get(2 * id);
        MemorySegment[] chunks = slab;
        int chunk = (int) (location >>> Integer.SIZE);
        int start = (int) location;
        if (chunk >= chunks.length || start + (long) length > chunks[chunk].byteSize()) {
            return false;
        }
        MemorySegment segment = chunks[chunk];
        for (int i = 0; i < length; i++) {
            if (segment.get(BYTE, start + i) != utf8[offset + i]) {
                return false;
            }
        }
        return true;
    }

    private int lengthOf(int id) {
        return (int) (entries.get(2 * id + 1) >>> Integer.SIZE);
    }

    private long slabRemaining() {
        return slab.length == 0 ? 0 : slab[slab.length - 1].byteSize() - slabUsed;
    }

    private void addSlabChunk(int bytes) {
        budget.charge(bytes);
        slab = Arrays.copyOf(slab, slab.length + 1);
        slab[slab.length - 1] = arena.allocate(bytes, 1);
        slabUsed = 0;
    }

    private long copyIntoSlab(byte[] utf8, int offset, int length) {
        int chunk = slab.length - 1;
        MemorySegment.copy(utf8, offset, slab[chunk], BYTE, slabUsed, length);
        long location = ((long) chunk << Integer.SIZE) | slabUsed;
        slabUsed += length;
        return location;
    }

    private void insert(long hash, int id) {
        int mask = bucketCount - 1;
        int slot = (int) hash & mask;
        while (buckets.get(slot) != 0) {
            slot = (slot + 1) & mask;
        }
        buckets.set(slot, id + 1L);
    }

    private void rebuildBuckets(int newCount) {
        NativeLongArray grown = new NativeLongArray(arena, budget, newCount, 0L);
        int mask = newCount - 1;
        for (int id = 0; id < count; id++) {
            int slot = (int) entries.get(2 * id + 1) & mask;
            while (grown.get(slot) != 0) {
                slot = (slot + 1) & mask;
            }
            grown.set(slot, id + 1L);
        }
        sequence.beginWrite();
        buckets = grown;
        bucketCount = newCount;
        sequence.endWrite();
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
