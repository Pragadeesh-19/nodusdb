package io.nodusdb.capi;

import java.util.Arrays;

public final class GraphSessions {

    private static final int INITIAL_SLOTS = 4;
    private static final int SLOT_BITS = 32;
    private static final long SLOT_MASK = 0xFFFF_FFFFL;

    private final Object lock = new Object();
    private GraphSession[] sessions = new GraphSession[INITIAL_SLOTS];
    private int[] generations = new int[INITIAL_SLOTS];
    private int[] freeSlots = new int[INITIAL_SLOTS];
    private int freeTop;
    private int nextSlot;

    public long open() {
        synchronized (lock) {
            int slot;
            if (freeTop > 0) {
                slot = freeSlots[--freeTop];
            } else {
                if (nextSlot == sessions.length) {
                    grow();
                }
                slot = nextSlot++;
            }
            sessions[slot] = new GraphSession();
            return encode(slot, generations[slot]);
        }
    }

    public GraphSession get(long handle) {
        synchronized (lock) {
            return sessions[decodeSlot(handle)];
        }
    }

    public void close(long handle) {
        synchronized (lock) {
            int slot = decodeSlot(handle);
            sessions[slot] = null;
            generations[slot]++;
            freeSlots[freeTop++] = slot;
        }
    }

    private int decodeSlot(long handle) {
        int slot = (int) (handle & SLOT_MASK) - 1;
        int generation = (int) (handle >>> SLOT_BITS);
        if (slot < 0 || slot >= nextSlot || sessions[slot] == null || generations[slot] != generation) {
            throw new IllegalArgumentException("invalid or closed graph handle: " + handle);
        }
        return slot;
    }

    private static long encode(int slot, int generation) {
        return ((long) generation << SLOT_BITS) | (slot + 1L);
    }

    private void grow() {
        int capacity = sessions.length << 1;
        sessions = Arrays.copyOf(sessions, capacity);
        generations = Arrays.copyOf(generations, capacity);
        freeSlots = Arrays.copyOf(freeSlots, capacity);
    }
}
