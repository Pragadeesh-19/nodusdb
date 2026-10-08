package io.nodusdb.ship;

import io.nodusdb.chain.ChainObject;

import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

public final class ChainRing {

    private static final long COPIES_HELD = 2;

    private final long maxBytes;
    private final ConcurrentSkipListMap<Long, ChainObject> objects = new ConcurrentSkipListMap<>();
    private long bytes;

    public ChainRing(long maxBytes) {
        if (maxBytes < 1) {
            throw new IllegalArgumentException("the ring needs a positive size: " + maxBytes);
        }
        this.maxBytes = maxBytes;
    }

    public synchronized void put(ChainObject object) {
        ChainObject replaced = objects.put(object.seq(), object);
        if (replaced != null) {
            bytes -= cost(replaced);
        }
        bytes += cost(object);
        while (bytes > maxBytes && objects.size() > 1) {
            bytes -= cost(objects.pollFirstEntry().getValue());
        }
    }

    public Optional<ChainObject> get(long seq) {
        return Optional.ofNullable(objects.get(seq));
    }

    public synchronized long bytes() {
        return bytes;
    }

    public int size() {
        return objects.size();
    }

    private static long cost(ChainObject object) {
        return object.encoded().length * COPIES_HELD;
    }
}
