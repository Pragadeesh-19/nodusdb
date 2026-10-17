package io.nodusdb.watch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongSupplier;

final class FallbackReaders implements AutoCloseable {

    static final int MAX_READERS = 4;
    static final Duration IDLE_LIMIT = Duration.ofSeconds(60);

    private final LongSupplier nanoClock;
    private final long idleNanos;
    private final List<FallbackCursor> idle = new ArrayList<>();
    private boolean closed;

    FallbackReaders(LongSupplier nanoClock) {
        this(nanoClock, IDLE_LIMIT);
    }

    FallbackReaders(LongSupplier nanoClock, Duration idleLimit) {
        this.nanoClock = nanoClock;
        this.idleNanos = idleLimit.toNanos();
    }

    synchronized FallbackCursor take(long position) {
        closeExpired();
        Iterator<FallbackCursor> cursors = idle.iterator();
        while (cursors.hasNext()) {
            FallbackCursor cursor = cursors.next();
            if (cursor.position() == position) {
                cursors.remove();
                return cursor;
            }
        }
        return null;
    }

    synchronized void give(FallbackCursor cursor) {
        if (closed) {
            closeAll(List.of(cursor));
            return;
        }
        cursor.touch(nanoClock.getAsLong());
        idle.add(cursor);
        List<FallbackCursor> evicted = new ArrayList<>();
        while (idle.size() > MAX_READERS) {
            evicted.add(idle.remove(0));
        }
        closeAll(evicted);
    }

    synchronized int size() {
        return idle.size();
    }

    @Override
    public synchronized void close() {
        closed = true;
        List<FallbackCursor> all = new ArrayList<>(idle);
        idle.clear();
        closeAll(all);
    }

    private void closeExpired() {
        long now = nanoClock.getAsLong();
        List<FallbackCursor> expired = new ArrayList<>();
        Iterator<FallbackCursor> cursors = idle.iterator();
        while (cursors.hasNext()) {
            FallbackCursor cursor = cursors.next();
            if (now - cursor.lastUsedNanos() >= idleNanos) {
                cursors.remove();
                expired.add(cursor);
            }
        }
        closeAll(expired);
    }

    static void closeAll(List<FallbackCursor> cursors) {
        IOException failure = null;
        for (FallbackCursor cursor : cursors) {
            try {
                cursor.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw new UncheckedIOException("could not close a change reader", failure);
        }
    }
}
