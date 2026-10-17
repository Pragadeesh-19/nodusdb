package io.nodusdb.kernel.changes;

import io.nodusdb.kernel.concurrency.WriteSequence;
import io.nodusdb.log.record.RecordReader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public final class ChangeRing {

    public static final int MIN_CAPACITY = 1 << 16;
    public static final int MAX_CAPACITY = 1 << 26;
    public static final int DEFAULT_CAPACITY = 1 << 20;

    private static final VarHandle LIMIT;
    private static final VarHandle PUBLISHED;
    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);
    private static final int MAX_ATTEMPTS = 8;
    private static final long TUPLE_ENTRIES = 2;
    private static final int HELD = 0;
    private static final int NOT_HELD = 1;
    private static final int RETRY = 2;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            LIMIT = lookup.findVarHandle(ChangeRing.class, "limit", long.class);
            PUBLISHED = lookup.findVarHandle(ChangeRing.class, "published", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long[] words;
    private final int capacity;
    private final int mask;
    private final WriteSequence spans = new WriteSequence();
    private final RecordReader reader = new RecordReader();
    private final Writer writer = new Writer();
    private final Object signal = new Object();
    private long written;
    private long limit;
    private long published;
    private long spanStart;
    private long spanStartLsn;
    private volatile long publishedLsn;
    private volatile int waiters;
    private volatile boolean closed;

    public ChangeRing(int capacity, long startLsn) {
        requireCapacity(capacity);
        if (startLsn < 0) {
            throw new IllegalArgumentException("the starting LSN must not be negative: " + startLsn);
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.words = new long[capacity * ChangeEntry.LONGS];
        this.spanStartLsn = startLsn;
        this.publishedLsn = startLsn;
    }

    public static int requireCapacity(int capacity) {
        if (capacity < MIN_CAPACITY || capacity > MAX_CAPACITY || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("the change buffer holds a power of two between " + MIN_CAPACITY
                    + " and " + MAX_CAPACITY + " changes: " + capacity);
        }
        return capacity;
    }

    public int capacity() {
        return capacity;
    }

    public long publishedLsn() {
        return publishedLsn;
    }

    public long servesFromLsn() {
        while (true) {
            long started = spans.beginRead();
            long start = spanStart;
            long startLsn = spanStartLsn;
            long oldest = (long) LIMIT.getAcquire(this) - capacity;
            long servesFrom = oldest <= start ? startLsn : lsnAt(oldest);
            if (spans.validate(started)) {
                return servesFrom;
            }
        }
    }

    public void record(ByteBuffer bytes, int start, int end, long lastLsn, long commitMicros) {
        reader.wrap(bytes, start, end);
        int records = 0;
        boolean endsOnCommit = false;
        while (reader.hasRecord()) {
            endsOnCommit = reader.isCommitPoint();
            records++;
            reader.advance();
        }
        if (records == 0) {
            throw new IllegalArgumentException("a transaction holds at least one record");
        }
        if (!endsOnCommit) {
            throw new IllegalArgumentException("a transaction ends on its commit point, LSN " + lastLsn);
        }
        long firstLsn = lastLsn - records + 1;
        if (firstLsn < 1) {
            throw new IllegalArgumentException(records + " records cannot end at LSN " + lastLsn);
        }
        if (!reserve(firstLsn, lastLsn, (long) records + 1)) {
            return;
        }
        reader.rewind();
        long lsn = firstLsn;
        while (reader.hasRecord()) {
            ChangeExtractor.extract(reader, lsn++, commitMicros, writer);
            reader.advance();
        }
        complete(lastLsn);
    }

    public void recordTuple(int kind, int object, int relation, int subjectRelation, int subject, long lsn,
                            long commitMicros) {
        if (!ChangeEntry.isTuple(kind)) {
            throw new IllegalArgumentException("a tuple change is an add or a remove: " + kind);
        }
        if (lsn < 1) {
            throw new IllegalArgumentException("a tuple change has a positive LSN: " + lsn);
        }
        if (!reserve(lsn, lsn, TUPLE_ENTRIES)) {
            return;
        }
        ChangeExtractor.autocommitTuple(kind, object, relation, subjectRelation, subject, lsn, commitMicros, writer);
        complete(lsn);
    }

    public boolean read(long afterLsn, long boundLsn, int maxChanges, ChangeBuffer out) {
        if (maxChanges < 1) {
            throw new IllegalArgumentException("a read returns at least one change: " + maxChanges);
        }
        int mark = out.size();
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            long started = spans.beginRead();
            long start = spanStart;
            long startLsn = spanStartLsn;
            long end = (long) PUBLISHED.getAcquire(this);
            if (!spans.validate(started)) {
                continue;
            }
            int verdict = copy(afterLsn, boundLsn, maxChanges, out, start, startLsn, end);
            if (verdict == RETRY || !spans.validate(started)) {
                out.truncate(mark);
                continue;
            }
            if (verdict == NOT_HELD) {
                out.truncate(mark);
                return false;
            }
            return true;
        }
        return false;
    }

    public boolean awaitBeyond(long lsn, long timeoutNanos) {
        if (publishedLsn > lsn || closed || timeoutNanos <= 0) {
            return publishedLsn > lsn;
        }
        long deadline = System.nanoTime() + timeoutNanos;
        synchronized (signal) {
            waiters++;
            try {
                long remaining = timeoutNanos;
                while (publishedLsn <= lsn && !closed && remaining > 0) {
                    signal.wait(remaining / NANOS_PER_MILLI, (int) (remaining % NANOS_PER_MILLI));
                    remaining = deadline - System.nanoTime();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                waiters--;
            }
        }
        return publishedLsn > lsn;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        closed = true;
        synchronized (signal) {
            signal.notifyAll();
        }
    }

    private int copy(long afterLsn, long boundLsn, int maxChanges, ChangeBuffer out, long start, long startLsn,
                     long end) {
        long oldest = Math.max(start, (long) LIMIT.getAcquire(this) - capacity);
        if (oldest >= end) {
            return end == start && afterLsn >= startLsn ? HELD : NOT_HELD;
        }
        long lowestRead = end;
        long low = oldest;
        long high = end;
        while (low < high) {
            long middle = (low + high) >>> 1;
            lowestRead = Math.min(lowestRead, middle);
            if (lsnAt(middle) > afterLsn) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        long transactionStart = low;
        if (transactionStart < end) {
            while (transactionStart > oldest && kindAt(transactionStart - 1) != ChangeEntry.COMMIT) {
                transactionStart--;
            }
            lowestRead = Math.min(lowestRead, transactionStart > oldest ? transactionStart - 1 : transactionStart);
            if (transactionStart == oldest && !(oldest == start && afterLsn >= startLsn)) {
                return overwrittenSince(lowestRead) ? RETRY : NOT_HELD;
            }
        }
        int kept = out.size();
        for (long index = transactionStart; index < end; index++) {
            int at = slot(index);
            long lsn = words[at];
            long meta = words[at + 1];
            out.entry(lsn, meta, words[at + 2]);
            if (ChangeEntry.kind(meta) != ChangeEntry.COMMIT) {
                continue;
            }
            if (lsn > boundLsn) {
                out.truncate(kept);
                break;
            }
            kept = out.size();
            if (out.changes() >= maxChanges) {
                break;
            }
        }
        if (kept < out.size()) {
            out.truncate(kept);
        }
        return overwrittenSince(lowestRead) ? RETRY : HELD;
    }

    private boolean overwrittenSince(long lowestRead) {
        VarHandle.acquireFence();
        return lowestRead < (long) LIMIT.getAcquire(this) - capacity;
    }

    private long lsnAt(long index) {
        return words[slot(index)];
    }

    private int kindAt(long index) {
        return ChangeEntry.kind(words[slot(index) + 1]);
    }

    private int slot(long index) {
        return (int) (index & mask) * ChangeEntry.LONGS;
    }

    private boolean reserve(long firstLsn, long lastLsn, long entries) {
        if (firstLsn != publishedLsn + 1) {
            restartSpan(firstLsn - 1);
        }
        if (entries > capacity) {
            restartSpan(lastLsn);
            publish(lastLsn);
            return false;
        }
        LIMIT.setOpaque(this, written + entries);
        VarHandle.releaseFence();
        return true;
    }

    private void complete(long lastLsn) {
        PUBLISHED.setRelease(this, written);
        publish(lastLsn);
    }

    private void restartSpan(long lsn) {
        spans.beginWrite();
        try {
            spanStart = written;
            spanStartLsn = lsn;
        } finally {
            spans.endWrite();
        }
    }

    private void publish(long lsn) {
        publishedLsn = lsn;
        if (waiters != 0) {
            synchronized (signal) {
                signal.notifyAll();
            }
        }
    }

    private final class Writer implements EntrySink {

        @Override
        public void entry(long lsn, long meta, long payload) {
            int at = slot(written);
            words[at] = lsn;
            words[at + 1] = meta;
            words[at + 2] = payload;
            written++;
        }
    }
}
