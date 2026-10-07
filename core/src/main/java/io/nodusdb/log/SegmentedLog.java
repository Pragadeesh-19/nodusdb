package io.nodusdb.log;

import io.nodusdb.error.IndeterminateOutcomeException;
import io.nodusdb.log.io.LogChannel;
import io.nodusdb.log.io.LogFileSystem;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFormat;
import io.nodusdb.log.record.SegmentHeader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

public final class SegmentedLog implements LogStore {

    private record Work(ByteBuffer batch, long upToLsn, long firstLsn, boolean force, boolean roll, boolean mark,
                        long serial) {

        boolean hasData() {
            return batch != null;
        }
    }

    private final Object appendLock = new Object();
    private final Object monitor = new Object();
    private final LogFileSystem files;
    private final LogConfig config;
    private final LongSupplier clockMicros;
    private final ForcedMark forcedMark;
    private final Thread flusher;
    private final long epoch;
    private final List<Long> segmentBases;

    private ByteBuffer filling;
    private ByteBuffer draining;
    private long lastLsn;
    private long fillFirstLsn;
    private long durableLsn;
    private long lastCommitMicros;
    private long requestedSerial;
    private long completedSerial;
    private boolean forceRequested;
    private boolean rollRequested;
    private boolean markRequested;
    private boolean urgent;
    private boolean closing;
    private boolean closed;
    private boolean aborted;
    private RuntimeException failure;

    private LogChannel channel;
    private long activeBase;
    private long activeSize;
    private long forcedBase;
    private long forcedOffset;
    private long forcedThroughLsn;
    private boolean markDirty;
    private long lastMarkNanos;

    private SegmentedLog(LogFileSystem files, LogConfig config, LongSupplier clockMicros, ForcedMark forcedMark,
                         RecoveryResult recovered, long epoch) {
        this.files = files;
        this.config = config;
        this.clockMicros = clockMicros;
        this.forcedMark = forcedMark;
        this.epoch = epoch;
        this.segmentBases = new ArrayList<>(recovered.segmentBases());
        this.filling = ByteBuffer.allocateDirect(config.bufferBytes());
        this.draining = ByteBuffer.allocateDirect(config.bufferBytes());
        this.lastLsn = recovered.lastLsn();
        this.durableLsn = recovered.lastLsn();
        this.forcedThroughLsn = recovered.lastLsn();
        this.lastCommitMicros = recovered.lastCommitMicros();
        this.flusher = new Thread(this::runFlusher, "nodus-log-flusher");
        this.flusher.setDaemon(true);
    }

    public static SegmentedLog open(LogFileSystem files, LogConfig config, RecoveryResult recovered, long epoch,
                                    int writerKey, LongSupplier clockMicros) throws IOException {
        ForcedMark mark = ForcedMark.open(files);
        SegmentedLog log = new SegmentedLog(files, config, clockMicros, mark, recovered, epoch);
        try {
            log.attachTail(recovered);
            log.flusher.start();
            log.beginTenure(writerKey, recovered.lastLsn());
            return log;
        } catch (IOException | RuntimeException e) {
            log.abort();
            throw e;
        }
    }

    @Override
    public long epoch() {
        return epoch;
    }

    @Override
    public long lastLsn() {
        synchronized (monitor) {
            return lastLsn;
        }
    }

    @Override
    public long durableLsn() {
        synchronized (monitor) {
            return durableLsn;
        }
    }

    @Override
    public long append(RecordBatch batch) {
        synchronized (appendLock) {
            synchronized (monitor) {
                checkUsable();
                long firstLsn = lastLsn + 1;
                lastCommitMicros = Math.max(clockMicros.getAsLong(), lastCommitMicros + 1);
                batch.seal(firstLsn, lastCommitMicros);
                copyIntoBuffer(batch);
                return lastLsn;
            }
        }
    }

    @Override
    public void awaitDurable(long lsn) {
        if (config.syncMode() != SyncMode.SYNC) {
            return;
        }
        synchronized (monitor) {
            if (durableLsn < lsn) {
                urgent = true;
                monitor.notifyAll();
            }
            while (durableLsn < lsn) {
                checkUsable();
                awaitIndefinitely();
            }
        }
    }

    @Override
    public void force() {
        synchronized (appendLock) {
            synchronized (monitor) {
                checkUsable();
                forceRequested = true;
                markRequested = true;
                awaitExplicitRequest();
            }
        }
    }

    @Override
    public long rollSegment() {
        synchronized (appendLock) {
            synchronized (monitor) {
                checkUsable();
                rollRequested = true;
                forceRequested = true;
                markRequested = true;
                awaitExplicitRequest();
                return activeBase;
            }
        }
    }

    @Override
    public void trim(long throughLsn) {
        synchronized (appendLock) {
            synchronized (monitor) {
                checkUsable();
                try {
                    while (segmentBases.size() > 1 && segmentBases.get(1) - 1 <= throughLsn) {
                        files.delete(SegmentNames.of(segmentBases.remove(0)));
                    }
                    files.trySyncDirectory();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (appendLock) {
            synchronized (monitor) {
                if (closed || closing) {
                    return;
                }
            }
            RuntimeException pendingFailure = null;
            try {
                force();
            } catch (RuntimeException e) {
                pendingFailure = e;
            }
            synchronized (monitor) {
                closing = true;
                monitor.notifyAll();
            }
            joinFlusher();
            releaseChannels();
            synchronized (monitor) {
                closed = true;
            }
            if (pendingFailure != null) {
                throw pendingFailure;
            }
        }
    }

    public void abort() {
        synchronized (monitor) {
            if (closed) {
                return;
            }
            aborted = true;
            closing = true;
            monitor.notifyAll();
        }
        if (flusher.isAlive()) {
            joinFlusher();
        }
        releaseChannelsQuietly();
        synchronized (monitor) {
            closed = true;
        }
    }

    public List<Long> segmentBases() {
        synchronized (monitor) {
            return List.copyOf(segmentBases);
        }
    }

    private void attachTail(RecoveryResult recovered) throws IOException {
        if (recovered.hasSegments()) {
            activeBase = recovered.tailBase();
            activeSize = recovered.tailOffset();
            channel = files.open(SegmentNames.of(activeBase));
            channel.truncate(activeSize);
            channel.force();
        } else {
            createSegment(recovered.lastLsn() + 1);
        }
        forcedBase = activeBase;
        forcedOffset = activeSize;
        forcedMark.record(forcedBase, forcedOffset);
        lastMarkNanos = System.nanoTime();
    }

    private void createSegment(long baseLsn) throws IOException {
        LogChannel created = files.create(SegmentNames.of(baseLsn));
        ByteBuffer header = ByteBuffer.allocate(SegmentHeader.BYTES);
        SegmentHeader.write(header, 0, clockMicros.getAsLong(), baseLsn);
        created.write(header, 0);
        created.force();
        files.trySyncDirectory();
        channel = created;
        activeBase = baseLsn;
        activeSize = SegmentHeader.BYTES;
        synchronized (monitor) {
            segmentBases.add(baseLsn);
        }
    }

    private void beginTenure(int writerKey, long handoffLsn) {
        RecordBatch tenure = new RecordBatch();
        tenure.epoch(epoch, writerKey, handoffLsn);
        tenure.commit();
        append(tenure);
        force();
    }

    private void copyIntoBuffer(RecordBatch batch) {
        ByteBuffer bytes = batch.bytes();
        int end = batch.size();
        int offset = 0;
        while (offset < end) {
            int length = bytes.getInt(offset + RecordFormat.LENGTH_OFFSET);
            while (filling.remaining() < length) {
                urgent = true;
                monitor.notifyAll();
                awaitIndefinitely();
                checkUsable();
            }
            long lsn = bytes.getLong(offset + RecordFormat.LSN_OFFSET);
            if (filling.position() == 0) {
                fillFirstLsn = lsn;
            }
            filling.put(filling.position(), bytes, offset, length);
            filling.position(filling.position() + length);
            lastLsn = lsn;
            offset += length;
        }
    }

    private void awaitExplicitRequest() {
        long ticket = ++requestedSerial;
        urgent = true;
        monitor.notifyAll();
        while (completedSerial < ticket) {
            checkUsable();
            awaitIndefinitely();
        }
    }

    private void runFlusher() {
        while (true) {
            Work work;
            synchronized (monitor) {
                work = awaitWork();
                if (work == null) {
                    return;
                }
            }
            if (!perform(work)) {
                return;
            }
        }
    }

    private Work awaitWork() {
        boolean waited = false;
        while (true) {
            if (aborted) {
                return null;
            }
            boolean hasData = filling.position() > 0;
            boolean requested = forceRequested || rollRequested || markRequested;
            if (!hasData && !requested) {
                if (closing) {
                    return null;
                }
                if (markIsDue()) {
                    return takeWork(false);
                }
                awaitMillis(idleWaitMillis());
                waited = false;
            } else if (!waited && !urgent && !closing && !requested) {
                awaitMillis(config.flushIntervalMillis());
                waited = true;
            } else {
                return takeWork(hasData);
            }
        }
    }

    private Work takeWork(boolean hasData) {
        ByteBuffer batch = null;
        if (hasData) {
            batch = filling;
            filling = draining;
            draining = batch;
            batch.flip();
        }
        Work work = new Work(batch, lastLsn, hasData ? fillFirstLsn : lastLsn + 1,
                config.syncMode() == SyncMode.SYNC || forceRequested, rollRequested, markRequested, requestedSerial);
        forceRequested = false;
        rollRequested = false;
        markRequested = false;
        urgent = false;
        monitor.notifyAll();
        return work;
    }

    private boolean perform(Work work) {
        try {
            if (shouldRoll(work)) {
                roll(work.firstLsn());
            }
            if (work.hasData()) {
                channel.write(work.batch(), activeSize);
                activeSize += work.batch().limit();
            }
            if (work.force()) {
                channel.force();
                recordForced(work.upToLsn());
            }
            if (work.mark() || markIsDue()) {
                forcedMark.record(forcedBase, forcedOffset);
                markDirty = false;
                lastMarkNanos = System.nanoTime();
            }
        } catch (IOException | RuntimeException e) {
            fail(e);
            return false;
        }
        synchronized (monitor) {
            if (work.hasData()) {
                work.batch().clear();
            }
            durableLsn = forcedThroughLsn;
            completedSerial = work.serial();
            monitor.notifyAll();
        }
        return true;
    }

    private boolean shouldRoll(Work work) {
        boolean hasRecords = activeSize > SegmentHeader.BYTES;
        return hasRecords && (work.roll() || activeSize >= config.segmentBytes());
    }

    private void roll(long nextBase) throws IOException {
        channel.force();
        recordForced(nextBase - 1);
        channel.close();
        createSegment(nextBase);
        forcedBase = activeBase;
        forcedOffset = activeSize;
        markDirty = true;
    }

    private void recordForced(long throughLsn) {
        forcedBase = activeBase;
        forcedOffset = activeSize;
        forcedThroughLsn = throughLsn;
        markDirty = true;
    }

    private boolean markIsDue() {
        return markDirty && System.nanoTime() - lastMarkNanos >= config.markIntervalMillis() * 1_000_000L;
    }

    private long idleWaitMillis() {
        if (!markDirty) {
            return config.flushIntervalMillis();
        }
        long elapsed = (System.nanoTime() - lastMarkNanos) / 1_000_000L;
        return Math.max(1L, config.markIntervalMillis() - elapsed);
    }

    private void fail(Exception cause) {
        synchronized (monitor) {
            failure = new IndeterminateOutcomeException(
                    "the write-ahead log failed and its last records may or may not be durable; reopen the graph",
                    cause);
            monitor.notifyAll();
        }
    }

    private void checkUsable() {
        if (failure != null) {
            throw failure;
        }
        if (closed || closing) {
            throw new IllegalStateException("write-ahead log is closed");
        }
    }

    private void awaitIndefinitely() {
        try {
            monitor.wait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the write-ahead log", e);
        }
    }

    private void awaitMillis(long millis) {
        try {
            monitor.wait(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void joinFlusher() {
        try {
            flusher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing the write-ahead log", e);
        }
    }

    private void releaseChannels() {
        try {
            channel.close();
            forcedMark.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void releaseChannelsQuietly() {
        closeQuietly(channel);
        closeQuietly(forcedMark);
    }

    private static boolean closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return true;
        }
        try {
            resource.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
