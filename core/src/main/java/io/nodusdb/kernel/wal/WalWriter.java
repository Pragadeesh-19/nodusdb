package io.nodusdb.kernel.wal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

final class WalWriter implements AutoCloseable {

    private final Object monitor = new Object();
    private final CRC32 crc = new CRC32();
    private final Thread flusher;
    private final SyncMode syncMode;
    private final long flushIntervalMillis;
    private final Path log;
    private ByteBuffer filling;
    private ByteBuffer draining;
    private FileChannel channel;
    private long accepted;
    private long persisted;
    private boolean busy;
    private boolean urgent;
    private boolean forceNext;
    private boolean deferWaits;
    private boolean closing;
    private boolean closed;
    private boolean aborted;
    private IOException failure;

    WalWriter(Path log, WalConfig config) throws IOException {
        this.log = log;
        this.syncMode = config.syncMode();
        this.flushIntervalMillis = config.flushIntervalMillis();
        int bytes = config.bufferFrames() * WalFormat.FRAME_BYTES;
        this.filling = ByteBuffer.allocateDirect(bytes).order(ByteOrder.BIG_ENDIAN);
        this.draining = ByteBuffer.allocateDirect(bytes).order(ByteOrder.BIG_ENDIAN);
        this.channel = openAppend(log);
        this.flusher = new Thread(this::runFlusher, "nodus-wal-flusher");
        this.flusher.setDaemon(true);
        this.flusher.start();
    }

    void append(byte op, long u, long v) {
        synchronized (monitor) {
            checkUsable();
            while (filling.remaining() < WalFormat.FRAME_BYTES) {
                monitor.notifyAll();
                awaitIndefinitely();
                checkUsable();
            }
            WalFormat.putFrame(filling, crc, op, u, v);
            accepted++;
            if (syncMode == SyncMode.SYNC && !deferWaits) {
                long target = accepted;
                urgent = true;
                monitor.notifyAll();
                while (persisted < target) {
                    checkUsable();
                    awaitIndefinitely();
                }
            }
        }
    }

    void deferWaits(boolean defer) {
        synchronized (monitor) {
            deferWaits = defer;
        }
    }

    void commitDeferred() {
        if (syncMode == SyncMode.SYNC) {
            drain();
        }
    }

    void drain() {
        synchronized (monitor) {
            checkUsable();
            long target = accepted;
            forceNext = true;
            urgent = true;
            monitor.notifyAll();
            while (persisted < target) {
                checkUsable();
                awaitIndefinitely();
            }
        }
    }

    void replaceLog(Path freshLog) throws IOException {
        synchronized (monitor) {
            checkUsable();
            while (busy) {
                awaitIndefinitely();
            }
            filling.clear();
            channel.close();
            Files.move(freshLog, log, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            channel = openAppend(log);
            persisted = accepted;
            monitor.notifyAll();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (monitor) {
            if (closed) {
                return;
            }
            closing = true;
            monitor.notifyAll();
        }
        joinFlusher();
        closeChannel();
        synchronized (monitor) {
            closed = true;
            if (failure != null) {
                throw failure;
            }
        }
    }

    void abort() {
        synchronized (monitor) {
            if (closed) {
                return;
            }
            aborted = true;
            closing = true;
            filling.clear();
            monitor.notifyAll();
        }
        joinFlusher();
        closeChannelQuietly();
        synchronized (monitor) {
            closed = true;
        }
    }

    private void runFlusher() {
        while (true) {
            ByteBuffer batch;
            FileChannel target;
            long upTo;
            boolean force;
            synchronized (monitor) {
                if (aborted) {
                    return;
                }
                if (!closing && !urgent && filling.remaining() >= WalFormat.FRAME_BYTES) {
                    awaitMillis(flushIntervalMillis);
                }
                if (aborted) {
                    return;
                }
                if (filling.position() == 0 && !forceNext) {
                    if (closing) {
                        return;
                    }
                    continue;
                }
                batch = filling;
                filling = draining;
                draining = batch;
                batch.flip();
                upTo = accepted;
                target = channel;
                force = syncMode == SyncMode.SYNC || forceNext;
                forceNext = false;
                busy = true;
                urgent = false;
                monitor.notifyAll();
            }
            try {
                while (batch.hasRemaining()) {
                    target.write(batch);
                }
                if (force) {
                    target.force(false);
                }
            } catch (IOException e) {
                synchronized (monitor) {
                    failure = e;
                    busy = false;
                    monitor.notifyAll();
                }
                return;
            }
            synchronized (monitor) {
                batch.clear();
                busy = false;
                persisted = upTo;
                monitor.notifyAll();
            }
        }
    }

    private void checkUsable() {
        if (failure != null) {
            throw new UncheckedIOException(failure);
        }
        if (closed) {
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

    private void closeChannel() throws IOException {
        channel.close();
    }

    private void closeChannelQuietly() {
        try {
            channel.close();
        } catch (IOException failed) {
            failure = failed;
        }
    }

    private static FileChannel openAppend(Path log) throws IOException {
        return FileChannel.open(log, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }
}
