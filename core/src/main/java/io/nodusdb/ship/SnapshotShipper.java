package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.objectstore.TransientStoreException;

import java.util.concurrent.TimeUnit;

public final class SnapshotShipper implements AutoCloseable {

    private static final long JOIN_MILLIS = 10_000;
    private static final long INTERRUPTED_JOIN_MILLIS = 2_000;

    private final SnapshotUploader uploader;
    private final ShipState state;
    private final ShipSettings settings;
    private final Object lock = new Object();
    private final Thread thread;
    private StagedSnapshot waiting;
    private boolean closing;

    private SnapshotShipper(SnapshotUploader uploader, ShipState state, ShipSettings settings, String name) {
        this.uploader = uploader;
        this.state = state;
        this.settings = settings;
        this.thread = new Thread(this::run, name);
        this.thread.setDaemon(true);
    }

    public static SnapshotShipper start(SnapshotUploader uploader, ShipState state, ShipSettings settings,
                                        String name) {
        SnapshotShipper shipper = new SnapshotShipper(uploader, state, settings, name);
        shipper.thread.start();
        return shipper;
    }

    public void submit(StagedSnapshot staged) {
        StagedSnapshot replaced;
        synchronized (lock) {
            if (closing) {
                staged.close();
                throw new IllegalStateException("the snapshot shipper is closed");
            }
            replaced = waiting;
            waiting = staged;
            lock.notifyAll();
        }
        if (replaced != null) {
            replaced.close();
        }
    }

    @Override
    public void close() {
        StagedSnapshot abandoned;
        synchronized (lock) {
            closing = true;
            abandoned = waiting;
            waiting = null;
            lock.notifyAll();
        }
        if (abandoned != null) {
            abandoned.close();
        }
        join(JOIN_MILLIS);
        if (thread.isAlive()) {
            thread.interrupt();
            join(INTERRUPTED_JOIN_MILLIS);
        }
    }

    private void join(long millis) {
        try {
            thread.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        while (true) {
            StagedSnapshot current = awaitSubmission();
            if (current == null) {
                return;
            }
            try {
                ship(current);
            } finally {
                current.close();
            }
        }
    }

    private StagedSnapshot awaitSubmission() {
        synchronized (lock) {
            try {
                while (waiting == null && !closing) {
                    lock.wait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            if (closing) {
                return null;
            }
            StagedSnapshot next = waiting;
            waiting = null;
            return next;
        }
    }

    private void ship(StagedSnapshot current) {
        int attempt = 0;
        while (true) {
            try {
                ChainBody.SnapshotRef reference = uploader.upload(current.file(), current.lsn(),
                        state.snapshot().chainSeq() + 1);
                state.offerReference(reference);
                return;
            } catch (WriterFencedException conflict) {
                state.fenced(conflict.getMessage());
                return;
            } catch (ObjectStoreException failure) {
                state.referenceFailed(failure.getMessage());
                attempt++;
                long delay = failure instanceof TransientStoreException ? settings.backoffNanos(attempt)
                        : settings.failedRetry().toNanos();
                if (!backOff(delay)) {
                    return;
                }
            } catch (RuntimeException unexpected) {
                state.referenceFailed("the snapshot upload failed: " + unexpected);
                return;
            }
        }
    }

    private boolean backOff(long nanos) {
        long deadline = System.nanoTime() + nanos;
        synchronized (lock) {
            try {
                while (waiting == null && !closing) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return true;
                    }
                    TimeUnit.NANOSECONDS.timedWait(lock, remaining);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
