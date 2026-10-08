package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.error.ShipTimeoutException;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.log.ShipWatermark;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class ShipState implements ShipWatermark, ShippingLogStore.Gate {

    public enum Phase {
        STARTING, ACTIVE, RETRYING, FAILED, FENCED, CLOSED
    }

    public enum BacklogEvent {
        NONE, WARNING, REFUSING, RESUMED
    }

    public record Snapshot(Phase phase, long epoch, long shippedLsn, long chainSeq, long backlogBytes,
                           long backlogCapBytes, long consecutiveFailures, String lastError, long objectsShipped,
                           long bytesShipped, long lastCommitNanos, long nowNanos, ReferenceStatus references,
                           RetentionStatus retention, ProjectionStatus projection) {
    }

    public record Event(long seq, String message) {
    }

    public record ReferenceStatus(long seq, long lsn, long committedNanos, long failures, String lastError) {
    }

    public record RetentionStatus(long sweeps, long chainObjectsDeleted, long snapshotsDeleted, long failures,
                                  String lastError) {
    }

    public enum ProjectionPhase {
        DISABLED, STARTING, ACTIVE, RETRYING, FAILED, FENCED, CLOSED
    }

    public record ProjectionStatus(ProjectionPhase phase, long projectedLsn, long projectedSeq, long snapshotId,
                                   long lastCommitNanos, long commits, long rows, long failures, String lastError) {
    }

    private static final int MAX_EVENTS = 64;
    private static final long DEFAULT_RING_BYTES = 32L << 20;

    private final Object monitor = new Object();
    private final long epoch;
    private final long backlogCapBytes;
    private final ChainRing ring;
    private final Deque<Event> events = new ArrayDeque<>();
    private long eventSeq;
    private Phase phase = Phase.STARTING;
    private long shippedLsn;
    private long chainSeq;
    private long backlogBytes;
    private boolean warned;
    private boolean refusing;
    private long consecutiveFailures;
    private String lastError = "";
    private long objectsShipped;
    private long bytesShipped;
    private long lastCommitNanos;
    private int waiters;
    private boolean workPending;
    private boolean stopRequested;
    private ChainBody.SnapshotRef offeredReference;
    private long referenceSeq;
    private long referenceLsn = -1;
    private long referenceNanos;
    private long referenceFailures;
    private String referenceError = "";
    private long sweeps;
    private long chainObjectsDeleted;
    private long snapshotsDeleted;
    private long retentionFailures;
    private String retentionError = "";
    private ProjectionPhase projectionPhase = ProjectionPhase.DISABLED;
    private long projectedLsn = -1;
    private volatile long projectedSeq;
    private long icebergSnapshotId = -1;
    private long projectionCommitNanos;
    private long projectionCommits;
    private long projectionRows;
    private long projectionFailures;
    private String projectionError = "";

    public ShipState(long epoch, long shippedLsn, long chainSeq, long backlogCapBytes) {
        this(epoch, shippedLsn, chainSeq, backlogCapBytes, DEFAULT_RING_BYTES);
    }

    public ShipState(long epoch, long shippedLsn, long chainSeq, long backlogCapBytes, long ringBytes) {
        this.epoch = epoch;
        this.shippedLsn = shippedLsn;
        this.chainSeq = chainSeq;
        this.backlogCapBytes = backlogCapBytes;
        this.ring = new ChainRing(ringBytes);
    }

    public ChainRing ring() {
        return ring;
    }

    @Override
    public boolean configured() {
        return true;
    }

    @Override
    public long shippedLsn() {
        synchronized (monitor) {
            return shippedLsn;
        }
    }

    @Override
    public String fencedReason() {
        synchronized (monitor) {
            return phase == Phase.FENCED ? lastError : null;
        }
    }

    @Override
    public String refusal() {
        synchronized (monitor) {
            return refusing ? "the unshipped log holds " + backlogBytes + " bytes, which reaches the cap of "
                    + backlogCapBytes + " bytes; writes resume once shipping catches up" : null;
        }
    }

    @Override
    public void awaitShipped(long tokenEpoch, long lsn, long timeoutNanos) {
        long deadline = System.nanoTime() + timeoutNanos;
        synchronized (monitor) {
            if (shippedLsn >= lsn) {
                return;
            }
            waiters++;
            workPending = true;
            monitor.notifyAll();
            try {
                while (shippedLsn < lsn) {
                    failFastIfTerminal(tokenEpoch, lsn);
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        throw new ShipTimeoutException("LSN " + lsn + " had not reached the object store when the "
                                + "wait ended; shipping is " + phase + (lastError.isEmpty() ? "" : ": " + lastError),
                                tokenEpoch, lsn);
                    }
                    TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ShipTimeoutException("the wait for shipping was interrupted", tokenEpoch, lsn);
            } finally {
                waiters--;
            }
        }
    }

    public boolean hasWaiters() {
        synchronized (monitor) {
            return waiters > 0;
        }
    }

    public void awaitWork(long maxNanos) {
        if (maxNanos <= 0) {
            return;
        }
        synchronized (monitor) {
            if (!workPending && !stopRequested && phase != Phase.CLOSED) {
                try {
                    TimeUnit.NANOSECONDS.timedWait(monitor, maxNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            workPending = false;
        }
    }

    public void pause(long nanos) {
        long deadline = System.nanoTime() + nanos;
        synchronized (monitor) {
            while (phase != Phase.CLOSED && !stopRequested) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return;
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    public void wake() {
        synchronized (monitor) {
            workPending = true;
            monitor.notifyAll();
        }
    }

    public void offerReference(ChainBody.SnapshotRef reference) {
        synchronized (monitor) {
            if (offeredReference == null || reference.lsn() >= offeredReference.lsn()) {
                offeredReference = reference;
            }
            workPending = true;
            monitor.notifyAll();
        }
    }

    public ChainBody.SnapshotRef takeReference() {
        synchronized (monitor) {
            ChainBody.SnapshotRef taken = offeredReference;
            offeredReference = null;
            return taken;
        }
    }

    public void referenceCommitted(long seq, long lsn, long nowNanos) {
        synchronized (monitor) {
            referenceSeq = seq;
            referenceLsn = lsn;
            referenceNanos = nowNanos;
            referenceError = "";
            monitor.notifyAll();
        }
    }

    public void referenceFailed(String reason) {
        synchronized (monitor) {
            referenceFailures++;
            referenceError = reason;
        }
    }

    public long projectedSeq() {
        return projectedSeq;
    }

    public boolean projectionTerminal() {
        synchronized (monitor) {
            return projectionPhase == ProjectionPhase.FENCED || projectionPhase == ProjectionPhase.CLOSED;
        }
    }

    public void projectionStarting() {
        synchronized (monitor) {
            projectionPhase = ProjectionPhase.STARTING;
        }
    }

    public void projectionResumed(long lsn, long seq, long snapshotId) {
        synchronized (monitor) {
            projectedLsn = lsn;
            projectedSeq = seq;
            icebergSnapshotId = snapshotId;
        }
    }

    public void projectionCommitted(long lsn, long seq, long snapshotId, long rows, long nowNanos) {
        synchronized (monitor) {
            projectedLsn = lsn;
            projectedSeq = seq;
            icebergSnapshotId = snapshotId;
            projectionCommitNanos = nowNanos;
            projectionCommits++;
            projectionRows += rows;
            projectionError = "";
            if (projectionPhase != ProjectionPhase.FENCED && projectionPhase != ProjectionPhase.CLOSED) {
                projectionPhase = ProjectionPhase.ACTIVE;
            }
        }
    }

    public void projectionActive() {
        synchronized (monitor) {
            if (projectionPhase != ProjectionPhase.FENCED && projectionPhase != ProjectionPhase.CLOSED) {
                projectionPhase = ProjectionPhase.ACTIVE;
                projectionError = "";
            }
        }
    }

    public void projectionRetrying(String reason) {
        synchronized (monitor) {
            if (projectionPhase != ProjectionPhase.FENCED && projectionPhase != ProjectionPhase.CLOSED) {
                projectionPhase = ProjectionPhase.RETRYING;
                projectionFailures++;
                projectionError = reason;
            }
        }
    }

    public void projectionFailed(String reason) {
        synchronized (monitor) {
            if (projectionPhase != ProjectionPhase.FENCED && projectionPhase != ProjectionPhase.CLOSED) {
                if (projectionPhase != ProjectionPhase.FAILED) {
                    event("projection failed: " + reason);
                }
                projectionPhase = ProjectionPhase.FAILED;
                projectionFailures++;
                projectionError = reason;
            }
        }
    }

    public void projectionFenced(String reason) {
        synchronized (monitor) {
            if (projectionPhase != ProjectionPhase.CLOSED) {
                projectionPhase = ProjectionPhase.FENCED;
                projectionError = reason;
                event("the projection was fenced: " + reason);
            }
        }
    }

    public void projectionClosed() {
        synchronized (monitor) {
            projectionPhase = ProjectionPhase.CLOSED;
        }
    }

    public void retentionSwept(int chainObjects, int snapshots) {
        synchronized (monitor) {
            sweeps++;
            chainObjectsDeleted += chainObjects;
            snapshotsDeleted += snapshots;
            retentionError = "";
        }
    }

    public void retentionFailed(String reason) {
        synchronized (monitor) {
            retentionFailures++;
            retentionError = reason;
        }
    }

    public void requestStop() {
        synchronized (monitor) {
            stopRequested = true;
            monitor.notifyAll();
        }
    }

    public boolean stopRequested() {
        synchronized (monitor) {
            return stopRequested;
        }
    }

    public Phase phase() {
        synchronized (monitor) {
            return phase;
        }
    }

    public boolean terminal() {
        synchronized (monitor) {
            return phase == Phase.FENCED || phase == Phase.CLOSED;
        }
    }

    public void active() {
        synchronized (monitor) {
            if (phase != Phase.FENCED && phase != Phase.CLOSED) {
                phase = Phase.ACTIVE;
                consecutiveFailures = 0;
                lastError = "";
            }
            monitor.notifyAll();
        }
    }

    public void retrying(String reason) {
        synchronized (monitor) {
            if (phase != Phase.FENCED && phase != Phase.CLOSED) {
                phase = Phase.RETRYING;
                consecutiveFailures++;
                lastError = reason;
            }
            monitor.notifyAll();
        }
    }

    public void failed(String reason) {
        synchronized (monitor) {
            if (phase != Phase.FENCED && phase != Phase.CLOSED) {
                if (phase != Phase.FAILED) {
                    event("shipping failed: " + reason);
                }
                phase = Phase.FAILED;
                consecutiveFailures++;
                lastError = reason;
            }
            monitor.notifyAll();
        }
    }

    public void fenced(String reason) {
        synchronized (monitor) {
            if (phase != Phase.CLOSED) {
                phase = Phase.FENCED;
                lastError = reason;
                event("this writer was fenced: " + reason);
            }
            monitor.notifyAll();
        }
    }

    public void closed() {
        synchronized (monitor) {
            phase = Phase.CLOSED;
            monitor.notifyAll();
        }
    }

    public void shipped(long lsn, long seq, int bytes, long nowNanos) {
        synchronized (monitor) {
            if (lsn > shippedLsn) {
                shippedLsn = lsn;
            }
            chainSeq = seq;
            objectsShipped++;
            bytesShipped += bytes;
            lastCommitNanos = nowNanos;
            monitor.notifyAll();
        }
    }

    public BacklogEvent backlog(long bytes) {
        synchronized (monitor) {
            backlogBytes = bytes;
            BacklogEvent result = BacklogEvent.NONE;
            if (!refusing && bytes >= backlogCapBytes) {
                refusing = true;
                result = BacklogEvent.REFUSING;
                event("the unshipped backlog reached its cap; writes are refused");
            } else if (refusing && bytes < (long) (backlogCapBytes * ShipSettings.RESUME_FRACTION)) {
                refusing = false;
                result = BacklogEvent.RESUMED;
                event("the unshipped backlog dropped below its cap; writes resume");
            }
            if (!warned && bytes >= (long) (backlogCapBytes * ShipSettings.WARN_FRACTION)) {
                warned = true;
                if (result == BacklogEvent.NONE) {
                    result = BacklogEvent.WARNING;
                }
                event("the unshipped backlog is half of its cap");
            } else if (warned && bytes < (long) (backlogCapBytes * ShipSettings.WARN_FRACTION) / 2) {
                warned = false;
            }
            return result;
        }
    }

    public Snapshot snapshot() {
        synchronized (monitor) {
            return new Snapshot(phase, epoch, shippedLsn, chainSeq, backlogBytes, backlogCapBytes,
                    consecutiveFailures, lastError, objectsShipped, bytesShipped, lastCommitNanos, System.nanoTime(),
                    new ReferenceStatus(referenceSeq, referenceLsn, referenceNanos, referenceFailures, referenceError),
                    new RetentionStatus(sweeps, chainObjectsDeleted, snapshotsDeleted, retentionFailures,
                            retentionError),
                    new ProjectionStatus(projectionPhase, projectedLsn, projectedSeq, icebergSnapshotId,
                            projectionCommitNanos, projectionCommits, projectionRows, projectionFailures,
                            projectionError));
        }
    }

    public List<String> drainEvents() {
        synchronized (monitor) {
            List<String> drained = new ArrayList<>(events.size());
            for (Event event : events) {
                drained.add(event.message());
            }
            events.clear();
            return drained;
        }
    }

    public List<Event> recentEvents() {
        synchronized (monitor) {
            return List.copyOf(events);
        }
    }

    private void event(String message) {
        if (events.size() == MAX_EVENTS) {
            events.removeFirst();
        }
        events.addLast(new Event(++eventSeq, message));
    }

    private void failFastIfTerminal(long tokenEpoch, long lsn) {
        switch (phase) {
            case FENCED -> throw new WriterFencedException(lastError);
            case CLOSED -> throw new ShipTimeoutException("the graph was closed before LSN " + lsn
                    + " reached the object store", tokenEpoch, lsn);
            case FAILED -> throw new ShipTimeoutException("shipping has failed and LSN " + lsn
                    + " is not in the object store: " + lastError, tokenEpoch, lsn);
            default -> {
                return;
            }
        }
    }
}
