package io.nodusdb.replica;

import io.nodusdb.ship.EventLog;

import java.util.List;
import java.util.concurrent.TimeUnit;

public final class FollowerState {

    public enum Phase {
        BOOTSTRAPPING, CATCHING_UP, CURRENT, LAGGING, STALLED, CLOSED
    }

    public record Snapshot(Phase phase, long epoch, long appliedLsn, long chainSeq, long snapshotLsn,
                           long objectsApplied, long bytesApplied, long bootstraps, long consecutiveFailures,
                           String lastError, String stallReason, long nanosSinceConfirmed) {
    }

    public static final long NEVER_CONFIRMED = -1;

    private final Object monitor = new Object();
    private final EventLog events = new EventLog();
    private Phase phase = Phase.BOOTSTRAPPING;
    private long epoch;
    private long appliedLsn;
    private long chainSeq;
    private long snapshotLsn;
    private long objectsApplied;
    private long bytesApplied;
    private long bootstraps;
    private long consecutiveFailures;
    private String lastError = "";
    private String stallReason = "";
    private boolean confirmed;
    private long confirmedNanos;
    private boolean waitingLogged;
    private boolean stopRequested;
    private long version;

    public Phase phase() {
        synchronized (monitor) {
            return phase;
        }
    }

    public boolean terminal() {
        synchronized (monitor) {
            return isTerminal();
        }
    }

    public Snapshot snapshot(long nowNanos) {
        synchronized (monitor) {
            long since = confirmed ? nowNanos - confirmedNanos : NEVER_CONFIRMED;
            return new Snapshot(phase, epoch, appliedLsn, chainSeq, snapshotLsn, objectsApplied, bytesApplied,
                    bootstraps, consecutiveFailures, lastError, stallReason, since);
        }
    }

    public void bootstrapped(long snapshot, long lsn, long seq, long newEpoch) {
        synchronized (monitor) {
            if (isTerminal()) {
                return;
            }
            boolean wasLagging = phase == Phase.LAGGING;
            moveTo(seq, lsn, newEpoch);
            snapshotLsn = snapshot;
            bootstraps++;
            phase = Phase.CATCHING_UP;
            settle(wasLagging);
            changed();
        }
    }

    public void applied(long seq, long lsn, long newEpoch, int bytes) {
        synchronized (monitor) {
            if (isTerminal()) {
                return;
            }
            boolean wasLagging = phase == Phase.LAGGING;
            moveTo(seq, lsn, newEpoch);
            objectsApplied++;
            bytesApplied += bytes;
            phase = Phase.CATCHING_UP;
            settle(wasLagging);
            changed();
        }
    }

    public void current(long nowNanos) {
        synchronized (monitor) {
            if (isTerminal()) {
                return;
            }
            boolean wasLagging = phase == Phase.LAGGING;
            phase = Phase.CURRENT;
            confirmed = true;
            confirmedNanos = nowNanos;
            settle(wasLagging);
            changed();
        }
    }

    public void note(String message) {
        events.add(message);
    }

    public long version() {
        synchronized (monitor) {
            return version;
        }
    }

    public long awaitChange(long knownVersion, long nanos) {
        long deadline = System.nanoTime() + nanos;
        synchronized (monitor) {
            while (version == knownVersion) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return version;
        }
    }

    public void waitingForSnapshot() {
        synchronized (monitor) {
            if (!waitingLogged && !isTerminal()) {
                waitingLogged = true;
                events.add("the object store holds no usable snapshot yet; waiting for the writer to ship one");
            }
        }
    }

    public void lagging(String reason) {
        synchronized (monitor) {
            if (isTerminal()) {
                return;
            }
            consecutiveFailures++;
            lastError = reason;
            if (phase != Phase.BOOTSTRAPPING && phase != Phase.LAGGING) {
                phase = Phase.LAGGING;
                events.add("falling behind: " + reason);
            }
            changed();
        }
    }

    public void stalled(String reason) {
        synchronized (monitor) {
            if (isTerminal()) {
                return;
            }
            phase = Phase.STALLED;
            stallReason = reason;
            events.add("stalled: " + reason);
            changed();
        }
    }

    public void closed() {
        synchronized (monitor) {
            phase = Phase.CLOSED;
            changed();
        }
    }

    public void requestStop() {
        synchronized (monitor) {
            stopRequested = true;
            changed();
        }
    }

    public boolean stopRequested() {
        synchronized (monitor) {
            return stopRequested;
        }
    }

    public void pause(long nanos) {
        long deadline = System.nanoTime() + nanos;
        synchronized (monitor) {
            while (!stopRequested && phase != Phase.CLOSED) {
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

    public List<String> drainEvents() {
        return events.drain();
    }

    public List<EventLog.Event> recentEvents() {
        return events.recent();
    }

    private boolean isTerminal() {
        return phase == Phase.STALLED || phase == Phase.CLOSED;
    }

    private void changed() {
        version++;
        monitor.notifyAll();
    }

    private void moveTo(long seq, long lsn, long newEpoch) {
        if (seq < chainSeq || lsn < appliedLsn || newEpoch < epoch) {
            throw new IllegalArgumentException("a follower position never moves back: (seq " + seq + ", lsn " + lsn
                    + ", epoch " + newEpoch + ") after (seq " + chainSeq + ", lsn " + appliedLsn + ", epoch "
                    + epoch + ")");
        }
        chainSeq = seq;
        appliedLsn = lsn;
        epoch = newEpoch;
    }

    private void settle(boolean wasLagging) {
        if (wasLagging) {
            events.add("recovered after " + consecutiveFailures + " failed attempts: " + lastError);
        }
        consecutiveFailures = 0;
        lastError = "";
    }
}
