package io.nodusdb.kernel;

import java.util.Arrays;

public final class EpochHistory {

    public record Tenure(long epoch, long firstLsn, long handoffLsn) {
    }

    private volatile Tenure[] tenures = new Tenure[0];

    public synchronized void record(long epoch, long firstLsn, long handoffLsn) {
        Tenure[] current = tenures;
        if (current.length > 0 && current[current.length - 1].epoch() >= epoch) {
            throw new IllegalStateException("epoch " + epoch + " does not follow epoch "
                    + current[current.length - 1].epoch());
        }
        Tenure[] grown = Arrays.copyOf(current, current.length + 1);
        grown[current.length] = new Tenure(epoch, firstLsn, handoffLsn);
        tenures = grown;
    }

    public int size() {
        return tenures.length;
    }

    public Tenure tenureAt(int index) {
        return tenures[index];
    }

    public long latestEpoch() {
        Tenure[] snapshot = tenures;
        return snapshot.length == 0 ? 0 : snapshot[snapshot.length - 1].epoch();
    }

    public boolean isLost(long epoch, long lsn) {
        for (Tenure tenure : tenures) {
            if (tenure.epoch() > epoch) {
                return lsn > tenure.handoffLsn();
            }
        }
        return false;
    }
}
