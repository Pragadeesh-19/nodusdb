package io.nodusdb.stats;

import io.nodusdb.kernel.changes.ChangeRing;

public record WatchReport(boolean enabled, int bufferChanges, long servesFromLsn, long publishedLsn,
                          int fallbackReaders, long expiredReads) {

    public static final WatchReport NONE = new WatchReport(false, 0, 0, 0, 0, 0);

    public static WatchReport of(ChangeRing ring, int fallbackReaders, long expiredReads) {
        if (ring == null) {
            return new WatchReport(false, 0, 0, 0, fallbackReaders, expiredReads);
        }
        return new WatchReport(true, ring.capacity(), ring.servesFromLsn(), ring.publishedLsn(), fallbackReaders,
                expiredReads);
    }
}
