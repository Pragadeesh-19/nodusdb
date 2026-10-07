package io.nodusdb.log;

import io.nodusdb.error.UnsupportedFeatureException;

public interface ShipWatermark {

    ShipWatermark NONE = new ShipWatermark() {

        @Override
        public boolean configured() {
            return false;
        }

        @Override
        public long shippedLsn() {
            return -1;
        }

        @Override
        public void awaitShipped(long epoch, long lsn, long timeoutNanos) {
            throw new UnsupportedFeatureException("shipping is not configured for this graph");
        }
    };

    boolean configured();

    long shippedLsn();

    void awaitShipped(long epoch, long lsn, long timeoutNanos);
}
