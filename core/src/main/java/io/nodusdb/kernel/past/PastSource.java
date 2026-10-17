package io.nodusdb.kernel.past;

import io.nodusdb.error.UnsupportedFeatureException;

import java.time.Duration;

public interface PastSource {

    PastSource NONE = new PastSource() {
        @Override
        public PastView open(PastTarget target, long maxMemoryBytes, Duration wait) {
            throw new UnsupportedFeatureException("as_of rebuilds the past from the bucket, and this graph does not "
                    + "ship; configure shipping (Shipping.directory serves one machine)");
        }

        @Override
        public PoolReport pool() {
            return PoolReport.NONE;
        }
    };

    record PoolReport(boolean configured, int openViews, long usedBytes, long limitBytes) {

        public static final PoolReport NONE = new PoolReport(false, 0, 0, 0);
    }

    PastView open(PastTarget target, long maxMemoryBytes, Duration wait);

    PoolReport pool();
}
