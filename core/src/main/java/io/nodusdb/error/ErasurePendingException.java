package io.nodusdb.error;

import java.util.List;

public final class ErasurePendingException extends NodusException {

    private static final long serialVersionUID = 1L;

    private final long epoch;
    private final long lsn;
    private final String[] pendingLayers;

    public ErasurePendingException(long epoch, long lsn, List<String> pendingLayers) {
        super(ErrorCode.ERASURE_PENDING, "the erasure at (" + epoch + ", " + lsn + ") is not yet scrubbed from "
                + String.join(", ", pendingLayers));
        this.epoch = epoch;
        this.lsn = lsn;
        this.pendingLayers = pendingLayers.toArray(String[]::new);
    }

    public long epoch() {
        return epoch;
    }

    public long lsn() {
        return lsn;
    }

    public List<String> pendingLayers() {
        return List.of(pendingLayers);
    }
}
