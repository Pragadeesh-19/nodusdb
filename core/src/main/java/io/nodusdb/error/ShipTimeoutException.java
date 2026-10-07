package io.nodusdb.error;

public final class ShipTimeoutException extends NodusException {

    private static final long serialVersionUID = 1L;

    private final long epoch;
    private final long lsn;

    public ShipTimeoutException(String message, long epoch, long lsn) {
        super(ErrorCode.SHIP_TIMEOUT, message);
        this.epoch = epoch;
        this.lsn = lsn;
    }

    public long epoch() {
        return epoch;
    }

    public long lsn() {
        return lsn;
    }
}
