package io.nodusdb.replica;

public final class SnapshotUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SnapshotUnavailableException(String message) {
        super(message);
    }
}
