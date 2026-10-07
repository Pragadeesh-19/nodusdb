package io.nodusdb.storage.snapshot;

public record SnapshotMeta(long lsn, long epoch, long lastCommitMicros, byte[] salt) {

    public SnapshotMeta {
        if (lsn < 0 || epoch < 0 || lastCommitMicros < 0) {
            throw new IllegalArgumentException("snapshot positions must not be negative");
        }
        if (salt.length != SnapshotFormat.SALT_BYTES) {
            throw new IllegalArgumentException("the salt must be " + SnapshotFormat.SALT_BYTES + " bytes");
        }
        salt = salt.clone();
    }

    @Override
    public byte[] salt() {
        return salt.clone();
    }
}
