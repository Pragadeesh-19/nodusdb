package io.nodusdb.replica;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainTrustException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class AntiRollbackMarker {

    static final String FILE_NAME = "follower.marker";

    private static final long MIN_WRITE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    private static final AntiRollbackMarker DISABLED = new AntiRollbackMarker(null, null);

    private final Path file;
    private MarkerPosition persisted;
    private MarkerPosition pending;
    private long lastWriteNanos;
    private boolean written;

    private AntiRollbackMarker(Path file, MarkerPosition persisted) {
        this.file = file;
        this.persisted = persisted;
    }

    public static AntiRollbackMarker disabled() {
        return DISABLED;
    }

    public static AntiRollbackMarker in(Path stateDirectory) throws IOException {
        Files.createDirectories(stateDirectory);
        Path file = stateDirectory.resolve(FILE_NAME);
        return new AntiRollbackMarker(file, MarkerFile.read(file).orElse(null));
    }

    public synchronized Optional<MarkerPosition> remembered() {
        return Optional.ofNullable(persisted);
    }

    public synchronized void requireBucketMayBeEmpty() {
        if (persisted != null) {
            throw new ChainTrustException("the object store holds no chain, but this follower had already followed "
                    + "object " + persisted.seq() + "; the bucket was rolled back or emptied");
        }
    }

    public synchronized void requireHead(long seq, ChainHash digest) {
        if (persisted == null) {
            return;
        }
        if (seq < persisted.seq()) {
            throw new ChainTrustException("the chain head is object " + seq + " but this follower had already "
                    + "followed object " + persisted.seq() + "; the bucket was rolled back");
        }
        requireObject(seq, digest);
    }

    public synchronized void requireObject(long seq, ChainHash digest) {
        if (persisted != null && seq == persisted.seq() && !digest.equals(persisted.digest())) {
            throw new ChainTrustException("object " + seq + " differs from the object this follower followed at "
                    + "that position; the chain forked");
        }
    }

    public synchronized void observe(MarkerPosition position, long nowNanos) {
        if (file == null || !isNewer(position)) {
            return;
        }
        pending = position;
        if (!written || nowNanos - lastWriteNanos >= MIN_WRITE_INTERVAL_NANOS) {
            persist(nowNanos);
        }
    }

    public synchronized void flush() {
        if (file != null && pending != null && isNewer(pending)) {
            persist(lastWriteNanos);
        }
    }

    private boolean isNewer(MarkerPosition position) {
        return persisted == null || position.seq() > persisted.seq();
    }

    private void persist(long nowNanos) {
        try {
            MarkerFile.write(file, pending);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        persisted = pending;
        pending = null;
        written = true;
        lastWriteNanos = nowNanos;
    }
}
