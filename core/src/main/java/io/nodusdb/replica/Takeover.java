package io.nodusdb.replica;

import io.nodusdb.error.WriterFencedException;
import io.nodusdb.storage.FileTrees;
import io.nodusdb.storage.GraphInstaller;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public final class Takeover<W> {

    public interface Precheck {
        void verify() throws IOException;
    }

    public interface Claim {
        long claim() throws IOException;
    }

    public interface RestoreStep {
        Restore.Restored restore(Path directory) throws IOException;
    }

    public interface HeadProbe {
        long headLsn() throws IOException;
    }

    public interface OpenStep<W> {
        W open(Path directory, long claimedEpoch, Restore.Restored restored) throws IOException;
    }

    public record Result<W>(W writer, long claimedEpoch, Restore.Restored restored, int attempts) {
    }

    static final int MAX_ATTEMPTS = 3;

    private final Precheck precheck;
    private final Claim claim;
    private final RestoreStep restore;
    private final HeadProbe head;
    private final OpenStep<W> open;

    public Takeover(Precheck precheck, Claim claim, RestoreStep restore, HeadProbe head, OpenStep<W> open) {
        this.precheck = Objects.requireNonNull(precheck, "precheck");
        this.claim = Objects.requireNonNull(claim, "claim");
        this.restore = Objects.requireNonNull(restore, "restore");
        this.head = Objects.requireNonNull(head, "head");
        this.open = Objects.requireNonNull(open, "open");
    }

    public Result<W> run(Path directory) throws IOException {
        Objects.requireNonNull(directory, "directory");
        GraphInstaller.requireVacant(directory);
        precheck.verify();
        long claimedEpoch = claim.claim();
        for (int attempt = 1; ; attempt++) {
            Restore.Restored restored = restoreOrClean(directory);
            try {
                return new Result<>(open.open(directory, claimedEpoch, restored), claimedEpoch, restored, attempt);
            } catch (WriterFencedException fenced) {
                discard(directory, fenced);
                if (attempt == MAX_ATTEMPTS || head.headLsn() <= restored.appliedLsn()) {
                    throw fenced;
                }
            } catch (IOException | RuntimeException | Error failure) {
                discard(directory, failure);
                throw failure;
            }
        }
    }

    private Restore.Restored restoreOrClean(Path directory) throws IOException {
        try {
            return restore.restore(directory);
        } catch (IOException | RuntimeException failure) {
            discard(directory, failure);
            throw failure;
        }
    }

    private static void discard(Path directory, Throwable cause) {
        try {
            FileTrees.deleteRecursively(directory);
        } catch (IOException cleanup) {
            cause.addSuppressed(cleanup);
        }
    }
}
