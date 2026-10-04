package io.nodusdb.kernel.wal;

import io.nodusdb.kernel.GraphKernel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class RecoveryManager {

    public record Recovery(GraphKernel kernel, long framesApplied, long truncatedBytes) {
    }

    record Opened(GraphKernel kernel, DurableStore store, long framesApplied, long truncatedBytes) {
    }

    private RecoveryManager() {
    }

    public static Recovery recover(Path directory, WalConfig config) throws IOException {
        Opened opened = open(directory, config);
        return new Recovery(opened.kernel(), opened.framesApplied(), opened.truncatedBytes());
    }

    static Opened open(Path directory, WalConfig config) throws IOException {
        Files.createDirectories(directory);
        DirectoryLock lock = DirectoryLock.acquire(directory);
        try {
            Files.deleteIfExists(directory.resolve(DurableStore.SNAPSHOT_TEMP));
            Files.deleteIfExists(directory.resolve(DurableStore.LOG_TEMP));
            GraphKernel kernel = new GraphKernel();
            Path snapshot = directory.resolve(DurableStore.SNAPSHOT);
            if (Files.exists(snapshot)) {
                SnapshotFile.load(snapshot, kernel);
            }
            Path log = directory.resolve(DurableStore.LOG);
            if (!Files.exists(log)) {
                Path fresh = directory.resolve(DurableStore.LOG_TEMP);
                WalFormat.writeEmptyLog(fresh);
                Files.move(fresh, log, StandardCopyOption.ATOMIC_MOVE);
            }
            LogReplayer.Result replayed = LogReplayer.replay(log, kernel);
            WalWriter writer = new WalWriter(log, config);
            DurableStore store = new DurableStore(directory, kernel, writer, lock);
            kernel.attachPersistence(store);
            return new Opened(kernel, store, replayed.framesApplied(), replayed.truncatedBytes());
        } catch (IOException | RuntimeException e) {
            releaseQuietly(lock, e);
            throw e;
        }
    }

    private static void releaseQuietly(DirectoryLock lock, Exception cause) {
        try {
            lock.close();
        } catch (IOException e) {
            cause.addSuppressed(e);
        }
    }
}
