package io.nodusdb.replica;

import io.nodusdb.chain.Keyring;
import io.nodusdb.config.StoreFactory;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainFetch.Trust;
import io.nodusdb.storage.FileTrees;
import io.nodusdb.storage.GraphInstaller;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class Restore {

    public record Restored(long appliedLsn, long epoch, long snapshotLsn) {
    }

    private static final String SCRATCH_SUFFIX = ".restore-scratch";

    private final ObjectStore store;
    private final Keyring keyring;
    private final int downloadParallelism;

    public Restore(ObjectStore store, Keyring keyring, int downloadParallelism) {
        this.store = Objects.requireNonNull(store, "store");
        this.keyring = Objects.requireNonNull(keyring, "keyring");
        this.downloadParallelism = downloadParallelism;
    }

    public static Restored restore(FollowerConfig config, Path target) throws IOException {
        Objects.requireNonNull(config, "config");
        Keyring keyring = TrustedKeys.load(config.trust());
        ObjectStore store = StoreFactory.open(config.storage()).store();
        try {
            return new Restore(store, keyring, config.follow().downloadParallelism()).restoreTo(target);
        } finally {
            store.close();
        }
    }

    public Restored restoreTo(Path target) throws IOException {
        Objects.requireNonNull(target, "target");
        Path destination = target.toAbsolutePath();
        GraphInstaller.requireVacant(destination);
        Path scratch = destination.resolveSibling(destination.getFileName() + SCRATCH_SUFFIX);
        FileTrees.deleteRecursively(scratch);
        Files.createDirectories(scratch);
        try {
            return replayInto(destination, scratch);
        } finally {
            FileTrees.deleteRecursively(scratch);
        }
    }

    private Restored replayInto(Path destination, Path scratch) throws IOException {
        ChainFetch fetch = new ChainFetch(store, keyring, Trust.REQUIRED);
        ChainReplay replay = new ChainReplay(store, fetch, new SnapshotDownloader(store, downloadParallelism),
                scratch);
        KernelReplicaSink sink = new KernelReplicaSink(GraphKernel.NO_MEMORY_LIMIT);
        ReplayPosition position = replay.bootstrap(sink).orElseThrow(() -> new IllegalStateException(
                "the object store holds no snapshot with the chain after it to restore from"));
        replay.catchUp(position, sink);
        GraphKernel kernel = sink.kernel();
        GraphInstaller.install(kernel, sink.loaded().salt(), destination);
        return new Restored(kernel.appliedLsn(), kernel.epoch(), sink.loaded().lsn());
    }
}
