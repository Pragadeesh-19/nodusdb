package io.nodusdb.replica;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.chain.Keyring;
import io.nodusdb.config.StoreFactory;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.storage.FileTrees;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class FollowerRuntime implements AutoCloseable {

    private static final String SCRATCH_DIRECTORY = "scratch";
    private static final String SCRATCH_PREFIX = "nodusdb-follower-";
    private static final String THREAD_NAME = "nodusdb-follower";

    private final GraphKernel kernel;
    private final TupleStore tuples;
    private final FollowerState state;
    private final FollowerConfig.Follow follow;
    private final FollowerRunner runner;
    private final ObjectStore store;
    private final Path scratch;
    private boolean closed;

    private FollowerRuntime(GraphKernel kernel, TupleStore tuples, FollowerState state, FollowerConfig.Follow follow,
                            FollowerRunner runner, ObjectStore store, Path scratch) {
        this.kernel = kernel;
        this.tuples = tuples;
        this.state = state;
        this.follow = follow;
        this.runner = runner;
        this.store = store;
        this.scratch = scratch;
    }

    public static FollowerRuntime start(FollowerConfig config, long maxMemoryBytes) throws IOException {
        Objects.requireNonNull(config, "config");
        Keyring keyring = TrustedKeys.load(config.trust());
        ObjectStore store = StoreFactory.open(config.storage()).store();
        try {
            return start(store, keyring, config.follow(), maxMemoryBytes);
        } catch (IOException | RuntimeException failure) {
            closeQuietly(store, failure);
            throw failure;
        }
    }

    public static FollowerRuntime start(ObjectStore store, Keyring keyring, FollowerConfig.Follow follow,
                                        long maxMemoryBytes) throws IOException {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(keyring, "keyring");
        Objects.requireNonNull(follow, "follow");
        AntiRollbackMarker marker = follow.persistsRollbackMemory()
                ? AntiRollbackMarker.in(follow.stateDirectory()) : AntiRollbackMarker.disabled();
        Path scratch = scratchDirectory(follow);
        KernelReplicaSink sink = new KernelReplicaSink(maxMemoryBytes);
        FollowerState state = new FollowerState();
        FollowerCore core = new FollowerCore(store, keyring, sink, marker, state, follow, scratch,
                System::nanoTime);
        TupleStore tuples = TupleStore.open(sink.kernel());
        FollowerRunner runner = FollowerRunner.start(core, state, marker, THREAD_NAME);
        return new FollowerRuntime(sink.kernel(), tuples, state, follow, runner, store, scratch);
    }

    public GraphKernel kernel() {
        return kernel;
    }

    public TupleStore tuples() {
        return tuples;
    }

    public FollowerState state() {
        return state;
    }

    public FollowerConfig.Follow follow() {
        return follow;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        runner.close();
        kernel.close();
        RuntimeException failure = null;
        try {
            store.close();
        } catch (RuntimeException e) {
            failure = e;
        }
        try {
            FileTrees.deleteRecursively(scratch);
        } catch (IOException e) {
            if (failure == null) {
                failure = new UncheckedIOException(e);
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static Path scratchDirectory(FollowerConfig.Follow follow) throws IOException {
        if (follow.persistsRollbackMemory()) {
            Path scratch = follow.stateDirectory().resolve(SCRATCH_DIRECTORY);
            FileTrees.deleteRecursively(scratch);
            return Files.createDirectories(scratch);
        }
        return Files.createTempDirectory(SCRATCH_PREFIX);
    }

    private static void closeQuietly(ObjectStore store, Exception cause) {
        try {
            store.close();
        } catch (RuntimeException closeFailure) {
            cause.addSuppressed(closeFailure);
        }
    }
}
