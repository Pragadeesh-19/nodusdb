package io.nodusdb.replica;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.chain.Keyring;
import io.nodusdb.config.ConfigFields;
import io.nodusdb.config.StoreFactory;
import io.nodusdb.history.BucketPast;
import io.nodusdb.kernel.GraphHistory;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.past.PastSource;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.stats.WatchReport;
import io.nodusdb.storage.FileTrees;
import io.nodusdb.watch.ChainRecordSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public final class FollowerRuntime implements AutoCloseable {

    private static final String SCRATCH_DIRECTORY = "scratch";
    private static final String SCRATCH_PREFIX = "nodusdb-follower-";
    private static final String THREAD_NAME = "nodusdb-follower";
    private static final String HISTORY_SCRATCH = "history";

    private final GraphKernel kernel;
    private final TupleStore tuples;
    private final FollowerState state;
    private final FollowerGate gate;
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
        this.gate = new FollowerGate(kernel, state, follow, System::nanoTime);
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
            return start(store, keyring, config.follow(), maxMemoryBytes, config.historyMemoryBytes());
        } catch (IOException | RuntimeException failure) {
            closeQuietly(store, failure);
            throw failure;
        }
    }

    public static FollowerRuntime start(ObjectStore store, Keyring keyring, FollowerConfig.Follow follow,
                                        long maxMemoryBytes) throws IOException {
        return start(store, keyring, follow, maxMemoryBytes, ConfigFields.HISTORY_LIMIT_OF_GRAPH);
    }

    public static FollowerRuntime start(ObjectStore store, Keyring keyring, FollowerConfig.Follow follow,
                                        long maxMemoryBytes, long historyMemoryBytes) throws IOException {
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
        ChainFetch fetch = new ChainFetch(store, keyring, ChainFetch.Trust.REQUIRED);
        BucketPast past = new BucketPast(sink.kernel(), store, fetch, scratch.resolve(HISTORY_SCRATCH),
                follow.downloadParallelism(), ConfigFields.historyPoolLimit(historyMemoryBytes, maxMemoryBytes));
        sink.kernel().attachHistory(new GraphHistory(List.of(new ChainRecordSource(store, fetch)), past));
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

    public FollowerGate gate() {
        return gate;
    }

    public String statsJson(WatchReport watch, PastSource.PoolReport history) {
        return FollowerStats.json(kernel, state, System.nanoTime(), watch, history);
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
