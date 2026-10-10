package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.log.LogConfig;
import io.nodusdb.replica.FollowerConfig;
import io.nodusdb.replica.FollowerRuntime;
import io.nodusdb.ship.ShippingConfig;
import io.nodusdb.storage.DurableGraph;

import java.io.IOException;
import java.nio.file.Path;

public final class GraphSessions {

    private final HandleTable<GraphSession> table = new HandleTable<>();

    public long open() {
        return open(GraphKernel.NO_MEMORY_LIMIT);
    }

    public long open(long maxMemoryBytes) {
        return table.open(new GraphSession(new GraphKernel(maxMemoryBytes)));
    }

    public long openDurable(Path directory, LogConfig config) throws IOException {
        return openDurable(directory, config, GraphKernel.NO_MEMORY_LIMIT);
    }

    public long openDurable(Path directory, LogConfig config, long maxMemoryBytes) throws IOException {
        return openDurable(directory, config, maxMemoryBytes, null);
    }

    public long openDurable(Path directory, LogConfig config, long maxMemoryBytes, ShippingConfig shipping)
            throws IOException {
        GraphKernel kernel = DurableGraph.open(directory, config, maxMemoryBytes, shipping).kernel();
        return table.open(new GraphSession(kernel));
    }

    public long adopt(GraphKernel kernel) {
        return table.open(new GraphSession(kernel));
    }

    public long openFollower(FollowerConfig config, long maxMemoryBytes) throws IOException {
        FollowerRuntime runtime = FollowerRuntime.start(config, maxMemoryBytes);
        try {
            return table.open(new GraphSession(runtime));
        } catch (RuntimeException failure) {
            runtime.close();
            throw failure;
        }
    }

    public GraphSession get(long handle) {
        return table.get(handle);
    }

    public void close(long handle) {
        GraphSession session = table.get(handle);
        session.close();
        table.close(handle);
    }
}
