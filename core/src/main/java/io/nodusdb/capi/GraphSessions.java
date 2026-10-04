package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.wal.WalConfig;

import java.io.IOException;
import java.nio.file.Path;

public final class GraphSessions {

    private final HandleTable<GraphSession> table = new HandleTable<>();

    public long open() {
        return table.open(new GraphSession(new GraphKernel()));
    }

    public long openDurable(Path directory, WalConfig config) throws IOException {
        return table.open(new GraphSession(GraphKernel.open(directory, config)));
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
