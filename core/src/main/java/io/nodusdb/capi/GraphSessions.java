package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.StringInterner;
import io.nodusdb.kernel.wal.SymbolLog;
import io.nodusdb.kernel.wal.WalConfig;

import java.io.IOException;
import java.nio.file.Path;

public final class GraphSessions {

    private final HandleTable<GraphSession> table = new HandleTable<>();

    public long open() {
        return table.open(new GraphSession(new GraphKernel()));
    }

    public long openDurable(Path directory, WalConfig config) throws IOException {
        GraphKernel kernel = GraphKernel.open(directory, config);
        SymbolLog symbols = null;
        try {
            StringInterner strings = new StringInterner();
            symbols = SymbolLog.open(directory.resolve(SymbolLog.FILE), strings);
            return table.open(new GraphSession(kernel, strings, symbols));
        } catch (IOException | RuntimeException e) {
            releaseQuietly(symbols, kernel, e);
            throw e;
        }
    }

    private static void releaseQuietly(SymbolLog symbols, GraphKernel kernel, Exception cause) {
        try {
            if (symbols != null) {
                symbols.close();
            }
        } catch (IOException e) {
            cause.addSuppressed(e);
        } finally {
            kernel.close();
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
