package io.nodusdb.storage.legacy;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class LegacyGraph {

    public static final String SNAPSHOT = "snapshot.bin";
    public static final String LOG = "nodus.wal";
    public static final String SYMBOLS = "symbols.nodus";

    private LegacyGraph() {
    }

    public static void recover(Path directory, GraphKernel kernel) throws IOException {
        Path snapshot = directory.resolve(SNAPSHOT);
        if (Files.isRegularFile(snapshot)) {
            LegacySnapshot.load(snapshot, kernel);
        }
        Path log = directory.resolve(LOG);
        if (Files.isRegularFile(log)) {
            LegacyWal.replay(log, new KernelSink(kernel));
        }
        restoreKeys(directory.resolve(SYMBOLS), kernel);
    }

    private static void restoreKeys(Path symbols, GraphKernel kernel) throws IOException {
        if (!Files.isRegularFile(symbols)) {
            kernel.restoreKeyKind(kernel.hasEdges() ? KeyKind.INTEGER : KeyKind.UNSET);
            return;
        }
        LegacySymbols.Contents contents = LegacySymbols.read(symbols);
        List<byte[]> strings = contents.strings();
        for (int id = 0; id < strings.size(); id++) {
            try {
                kernel.restoreSymbol(id, strings.get(id));
            } catch (IllegalStateException e) {
                throw new IOException("symbol log repeats a string at id " + id + ": " + symbols, e);
            }
        }
        boolean integerKeyed = contents.kind() == KeyKind.UNSET && kernel.hasEdges();
        kernel.restoreKeyKind(integerKeyed ? KeyKind.INTEGER : contents.kind());
    }

    private record KernelSink(GraphKernel kernel) implements LegacyWal.EdgeSink {

        @Override
        public void add(long source, long target) {
            kernel.addEdge(source, target);
        }

        @Override
        public void remove(long source, long target) {
            kernel.removeEdge(source, target);
        }
    }
}
