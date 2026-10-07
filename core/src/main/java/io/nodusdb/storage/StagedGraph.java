package io.nodusdb.storage;

import io.nodusdb.error.UpgradeFailedException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.symbols.SymbolTable;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.RecoveryResult;
import io.nodusdb.log.SegmentedLog;
import io.nodusdb.log.SyncMode;
import io.nodusdb.log.io.DirectoryLogFileSystem;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFormat;
import io.nodusdb.storage.snapshot.SnapshotMeta;
import io.nodusdb.storage.snapshot.SnapshotWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

final class StagedGraph {

    private static final long FIRST_EPOCH = 1L;
    private static final int LOCAL_WRITER_KEY = 0;

    private final GraphKernel recovered;
    private final SegmentedLog log;
    private final RecordBatch batch = new RecordBatch();

    private StagedGraph(GraphKernel recovered, SegmentedLog log) {
        this.recovered = recovered;
        this.log = log;
    }

    static void build(Path staging, GraphKernel recovered, byte[] salt) throws IOException {
        Path logDirectory = staging.resolve(GraphFiles.LOG_DIRECTORY);
        Files.createDirectories(logDirectory);
        RecoveryResult empty = new RecoveryResult(0, 0, List.of(), 0, 0, 0);
        LogConfig config = LogConfig.withSyncMode(SyncMode.SYNC);
        SegmentedLog log = SegmentedLog.open(new DirectoryLogFileSystem(logDirectory), config, empty, FIRST_EPOCH,
                LOCAL_WRITER_KEY, DurableGraph::wallClockMicros);
        try {
            StagedGraph staged = new StagedGraph(recovered, log);
            staged.writeKeysAndSymbols();
            recovered.recordEpoch(FIRST_EPOCH, 1, 0);
            SnapshotMeta meta = new SnapshotMeta(log.lastLsn(), FIRST_EPOCH, log.lastCommitMicros(), salt);
            SnapshotWriter.write(recovered, meta, staging.resolve(GraphFiles.SNAPSHOT));
        } catch (IOException | RuntimeException e) {
            log.abort();
            throw e;
        }
        log.close();
    }

    private void writeKeysAndSymbols() {
        KeyKind kind = recovered.keyKind();
        SymbolTable symbols = recovered.symbols();
        batch.clear();
        if (kind != KeyKind.UNSET) {
            batch.graphConfig(kind.code());
        }
        for (int id = 0; id < symbols.size(); id++) {
            byte[] name = symbols.resolve(id);
            if (overflows(name.length)) {
                flush();
            }
            addSymbol(id, name);
        }
        flush();
    }

    private void addSymbol(int id, byte[] name) {
        try {
            batch.symbol(id, name, 0, name.length);
        } catch (IllegalArgumentException e) {
            throw new UpgradeFailedException("string key " + id + " is too long for the new format: "
                    + name.length + " bytes", e);
        }
    }

    private boolean overflows(int length) {
        long used = batch.size() + (long) RecordFormat.symbolBytes(length) + RecordFormat.COMMIT_BYTES;
        return !batch.isEmpty() && used > RecordFormat.WRITE_LIMIT_BYTES;
    }

    private void flush() {
        if (batch.isEmpty()) {
            return;
        }
        batch.commit();
        log.awaitDurable(log.append(batch));
        batch.clear();
    }
}
