package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainCursor;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainVerifier;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.io.LogFileSystem;
import io.nodusdb.objectstore.ListPage;
import io.nodusdb.objectstore.ObjectStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ChainAudit {

    record Result(int objects, long lastLsn, long snapshotLsn, List<Long> epochs) {
    }

    private static final int NO_LIMIT = 1 << 30;

    private ChainAudit() {
    }

    static Result verify(ObjectStore store, LogFileSystem disk, long expectedLastLsn) {
        List<String> keys = chainKeys(store);
        assertTrue(!keys.isEmpty(), "the chain is empty");
        ChainVerifier verifier = new ChainVerifier(ChainBuilder.keyring());
        ChainCursor cursor = ChainCursor.beforeFirst();
        ByteArrayOutputStream shipped = new ByteArrayOutputStream();
        List<Long> epochs = new ArrayList<>();
        long snapshotLsn = -1;
        for (int i = 0; i < keys.size(); i++) {
            assertEquals(ChainLayout.chainKey(i + 1L), keys.get(i), "the chain has a gap or a stray object");
            ChainObject object = ChainCodec.decode(store.get(keys.get(i)).orElseThrow());
            verifier.verify(object);
            cursor.accept(object);
            if (epochs.isEmpty() || epochs.get(epochs.size() - 1) != object.epoch()) {
                epochs.add(object.epoch());
            }
            if (object.body() instanceof ChainBody.SnapshotRef reference && snapshotLsn < 0) {
                assertEquals(ChainKind.SNAPSHOT_REF, object.header().kind());
                snapshotLsn = reference.lsn();
            }
            if (object.body() instanceof ChainBody.Records records) {
                shipped.writeBytes(records.records());
            }
        }
        assertEquals(expectedLastLsn, cursor.lastLsn(), "the chain does not end at the log's last LSN");
        if (disk != null) {
            assertArrayEquals(logRecords(disk, snapshotLsn, expectedLastLsn), shipped.toByteArray(),
                    "the shipped records differ from the log");
        }
        return new Result(keys.size(), cursor.lastLsn(), snapshotLsn, List.copyOf(epochs));
    }

    static List<String> chainKeys(ObjectStore store) {
        List<String> keys = new ArrayList<>();
        String after = "";
        ListPage page;
        do {
            page = store.list(ChainLayout.CHAIN_PREFIX, after, 1000);
            page.entries().forEach(entry -> keys.add(entry.key()));
            after = page.lastKey();
        } while (page.truncated());
        return keys;
    }

    private static byte[] logRecords(LogFileSystem disk, long afterLsn, long lastLsn) {
        if (lastLsn <= afterLsn) {
            return new byte[0];
        }
        try (LogTailReader reader = new LogTailReader(disk, afterLsn)) {
            LogTailReader.Batch batch = reader.read(lastLsn, NO_LIMIT);
            assertEquals(lastLsn, batch.lsnLast());
            return batch.records();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
