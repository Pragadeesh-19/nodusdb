package io.nodusdb.authz;

import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.CheckDepthException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.closure.ClosureStats;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.storage.DurableGraph;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReachabilityDurableTest {

    private static final LogConfig SYNC = LogConfig.withSyncMode(SyncMode.SYNC);
    private static final int DEPTH = 40;
    private static final long SETTLE_NANOS = TimeUnit.SECONDS.toNanos(20);

    @TempDir
    Path directory;

    private static long writeChain(GraphKernel kernel) {
        TupleStore store = TupleStore.open(kernel);
        store.applySchema(SchemaFixtures.DOCUMENTS);
        TupleTransaction chain = new TupleTransaction();
        for (int i = 0; i < DEPTH; i++) {
            chain.add("group:g" + i, "member", "group:g" + (i + 1), "member");
        }
        chain.add("group:g" + DEPTH, "member", "user:alice");
        store.write(chain);
        assertTrue(store.check("group:g0", "member", "user:alice"));
        return kernel.closureStats().entries();
    }

    private static boolean checkAfterSettling(TupleStore store) {
        long deadline = System.nanoTime() + SETTLE_NANOS;
        try {
            store.check("group:g0", "member", "user:alice");
        } catch (CheckDepthException stillBuilding) {
            assertTrue(store.kernel().closureStats().building());
        }
        while (store.kernel().closureStats().building()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the index never settled: " + store.kernel().closureStats());
            }
            Thread.onSpinWait();
        }
        return store.check("group:g0", "member", "user:alice");
    }

    @Test
    void aSnapshotLoadRebuildsTheIndexInTheBackground() throws IOException {
        GraphKernel first = DurableGraph.open(directory, SYNC).kernel();
        long entries = writeChain(first);
        first.checkpoint();
        first.close();

        GraphKernel second = DurableGraph.open(directory, SYNC).kernel();
        try {
            TupleStore reopened = TupleStore.open(second);
            assertTrue(second.closureStats().fullPasses() >= 1);

            assertTrue(checkAfterSettling(reopened));
            ClosureStats stats = second.closureStats();
            assertEquals(entries, stats.entries());
            assertEquals(0, stats.dirtyNodes());
            assertFalse(reopened.check("group:g0", "member", "user:bob"));
        } finally {
            second.close();
        }
    }

    @Test
    void replayingTheLogWithoutASnapshotBuildsTheSameIndex() throws IOException {
        GraphKernel first = DurableGraph.open(directory, SYNC).kernel();
        long entries = writeChain(first);
        first.close();

        GraphKernel second = DurableGraph.open(directory, SYNC).kernel();
        try {
            TupleStore reopened = TupleStore.open(second);

            assertTrue(checkAfterSettling(reopened));
            assertEquals(entries, second.closureStats().entries());
        } finally {
            second.close();
        }
    }
}
