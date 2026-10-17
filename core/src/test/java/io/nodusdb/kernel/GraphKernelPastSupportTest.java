package io.nodusdb.kernel;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.kernel.closure.ClosureLimits;
import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.log.record.RecordFixtures;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphKernelPastSupportTest {

    @Test
    void completeClosureReturnsOnlyWhenTheIndexIsCurrent() {
        GraphKernel kernel = new GraphKernel(GraphKernel.NO_MEMORY_LIMIT, ClosureLimits.DEFAULT.withWorkBudget(1));
        TupleStore store = TupleStore.open(kernel);
        store.applySchema(SchemaFixtures.DOCUMENTS);
        TupleTransaction nesting = new TupleTransaction();
        for (int depth = 0; depth < 30; depth++) {
            nesting.add("group:g" + depth, "member", "group:g" + (depth + 1), "member");
            nesting.add("group:g" + depth, "member", "user:u" + depth);
        }
        store.write(nesting);

        kernel.completeClosure();

        assertFalse(kernel.closureStats().building());
        assertEquals(0, kernel.closureStats().dirtyNodes());
        kernel.close();
    }

    @Test
    void aReplicaScrubsASymbolInPlaceOnce() {
        GraphKernel replica = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
        byte[] name = "user:alice".getBytes(StandardCharsets.UTF_8);
        new ReplicatedStream().transaction(batch -> batch.symbol(0, name, 0, name.length)).drain().applyTo(replica);
        byte[] pseudonym = RecordFixtures.utf8("erased:0123456789abcdef");

        assertTrue(replica.scrubSymbol(0, pseudonym));

        assertArrayEquals(pseudonym, replica.symbols().resolve(0));
        assertEquals(-1, replica.symbols().lookup(name, 0, name.length));
        assertFalse(replica.scrubSymbol(0, pseudonym));
        assertFalse(replica.scrubSymbol(7, pseudonym));
        assertFalse(replica.scrubSymbol(-1, pseudonym));
        replica.close();
    }

    @Test
    void onlyAReplicaScrubsASymbol() {
        GraphKernel writer = new GraphKernel();

        assertThrows(IllegalStateException.class, () -> writer.scrubSymbol(0, new byte[23]));
        writer.close();
    }

    @Test
    void aReplicaCanDrawOnAGivenBudget() {
        MemoryBudget pool = MemoryBudget.limitedTo(1L << 30);
        MemoryBudget budget = MemoryBudget.drawingOn(pool, 1L << 29);
        GraphKernel replica = GraphKernel.openReplica(budget);

        new ReplicatedStream().add(1, 2).drain().applyTo(replica);

        assertSame(budget, replica.budget());
        assertTrue(replica.isReplica());
        assertTrue(pool.used() > 0);
        assertEquals(pool.used(), budget.used());
        replica.close();
    }
}
