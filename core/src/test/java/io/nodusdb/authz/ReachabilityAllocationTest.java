package io.nodusdb.authz;

import io.nodusdb.authz.schema.CompiledSchema;
import io.nodusdb.authz.schema.SchemaCompiler;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.symbols.SymbolTable;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReachabilityAllocationTest {

    private static final int GROUPS = 200;
    private static final int WARMUP_ROUNDS = 50_000;
    private static final int MEASURED_ROUNDS = 200_000;
    private static final int MAX_MEASURED_WINDOWS = 5;

    @Test
    void aCheckAnsweredThroughACleanIndexAllocatesNothing() {
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(bean.isThreadAllocatedMemorySupported());
        GraphKernel kernel = new GraphKernel();
        TupleStore store = TupleStore.open(kernel);
        store.applySchema(SchemaFixtures.DOCUMENTS);
        TupleTransaction chain = new TupleTransaction();
        for (int i = 0; i < GROUPS; i++) {
            chain.add("group:g" + i, "member", "group:g" + (i + 1), "member");
        }
        chain.add("group:g" + GROUPS, "member", "user:alice");
        chain.add("group:other", "member", "user:bob");
        store.write(chain);
        assertEquals(0, kernel.closureStats().dirtyNodes());

        CompiledSchema model = SchemaCompiler.rebuild(kernel.catalog(), kernel.symbols());
        int definition = model.definition(model.typeIndex("group"), "member");
        int top = node(kernel.symbols(), "group:g0");
        int alice = node(kernel.symbols(), "user:alice");
        int bob = node(kernel.symbols(), "user:bob");
        CheckEvaluator evaluator = new CheckEvaluator(kernel, new NodeTypes(kernel.symbols()), 32);
        assertTrue(evaluator.check(model, top, definition, alice));
        assertFalse(evaluator.check(model, top, definition, bob));

        long threadId = Thread.currentThread().threadId();
        run(evaluator, model, definition, top, alice, bob, WARMUP_ROUNDS);
        long steadyState = Long.MAX_VALUE;
        for (int window = 0; window < MAX_MEASURED_WINDOWS && steadyState != 0; window++) {
            long before = bean.getThreadAllocatedBytes(threadId);
            run(evaluator, model, definition, top, alice, bob, MEASURED_ROUNDS);
            steadyState = bean.getThreadAllocatedBytes(threadId) - before;
        }

        assertEquals(0L, steadyState, "bytes allocated by indexed checks in a primed window");
        kernel.close();
    }

    private static long run(CheckEvaluator evaluator, CompiledSchema model, int definition, int top, int alice,
                            int bob, int rounds) {
        long granted = 0;
        for (int round = 0; round < rounds; round++) {
            granted += evaluator.check(model, top, definition, alice) ? 1 : 0;
            granted += evaluator.check(model, top, definition, bob) ? 1 : 0;
        }
        return granted;
    }

    private static int node(SymbolTable symbols, String name) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return symbols.lookup(bytes, 0, bytes.length);
    }
}
