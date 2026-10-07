package io.nodusdb.objectstore;

import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.objectstore.FaultyObjectStore.SimulatedCrash;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FaultyObjectStoreTest {

    private MemoryObjectStore memory;
    private FaultyObjectStore faulty;

    @BeforeEach
    void setUp() {
        memory = new MemoryObjectStore();
        faulty = new FaultyObjectStore(memory);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void withoutRulesEveryCallPassesThrough() {
        assertEquals(PutResult.CREATED, faulty.putIfAbsent("a/b", bytes("x")));
        assertArrayEquals(bytes("x"), faulty.get("a/b").orElseThrow());
        assertEquals(1, faulty.list("a/", "", 10).entries().size());
        faulty.delete("a/b");
        assertFalse(memory.head("a/b").isPresent());
    }

    @Test
    void aFailureBeforeLeavesTheStoreUntouched() {
        faulty.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class, () -> faulty.putIfAbsent("a/b", bytes("x")));

        assertFalse(memory.head("a/b").isPresent());
        assertEquals(PutResult.CREATED, faulty.putIfAbsent("a/b", bytes("x")));
    }

    @Test
    void aFailureAfterAppliesTheWriteAndStillThrows() {
        faulty.failNext(Operation.PUT_IF_ABSENT, Fault.FAIL_AFTER);

        assertThrows(TransientStoreException.class, () -> faulty.putIfAbsent("a/b", bytes("x")));

        assertTrue(memory.head("a/b").isPresent());
        assertEquals(PutResult.ALREADY_EXISTS, faulty.putIfAbsent("a/b", bytes("y")));
    }

    @Test
    void aConflictReportsAConflictWithoutApplyingTheWrite() {
        faulty.failNext(Operation.PUT_IF_ABSENT, Fault.CONFLICT);

        assertEquals(PutResult.CONFLICT, faulty.putIfAbsent("a/b", bytes("x")));

        assertFalse(memory.head("a/b").isPresent());
    }

    @Test
    void aConflictOnAnOperationThatCannotConflictIsAProgrammingError() {
        faulty.failNext(Operation.GET, Fault.CONFLICT);

        assertThrows(IllegalStateException.class, () -> faulty.get("a/b"));
    }

    @Test
    void aRejectionAndExpiredCredentialsAreFatalAndRefreshableErrors() {
        faulty.failNext(Operation.GET, Fault.FATAL);
        faulty.failNext(Operation.HEAD, Fault.EXPIRED);

        FatalStoreException rejected = assertThrows(FatalStoreException.class, () -> faulty.get("a/b"));
        assertThrows(ExpiredCredentialsException.class, () -> faulty.head("a/b"));

        assertEquals(403, rejected.status());
    }

    @Test
    void aCrashBeforeAndAfterDifferInWhetherTheWriteLanded() {
        faulty.failNext(Operation.PUT, Fault.CRASH_BEFORE);
        assertThrows(SimulatedCrash.class, () -> faulty.put("a/one", bytes("x")));
        assertFalse(memory.head("a/one").isPresent());

        faulty.failNext(Operation.PUT, Fault.CRASH_AFTER);
        assertThrows(SimulatedCrash.class, () -> faulty.put("a/two", bytes("x")));
        assertTrue(memory.head("a/two").isPresent());
    }

    @Test
    void aCallOrdinalSelectsOneExactCallAcrossOperations() {
        faulty.failCall(3, Fault.FAIL_BEFORE);

        faulty.put("a/1", bytes("x"));
        faulty.get("a/1");
        assertThrows(TransientStoreException.class, () -> faulty.head("a/1"));
        faulty.head("a/1");

        assertEquals(List.of(Operation.PUT, Operation.GET, Operation.HEAD, Operation.HEAD),
                faulty.calls().stream().map(FaultyObjectStore.Call::operation).toList());
        assertEquals(4, faulty.callCount());
    }

    @Test
    void aRuleFiresOnlyTheRequestedNumberOfTimes() {
        faulty.failNext(Operation.GET, 2, Fault.FAIL_BEFORE);

        assertThrows(TransientStoreException.class, () -> faulty.get("a/1"));
        assertThrows(TransientStoreException.class, () -> faulty.get("a/1"));
        faulty.get("a/1");
        assertEquals(3, faulty.count(Operation.GET));
    }

    @Test
    void aRuleMayMatchOnTheKey() {
        faulty.failWhen(call -> call.key().startsWith("chain/"), 1, Fault.FAIL_BEFORE);

        faulty.put("other/1", bytes("x"));
        assertThrows(TransientStoreException.class, () -> faulty.put("chain/1", bytes("x")));
        faulty.put("chain/1", bytes("x"));
    }

    @Test
    void aStoreThatIgnoresConditionalWritesOverwritesAndClaimsCreation() {
        faulty.ignoreConditionalWrites();

        assertEquals(PutResult.CREATED, faulty.putIfAbsent("a/b", bytes("first")));
        assertEquals(PutResult.CREATED, faulty.putIfAbsent("a/b", bytes("second")));

        assertArrayEquals(bytes("second"), memory.get("a/b").orElseThrow());
    }

    @Test
    void randomFaultsRepeatExactlyForTheSameSeed() {
        List<String> first = outcomes(42);
        List<String> second = outcomes(42);

        assertEquals(first, second);
        assertTrue(first.contains("before"));
        assertTrue(first.contains("after"));
        assertTrue(first.contains("conflict"));
        assertTrue(first.contains("ok"));
        assertFalse(outcomes(43).equals(first));
    }

    private static List<String> outcomes(long seed) {
        FaultyObjectStore store = new FaultyObjectStore(new MemoryObjectStore()).randomFaults(seed, 0.2, 0.2, 0.2);
        List<String> outcomes = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            try {
                PutResult result = store.putIfAbsent("k/" + i, bytes("x"));
                outcomes.add(result == PutResult.CONFLICT ? "conflict" : "ok");
            } catch (TransientStoreException failure) {
                outcomes.add(failure.getMessage().contains("before") ? "before" : "after");
            }
        }
        return outcomes;
    }

    @Test
    void randomFaultsCanBeStopped() {
        faulty.randomFaults(1, 1.0, 0, 0);
        assertThrows(TransientStoreException.class, () -> faulty.get("a/1"));

        faulty.stopRandomFaults();

        faulty.get("a/1");
    }
}
