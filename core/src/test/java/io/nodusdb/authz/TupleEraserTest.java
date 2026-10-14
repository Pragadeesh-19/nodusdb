package io.nodusdb.authz;

import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Partition;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.symbols.Pseudonym;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TupleEraserTest {

    private final GraphKernel kernel = new GraphKernel();
    private final TupleStore store = TupleStore.open(kernel);

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private int idOf(String name) {
        byte[] bytes = utf8(name);
        return kernel.symbols().lookup(bytes, 0, bytes.length);
    }

    private int tuplesTouching(int node) {
        int total = 0;
        for (Partition partition : Partition.values()) {
            total += kernel.degree(partition, true, node) + kernel.degree(partition, false, node);
        }
        return total;
    }

    private void grantAliceEverywhere() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        store.add("document:readme", "viewer", "user:alice");
        store.add("document:plan", "editor", "user:alice");
        store.add("group:eng", "member", "user:alice");
        store.add("folder:eng", "viewer", "group:eng", "member");
        store.add("document:spec", "parent", "folder:eng");
    }

    @Test
    void erasingASubjectRemovesEveryTupleThatNamesItAndScrubsTheName() {
        grantAliceEverywhere();
        int alice = idOf("user:alice");

        Token token = store.erase("user:alice");

        assertEquals(kernel.appliedLsn(), token.lsn());
        assertEquals(-1, idOf("user:alice"));
        assertTrue(kernel.symbols().isErased(alice));
        assertArrayEquals(Pseudonym.of(kernel.erasureSalt(), utf8("user:alice")), kernel.symbols().resolve(alice));
        assertEquals(0, tuplesTouching(alice));
        assertFalse(store.check("document:readme", "view", "user:alice"));
        assertFalse(store.check("document:spec", "view", "user:alice"));
        assertEquals(1, tuplesTouching(idOf("group:eng")));
        assertEquals(1, kernel.erasedSymbols());
        assertEquals(token.lsn(), kernel.newestEraseLsn());
    }

    @Test
    void erasingAnObjectRemovesItsGrantsAndItsUsersetsOnBothSides() {
        grantAliceEverywhere();
        store.add("group:eng", "member", "user:bob");
        int eng = idOf("group:eng");

        store.erase("group:eng");

        assertEquals(0, tuplesTouching(eng));
        assertFalse(store.check("document:spec", "view", "user:bob"));
        assertTrue(store.check("document:readme", "view", "user:alice"));
        assertEquals(-1, idOf("group:eng"));
    }

    @Test
    void aSelfReferencingTupleIsRemovedOnce() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        store.add("group:loop", "member", "group:loop", "member");
        store.add("group:loop", "member", "user:carol");
        int loop = idOf("group:loop");

        store.erase("group:loop");

        assertEquals(0, tuplesTouching(loop));
        assertTrue(kernel.symbols().isErased(loop));
    }

    @Test
    void aLargeEraseCommitsInChunksAndFinishesWithTheErasure() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        TupleTransaction many = new TupleTransaction();
        for (int i = 0; i < 25; i++) {
            many.add("document:d" + i, "viewer", "user:alice", null);
        }
        store.write(many);
        int alice = idOf("user:alice");
        long before = kernel.appliedLsn();

        Token token = new TupleEraser(kernel, 4).erase("user:alice");

        assertEquals(0, tuplesTouching(alice));
        assertTrue(kernel.symbols().isErased(alice));
        int removals = 25;
        int commits = 7;
        assertEquals(before + removals + commits + 1, token.lsn());
    }

    @Test
    void anInterruptedEraseIsFinishedByErasingAgain() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        store.add("document:a", "viewer", "user:alice");
        store.add("document:b", "viewer", "user:alice");
        store.remove("document:a", "viewer", "user:alice");
        int alice = idOf("user:alice");

        store.erase("user:alice");

        assertEquals(0, tuplesTouching(alice));
        assertTrue(kernel.symbols().isErased(alice));
    }

    @Test
    void erasingAgainOrErasingAnUnknownNameChangesNothing() {
        grantAliceEverywhere();
        store.erase("user:alice");
        long applied = kernel.appliedLsn();

        Token again = store.erase("user:alice");
        Token unknown = store.erase("user:nobody");

        assertEquals(applied, again.lsn());
        assertEquals(applied, unknown.lsn());
        assertEquals(applied, kernel.appliedLsn());
        assertEquals(1, kernel.erasedSymbols());
    }

    @Test
    void theSameNameWrittenLaterIsANewSubjectWithNoGrants() {
        grantAliceEverywhere();
        int erased = idOf("user:alice");
        store.erase("user:alice");

        store.add("document:new", "viewer", "user:alice");

        assertNotEquals(erased, idOf("user:alice"));
        assertTrue(store.check("document:new", "view", "user:alice"));
        assertFalse(store.check("document:readme", "view", "user:alice"));
    }

    @Test
    void aNameUsedByTheSchemaIsRefusedAndNothingChanges() {
        grantAliceEverywhere();
        long applied = kernel.appliedLsn();

        assertThrows(SchemaViolationException.class, () -> store.erase("document"));
        assertThrows(SchemaViolationException.class, () -> store.erase("viewer"));

        assertEquals(applied, kernel.appliedLsn());
        assertEquals(0, kernel.erasedSymbols());
    }

    @Test
    void anIntegerKeyedGraphCannotErase() {
        GraphKernel integers = new GraphKernel();
        integers.claimKeyKind(KeyKind.INTEGER);

        assertThrows(UnsupportedFeatureException.class, () -> TupleStore.open(integers).erase("user:alice"));
    }

    @Test
    void aFollowerCannotErase() {
        TupleStore follower = TupleStore.open(GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT));

        assertThrows(UnsupportedFeatureException.class, () -> follower.erase("user:alice"));
    }

    @Test
    void theSaltOfAnInMemoryGraphIsStableAndDiffersBetweenGraphs() {
        assertArrayEquals(kernel.erasureSalt(), kernel.erasureSalt());
        assertFalse(Arrays.equals(kernel.erasureSalt(), new GraphKernel().erasureSalt()));
    }
}
