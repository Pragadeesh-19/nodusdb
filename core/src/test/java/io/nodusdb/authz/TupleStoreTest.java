package io.nodusdb.authz;

import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.CheckDepthException;
import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.error.StaleReadException;
import io.nodusdb.error.TokenLostException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.storage.DurableGraph;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TupleStoreTest {

    @TempDir
    Path directory;

    private final TupleStore store = TupleStore.open(new GraphKernel());

    private void loadSchema() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
    }

    @Test
    void aDirectGrantIsVisibleToThePermissionThatIncludesIt() {
        loadSchema();

        store.add("document:readme", "viewer", "user:alice");

        assertTrue(store.check("document:readme", "view", "user:alice"));
        assertTrue(store.check("document:readme", "viewer", "user:alice"));
        assertFalse(store.check("document:readme", "editor", "user:alice"));
        assertFalse(store.check("document:readme", "view", "user:bob"));
    }

    @Test
    void membershipOfAGroupGrantsWhatTheGroupHolds() {
        loadSchema();
        store.add("document:plan", "editor", "group:eng", "member");
        store.add("group:eng", "member", "user:carol");

        assertTrue(store.check("document:plan", "view", "user:carol"));
        assertFalse(store.check("document:plan", "view", "user:dave"));
    }

    @Test
    void aGrantOnAParentFolderReachesItsDocuments() {
        loadSchema();
        store.add("document:readme", "parent", "folder:eng");
        store.add("folder:eng", "parent", "folder:root");
        store.add("folder:root", "viewer", "group:staff", "member");
        store.add("group:staff", "member", "group:interns", "member");
        store.add("group:interns", "member", "user:erin");

        assertTrue(store.check("document:readme", "view", "user:erin"));
        assertTrue(store.check("folder:eng", "view", "user:erin"));
        assertFalse(store.check("document:other", "view", "user:erin"));
    }

    @Test
    void removingTheGrantRemovesTheAccess() {
        loadSchema();
        store.add("document:readme", "viewer", "user:alice");

        store.remove("document:readme", "viewer", "user:alice");

        assertFalse(store.check("document:readme", "view", "user:alice"));
    }

    @Test
    void groupsThatContainEachOtherDoNotLoopForever() {
        loadSchema();
        store.add("group:a", "member", "group:b", "member");
        store.add("group:b", "member", "group:a", "member");
        store.add("document:cyclic", "viewer", "group:a", "member");
        store.add("group:b", "member", "user:frank");

        assertTrue(store.check("document:cyclic", "view", "user:frank"));
        assertFalse(store.check("document:cyclic", "view", "user:grace"));
    }

    @Test
    void aTransactionAppliesAllItsTuplesOrNone() {
        loadSchema();
        TupleTransaction valid = new TupleTransaction()
                .add("document:one", "viewer", "user:alice")
                .add("document:two", "viewer", "user:alice");
        TupleTransaction invalid = new TupleTransaction()
                .add("document:three", "viewer", "user:alice")
                .add("document:four", "nothing", "user:alice");
        int symbolsBefore = store.kernel().symbols().size();

        store.write(valid);
        int symbolsAfterValid = store.kernel().symbols().size();

        assertThrows(SchemaViolationException.class, () -> store.write(invalid));
        assertEquals(symbolsAfterValid, store.kernel().symbols().size());
        assertTrue(symbolsAfterValid > symbolsBefore);
        assertTrue(store.check("document:one", "view", "user:alice"));
        assertTrue(store.check("document:two", "view", "user:alice"));
        assertFalse(store.check("document:three", "view", "user:alice"));
    }

    @Test
    void aTransactionIsOneCommitWithOneToken() {
        loadSchema();
        TupleTransaction transaction = new TupleTransaction()
                .add("document:one", "viewer", "user:alice")
                .add("document:two", "viewer", "user:bob")
                .add("document:three", "viewer", "user:carol");

        Token token = store.write(transaction);

        assertEquals(store.token(), token);
        assertEquals(store.kernel().epoch(), token.epoch());
    }

    @Test
    void tuplesMustFollowTheSchema() {
        loadSchema();

        assertThrows(SchemaViolationException.class, () -> store.add("document:x", "view", "user:alice"));
        assertThrows(SchemaViolationException.class, () -> store.add("document:x", "viewer", "document:y"));
        assertThrows(SchemaViolationException.class, () -> store.add("document:x", "viewer", "group:g"));
        assertThrows(SchemaViolationException.class, () -> store.add("widget:x", "viewer", "user:alice"));
        assertThrows(SchemaViolationException.class, () -> store.add("document", "viewer", "user:alice"));
        assertThrows(SchemaViolationException.class, () -> store.add("document:x", "viewer", "user:alice", "member"));
        assertThrows(SchemaViolationException.class, () -> store.add("document:x", "nothing", "user:alice"));
    }

    @Test
    void nothingCanBeWrittenBeforeASchemaExists() {
        assertThrows(SchemaViolationException.class, () -> store.add("document:x", "viewer", "user:alice"));
        assertThrows(SchemaViolationException.class, () -> store.check("document:x", "view", "user:alice"));
    }

    @Test
    void checkingAnUnknownRelationOrTypeIsAnError() {
        loadSchema();

        assertThrows(SchemaViolationException.class, () -> store.check("document:x", "nothing", "user:alice"));
        assertThrows(SchemaViolationException.class, () -> store.check("widget:x", "view", "user:alice"));
        assertThrows(SchemaViolationException.class, () -> store.check("document:x", "view", "widget:y"));
    }

    @Test
    void anObjectThatWasNeverWrittenHasNoAccess() {
        loadSchema();
        store.add("document:readme", "viewer", "user:alice");

        assertFalse(store.check("document:unknown", "view", "user:alice"));
        assertFalse(store.check("document:readme", "view", "user:unknown"));
    }

    @Test
    void removingATupleThatNeverExistedChangesNothing() {
        loadSchema();
        Token before = store.token();

        Token after = store.remove("document:ghost", "viewer", "user:nobody");

        assertEquals(before, after);
    }

    @Test
    void writingTheSameTupleAgainReturnsTheCurrentToken() {
        loadSchema();
        store.add("document:readme", "viewer", "user:alice");
        Token first = store.token();

        assertEquals(first, store.add("document:readme", "viewer", "user:alice"));
    }

    @Test
    void lakeDurabilityNeedsShippingToBeConfigured() {
        loadSchema();
        TupleTransaction transaction = new TupleTransaction().add("document:x", "viewer", "user:alice");

        assertThrows(UnsupportedFeatureException.class, () -> store.write(transaction, Durability.LAKE));
        assertFalse(store.check("document:x", "view", "user:alice"));
    }

    @Test
    void aTokenFromAWriteCanBeUsedToDemandFreshness() {
        loadSchema();
        Token token = store.add("document:readme", "viewer", "user:alice");

        assertTrue(store.check("document:readme", "view", "user:alice", token));
        assertThrows(StaleReadException.class, () -> store.check("document:readme", "view", "user:alice",
                new Token(token.epoch(), token.lsn() + 100)));
        assertThrows(StaleReadException.class, () -> store.check("document:readme", "view", "user:alice",
                new Token(token.epoch() + 1, 1)));
    }

    @Test
    void theSchemaMovesForwardOneVersionAtATime() {
        loadSchema();
        store.add("document:readme", "viewer", "user:alice");

        store.applySchema(SchemaFixtures.DOCUMENTS.replace("schema 1", "schema 2")
                .replace("type user", "type user\ntype team { relation member: user }"));

        assertEquals(2, store.schemaVersion());
        assertTrue(store.check("document:readme", "view", "user:alice"));
        store.add("team:core", "member", "user:alice");
        assertTrue(store.check("team:core", "member", "user:alice"));
        assertThrows(SchemaViolationException.class, () -> store.applySchema(SchemaFixtures.DOCUMENTS));
    }

    @Test
    void aDeepChainRaisesTheDepthErrorWhenItCannotDecide() {
        TupleStore shallow = TupleStore.open(new GraphKernel(), 5);
        shallow.applySchema(SchemaFixtures.DOCUMENTS);
        for (int i = 0; i < 10; i++) {
            shallow.add("group:g" + i, "member", "group:g" + (i + 1), "member");
        }
        shallow.add("group:g10", "member", "user:deep");
        shallow.add("group:g1", "member", "user:near");
        shallow.add("document:d", "viewer", "group:g0", "member");

        assertTrue(shallow.check("document:d", "view", "user:near"));
        assertThrows(CheckDepthException.class, () -> shallow.check("document:d", "view", "user:deep"));
    }

    @Test
    void aShortAnswerIsReturnedEvenWhenAnotherBranchIsTooDeep() {
        TupleStore shallow = TupleStore.open(new GraphKernel(), 4);
        shallow.applySchema(SchemaFixtures.DOCUMENTS);
        for (int i = 0; i < 10; i++) {
            shallow.add("group:g" + i, "member", "group:g" + (i + 1), "member");
        }
        shallow.add("document:d", "viewer", "group:g0", "member");
        shallow.add("document:d", "editor", "user:quick");

        assertTrue(shallow.check("document:d", "view", "user:quick"));
    }

    @Test
    void aGraphKeyedByIntegersRefusesTypedTuples() {
        GraphKernel kernel = new GraphKernel();
        kernel.claimKeyKind(KeyKind.INTEGER);
        TupleStore integerStore = TupleStore.open(kernel);

        assertThrows(IllegalStateException.class, () -> integerStore.applySchema(SchemaFixtures.DOCUMENTS));
    }

    @Test
    void theStoreAndItsSchemaSurviveARestart() throws IOException {
        LogConfig config = LogConfig.withSyncMode(SyncMode.SYNC);
        GraphKernel first = DurableGraph.open(directory, config).kernel();
        TupleStore written = TupleStore.open(first);
        written.applySchema(SchemaFixtures.DOCUMENTS);
        written.add("document:readme", "parent", "folder:eng");
        written.add("folder:eng", "viewer", "group:staff", "member");
        Token token = written.add("group:staff", "member", "user:alice");
        first.close();

        GraphKernel second = DurableGraph.open(directory, config).kernel();
        try {
            TupleStore reopened = TupleStore.open(second);

            assertEquals(1, reopened.schemaVersion());
            assertTrue(reopened.check("document:readme", "view", "user:alice", token));
            assertFalse(reopened.check("document:readme", "view", "user:bob"));
            assertNotEquals(token.epoch(), reopened.token().epoch());
        } finally {
            second.close();
        }
    }

    @Test
    void aTokenThatWasAcknowledgedButNeverDurableIsReportedLost() throws IOException {
        LogConfig config = LogConfig.withSyncMode(SyncMode.SYNC);
        GraphKernel first = DurableGraph.open(directory, config).kernel();
        TupleStore written = TupleStore.open(first);
        written.applySchema(SchemaFixtures.DOCUMENTS);
        Token durable = written.add("document:readme", "viewer", "user:alice");
        first.close();

        GraphKernel second = DurableGraph.open(directory, config).kernel();
        try {
            TupleStore reopened = TupleStore.open(second);
            Token invented = new Token(durable.epoch(), durable.lsn() + 50);

            assertTrue(reopened.check("document:readme", "view", "user:alice", durable));
            assertThrows(TokenLostException.class,
                    () -> reopened.check("document:readme", "view", "user:alice", invented));
        } finally {
            second.close();
        }
    }
}
