package io.nodusdb.capi;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.ErrorCode;
import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Token;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionAuthzTest {

    private final GraphSession session = new GraphSession(new GraphKernel());

    @Test
    void aSessionAppliesASchemaWritesTuplesAndChecks() {
        Token schema = session.applySchema(SchemaFixtures.DOCUMENTS);
        Token write = session.writeTuples(new TupleTransaction().add("document:readme", "viewer", "user:alice"),
                Durability.LOCAL);

        assertEquals(1, session.schemaVersion());
        assertTrue(write.lsn() > schema.lsn());
        assertTrue(session.check("document:readme", "view", "user:alice", write));
        assertFalse(session.check("document:readme", "view", "user:bob", null));
        assertEquals(session.token(), write);
        assertEquals(KeyKind.STRING, session.keyKind());
    }

    @Test
    void aSchemaViolationKeepsItsCodeThroughTheFailureMapping() {
        session.applySchema(SchemaFixtures.DOCUMENTS);

        SchemaViolationException failure = assertThrows(SchemaViolationException.class,
                () -> session.writeTuples(new TupleTransaction().add("document:x", "nothing", "user:alice"),
                        Durability.LOCAL));

        assertEquals(ErrorCode.SCHEMA_VIOLATION.value(), Failures.codeOf(failure));
        assertEquals(failure.getMessage(), new String(Failures.lastMessage(), StandardCharsets.UTF_8));
    }

    @Test
    void lakeDurabilityMapsToTheUnsupportedCode() {
        session.applySchema(SchemaFixtures.DOCUMENTS);

        UnsupportedFeatureException failure = assertThrows(UnsupportedFeatureException.class,
                () -> session.writeTuples(new TupleTransaction().add("document:x", "viewer", "user:alice"),
                        Durability.LAKE));

        assertEquals(ErrorCode.UNSUPPORTED.value(), Failures.codeOf(failure));
    }

    @Test
    void foreignFailuresMapToTheGenericCode() {
        assertEquals(ErrorCode.FAILURE.value(), Failures.codeOf(new IllegalStateException("boom")));
        assertEquals("boom", new String(Failures.lastMessage(), StandardCharsets.UTF_8));
        assertEquals(ErrorCode.FAILURE.value(), Failures.codeOf(new IllegalStateException()));
        assertEquals("IllegalStateException", new String(Failures.lastMessage(), StandardCharsets.UTF_8));
    }

    @Test
    void theWireFormatDecodesOperationsInOrder() {
        byte[] blob = "document:readmevieweruser:alice".getBytes(StandardCharsets.UTF_8);
        TupleTransaction plain = TupleWire.decode(blob, new int[] {15, 6, 10, -1}, new int[] {1}, 1);

        assertEquals(1, plain.size());
        assertTrue(plain.operations().get(0).add());
        assertEquals("document:readme", plain.operations().get(0).object());
        assertEquals("viewer", plain.operations().get(0).relation());
        assertEquals("user:alice", plain.operations().get(0).subject());
        assertNull(plain.operations().get(0).subjectRelation());
    }

    @Test
    void theWireFormatCarriesUsersetsAndRemovals() {
        byte[] blob = "doc:aeditorgroup:gmemberdoc:aeditoruser:u".getBytes(StandardCharsets.UTF_8);
        int[] lengths = {5, 6, 7, 6, 5, 6, 6, -1};
        int[] kinds = {1, 0};

        TupleTransaction transaction = TupleWire.decode(blob, lengths, kinds, 2);

        assertEquals("group:g", transaction.operations().get(0).subject());
        assertEquals("member", transaction.operations().get(0).subjectRelation());
        assertFalse(transaction.operations().get(1).add());
        assertEquals("user:u", transaction.operations().get(1).subject());
        assertNull(transaction.operations().get(1).subjectRelation());
    }

    @Test
    void theWireFormatRejectsInconsistentInput() {
        byte[] blob = "abcdef".getBytes(StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class, () -> TupleWire.decode(blob, new int[] {1, 1, 1}, new int[] {1}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> TupleWire.decode(blob, new int[] {1, 1, 1, -1}, new int[] {1}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> TupleWire.decode(blob, new int[] {2, 2, 2, -1}, new int[] {5}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> TupleWire.decode(blob, new int[] {4, 4, 4, -1}, new int[] {1}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> TupleWire.decode(blob, new int[] {-1, 2, 2, 2}, new int[] {1}, 1));
    }
}
