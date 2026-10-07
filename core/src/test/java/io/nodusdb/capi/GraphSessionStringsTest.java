package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.log.LogConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSessionStringsTest {

    @TempDir
    Path directory;

    @Test
    void stringsInternToDenseIdsAndResolveBack() {
        GraphSession session = new GraphSession(new GraphKernel());
        assertEquals(KeyKind.UNSET, session.keyKind());

        long alice = session.intern(bytes("user:alice"), 0, 10);
        long admin = session.intern(bytes("role:admin"), 0, 10);

        assertEquals(KeyKind.STRING, session.keyKind());
        assertEquals(0L, alice);
        assertEquals(1L, admin);
        assertArrayEquals(bytes("role:admin"), session.resolve(admin));
        assertEquals(-1L, session.lookup(bytes("user:bob"), 0, 7));
    }

    @Test
    void batchOfStringPairsAddsEdgesAndSkipsNothing() {
        GraphSession session = new GraphSession(new GraphKernel());
        String[] keys = {"user:a", "role:x", "user:b", "role:x", "user:a", "role:y"};
        byte[] blob = concatenate(keys);
        int[] lengths = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            lengths[i] = keys[i].getBytes(StandardCharsets.UTF_8).length;
        }

        int added = session.addStringEdges(blob, lengths, keys.length / 2);

        assertEquals(3, added);
        long userA = session.lookup(bytes("user:a"), 0, 6);
        long roleX = session.lookup(bytes("role:x"), 0, 6);
        assertTrue(session.hasEdge(userA, roleX));
        assertEquals(2, session.degree(userA));
    }

    @Test
    void intKeysAfterStringKeysAreRefusedByTheKeyClaim() {
        GraphSession session = new GraphSession(new GraphKernel());
        session.intern(bytes("user:alice"), 0, 10);

        assertEquals(KeyKind.STRING, session.claimKeys(KeyKind.INTEGER));
    }

    @Test
    void stringKeysAfterIntegerKeysAreRefused() {
        GraphSession session = new GraphSession(new GraphKernel());
        session.claimKeys(KeyKind.INTEGER);

        assertThrows(IllegalStateException.class, () -> session.intern(bytes("user:alice"), 0, 10));
        assertThrows(IllegalStateException.class, () -> session.lookup(bytes("user:alice"), 0, 10));
    }

    @Test
    void durableGraphRestoresStringIdsAndEdgesAfterReopen() throws IOException {
        GraphSessions sessions = new GraphSessions();
        long handle = sessions.openDurable(directory, LogConfig.DEFAULT);
        GraphSession session = sessions.get(handle);
        long alice = session.intern(bytes("user:alice"), 0, 10);
        long admin = session.intern(bytes("role:admin"), 0, 10);
        session.addEdge(alice, admin);
        sessions.close(handle);

        GraphSessions reopenedSessions = new GraphSessions();
        long reopenedHandle = reopenedSessions.openDurable(directory, LogConfig.DEFAULT);
        GraphSession reopened = reopenedSessions.get(reopenedHandle);

        assertEquals(KeyKind.STRING, reopened.keyKind());
        assertEquals(alice, reopened.lookup(bytes("user:alice"), 0, 10));
        assertEquals(admin, reopened.lookup(bytes("role:admin"), 0, 10));
        assertTrue(reopened.hasEdge(alice, admin));
        reopenedSessions.close(reopenedHandle);
    }

    @Test
    void integerGraphKeepsItsKindAcrossReopen() throws IOException {
        GraphSessions sessions = new GraphSessions();
        long handle = sessions.openDurable(directory, LogConfig.DEFAULT);
        sessions.get(handle).claimKeys(KeyKind.INTEGER);
        sessions.close(handle);

        GraphSessions reopenedSessions = new GraphSessions();
        long reopenedHandle = reopenedSessions.openDurable(directory, LogConfig.DEFAULT);
        GraphSession reopened = reopenedSessions.get(reopenedHandle);

        assertEquals(KeyKind.INTEGER, reopened.keyKind());
        assertThrows(IllegalStateException.class, () -> reopened.intern(bytes("user:alice"), 0, 10));
        reopenedSessions.close(reopenedHandle);
    }

    @Test
    void graphWithEdgesBeforeTheSymbolFileExistsIsTreatedAsIntegerKeyed() throws IOException {
        GraphKernel kernel = new GraphKernel();
        kernel.addEdge(0L, 1L);
        GraphSession session = new GraphSession(kernel);

        assertEquals(KeyKind.INTEGER, session.keyKind());
        assertThrows(IllegalStateException.class, () -> session.intern(bytes("user:alice"), 0, 10));
    }

    @Test
    void hundredThousandStringsSurviveReopen() throws IOException {
        GraphSessions sessions = new GraphSessions();
        long handle = sessions.openDurable(directory, LogConfig.DEFAULT);
        GraphSession session = sessions.get(handle);
        Set<Long> ids = new HashSet<>();
        for (int i = 0; i < 100_000; i++) {
            byte[] key = bytes("node:" + i);
            ids.add(session.intern(key, 0, key.length));
        }
        sessions.close(handle);

        GraphSessions reopenedSessions = new GraphSessions();
        long reopenedHandle = reopenedSessions.openDurable(directory, LogConfig.DEFAULT);
        GraphSession reopened = reopenedSessions.get(reopenedHandle);
        assertEquals(100_000, ids.size());
        for (int i = 0; i < 100_000; i++) {
            byte[] key = bytes("node:" + i);
            assertEquals(i, reopened.lookup(key, 0, key.length));
        }
        reopenedSessions.close(reopenedHandle);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concatenate(String... values) {
        int total = 0;
        for (String value : values) {
            total += value.getBytes(StandardCharsets.UTF_8).length;
        }
        byte[] out = new byte[total];
        int cursor = 0;
        for (String value : values) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            System.arraycopy(bytes, 0, out, cursor, bytes.length);
            cursor += bytes.length;
        }
        return out;
    }
}
