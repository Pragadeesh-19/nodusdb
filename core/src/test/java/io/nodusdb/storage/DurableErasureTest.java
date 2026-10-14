package io.nodusdb.storage;

import io.nodusdb.authz.TupleStore;
import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.ErasurePendingException;
import io.nodusdb.kernel.ErasureProgress;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.symbols.Pseudonym;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableErasureTest {

    private static final String NAME = "user:alice-erasure-target";
    private static final Duration WAIT = Duration.ofSeconds(30);

    @TempDir
    Path directory;

    @TempDir
    Path image;

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static int idOf(GraphKernel kernel, String name) {
        byte[] bytes = utf8(name);
        return kernel.symbols().lookup(bytes, 0, bytes.length);
    }

    private static List<Path> filesHolding(Path root, byte[] needle) throws IOException {
        List<Path> holding = new ArrayList<>();
        try (Stream<Path> files = Files.walk(root)) {
            for (Path file : files.filter(Files::isRegularFile).filter(DurableErasureTest::readable).toList()) {
                byte[] content = Files.readAllBytes(file);
                for (int start = 0; start + needle.length <= content.length; start++) {
                    if (Arrays.equals(content, start, start + needle.length, needle, 0, needle.length)) {
                        holding.add(file);
                        break;
                    }
                }
            }
        }
        return holding;
    }

    private static boolean readable(Path file) {
        return !file.getFileName().toString().equals(GraphFiles.LOCK);
    }

    private static TupleStore grant(GraphKernel kernel) {
        TupleStore store = TupleStore.open(kernel);
        store.applySchema(SchemaFixtures.DOCUMENTS);
        store.add("document:readme", "viewer", NAME);
        store.add("group:eng", "member", NAME);
        store.add("document:plan", "viewer", "user:bob");
        return store;
    }

    @Test
    void anErasureSurvivesARestartThroughTheSnapshot() throws IOException {
        GraphKernel first = StoredGraphs.open(directory, StoredGraphs.SYNC);
        TupleStore store = grant(first);
        int erased = idOf(first, NAME);
        byte[] pseudonym = Pseudonym.of(first.erasureSalt(), utf8(NAME));
        Token token = store.erase(NAME);
        first.close();

        GraphKernel second = StoredGraphs.open(directory, StoredGraphs.SYNC);
        try {
            assertTrue(second.symbols().isErased(erased));
            assertArrayEquals(pseudonym, second.symbols().resolve(erased));
            assertEquals(-1, idOf(second, NAME));
            assertEquals(1, second.erasedSymbols());
            assertTrue(second.newestEraseLsn() >= token.lsn());
            assertArrayEquals(pseudonym, Pseudonym.of(second.erasureSalt(), utf8(NAME)));
            assertTrue(TupleStore.open(second).check("document:plan", "view", "user:bob"));
        } finally {
            second.close();
        }
    }

    @Test
    void anErasureIsReplayedFromTheLogAfterACrash() throws IOException {
        GraphKernel live = StoredGraphs.open(directory, StoredGraphs.SYNC);
        int erased;
        Token token;
        try {
            TupleStore store = grant(live);
            erased = idOf(live, NAME);
            token = store.erase(NAME);
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        GraphKernel recovered = StoredGraphs.open(image, StoredGraphs.SYNC);
        try {
            assertTrue(recovered.symbols().isErased(erased));
            assertEquals(-1, idOf(recovered, NAME));
            assertEquals(token.lsn(), recovered.newestEraseLsn());
            assertFalse(TupleStore.open(recovered).check("document:readme", "view", NAME));
        } finally {
            recovered.close();
        }
    }

    @Test
    void aCheckpointAfterTheErasureLeavesNoCopyOfTheNameInTheDirectory() throws IOException {
        GraphKernel kernel = StoredGraphs.open(directory, StoredGraphs.SYNC);
        try {
            TupleStore store = grant(kernel);
            kernel.checkpoint();
            assertFalse(filesHolding(directory, utf8(NAME)).isEmpty());

            Token token = store.erase(NAME);
            kernel.checkpoint();

            assertEquals(List.of(), filesHolding(directory, utf8(NAME)));
            assertTrue(kernel.checkpointLsn() >= token.lsn());
            assertTrue(kernel.oldestLogLsn() > kernel.newestEraseLsn());
        } finally {
            kernel.close();
        }
    }

    @Test
    void beforeTheCheckpointTheLogStillHoldsTheName() throws IOException {
        GraphKernel kernel = StoredGraphs.open(directory, StoredGraphs.SYNC);
        try {
            TupleStore store = grant(kernel);
            store.erase(NAME);

            assertFalse(filesHolding(directory, utf8(NAME)).isEmpty());
            assertTrue(kernel.oldestLogLsn() <= kernel.newestEraseLsn());
            assertTrue(kernel.erasureProgress().layers().get(0).pending());
        } finally {
            kernel.close();
        }
    }

    @Test
    void waitingForTheErasureScrubsTheDirectoryWithoutAnExplicitCheckpoint() throws IOException {
        GraphKernel kernel = StoredGraphs.open(directory, StoredGraphs.SYNC);
        try {
            Token token = grant(kernel).erase(NAME);

            kernel.erasureProgress().awaitErased(token, WAIT);

            assertEquals(List.of(), filesHolding(directory, utf8(NAME)));
            ErasureProgress.Layer local = kernel.erasureProgress().layers().get(0);
            assertEquals("local", local.name());
            assertFalse(local.pending());
            assertTrue(local.coveredLsn() >= token.lsn());
        } finally {
            kernel.close();
        }
    }

    @Test
    void aRestartFinishesAScrubThatACrashInterrupted() throws IOException {
        GraphKernel live = StoredGraphs.open(directory, StoredGraphs.SYNC);
        Token token;
        try {
            token = grant(live).erase(NAME);
            StoredGraphs.crashImage(directory, image);
        } finally {
            live.close();
        }

        GraphKernel recovered = StoredGraphs.open(image, StoredGraphs.SYNC);
        try {
            assertTrue(recovered.erasureProgress().layers().get(0).pending());

            recovered.erasureProgress().awaitErased(token, WAIT);

            assertEquals(List.of(), filesHolding(image, utf8(NAME)));
        } finally {
            recovered.close();
        }
    }

    @Test
    void aWaitThatEndsFirstNamesTheLocalLayer() throws IOException {
        GraphKernel kernel = StoredGraphs.open(directory, StoredGraphs.SYNC);
        try {
            Token token = grant(kernel).erase(NAME);

            ErasurePendingException pending = assertThrows(ErasurePendingException.class,
                    () -> kernel.erasureProgress().awaitErased(token, Duration.ZERO));

            assertEquals(List.of("local"), pending.pendingLayers());
        } finally {
            kernel.close();
        }
    }

    @Test
    void closingTheGraphDoesNotWaitForTheCheckpointDelay() throws IOException {
        GraphKernel kernel = StoredGraphs.open(directory, StoredGraphs.SYNC);
        grant(kernel).erase(NAME);
        long started = System.nanoTime();

        kernel.close();

        assertTrue(System.nanoTime() - started < Duration.ofSeconds(20).toNanos());
        assertEquals(List.of(), filesHolding(directory, utf8(NAME)));
    }
}
