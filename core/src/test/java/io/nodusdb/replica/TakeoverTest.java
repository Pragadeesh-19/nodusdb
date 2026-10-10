package io.nodusdb.replica;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.error.WriterFencedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TakeoverTest {

    @TempDir
    Path root;

    private final List<String> calls = new ArrayList<>();
    private final AtomicInteger restores = new AtomicInteger();
    private long headLsn = 100;
    private long restoredLsn = 100;
    private RuntimeException failEveryOpen;
    private int failOpens;

    private Takeover<String> takeover() {
        return new Takeover<>(
                () -> calls.add("precheck"),
                () -> {
                    calls.add("claim");
                    return 5;
                },
                directory -> {
                    calls.add("restore");
                    restores.incrementAndGet();
                    Files.createDirectories(directory);
                    Files.writeString(directory.resolve("snapshot.bin"), "restore " + restores.get());
                    return new Restore.Restored(restoredLsn, 2, 90);
                },
                () -> headLsn,
                (directory, claimed, restored) -> {
                    calls.add("open");
                    assertTrue(Files.exists(directory.resolve("snapshot.bin")));
                    if (failEveryOpen != null) {
                        throw failEveryOpen;
                    }
                    if (failOpens > 0) {
                        failOpens--;
                        headLsn += 7;
                        throw new WriterFencedException("the shipped chain reaches LSN " + headLsn
                                + " but this directory's log ends at LSN " + restored.appliedLsn());
                    }
                    return "writer@" + restored.appliedLsn() + "/" + claimed;
                });
    }

    @Test
    void aTakeoverChecksClaimsRestoresAndOpensInThatOrder() throws IOException {
        Takeover.Result<String> result = takeover().run(root.resolve("graph"));

        assertEquals(List.of("precheck", "claim", "restore", "open"), calls);
        assertEquals("writer@100/5", result.writer());
        assertEquals(5, result.claimedEpoch());
        assertEquals(1, result.attempts());
        assertEquals(100, result.restored().appliedLsn());
    }

    @Test
    void anOldWriterThatLandsOneMoreObjectMakesTheTakeoverRestoreAgainWithoutClaimingAgain() throws IOException {
        failOpens = 1;
        Path directory = root.resolve("graph");

        Takeover.Result<String> result = takeover().run(directory);

        assertEquals(List.of("precheck", "claim", "restore", "open", "restore", "open"), calls);
        assertEquals(2, result.attempts());
        assertEquals("restore 2", Files.readString(directory.resolve("snapshot.bin")));
    }

    @Test
    void afterTwoMovedHeadsTheThirdRestoreOpens() throws IOException {
        failOpens = 2;

        Takeover.Result<String> result = takeover().run(root.resolve("graph"));

        assertEquals(3, result.attempts());
        assertEquals(3, restores.get());
    }

    @Test
    void aHeadThatKeepsMovingStopsTheTakeoverAfterThreeAttemptsAndLeavesNoDirectory() throws IOException {
        failOpens = 10;
        Path directory = root.resolve("graph");

        assertThrows(WriterFencedException.class, () -> takeover().run(directory));

        assertEquals(3, restores.get());
        assertFalse(Files.exists(directory));
    }

    @Test
    void aFencedOpenWhoseHeadDidNotMoveIsRealAndIsNotRetried() {
        failEveryOpen = new WriterFencedException("another writer has continued the chain");
        Path directory = root.resolve("graph");

        assertThrows(WriterFencedException.class, () -> takeover().run(directory));

        assertEquals(1, restores.get());
        assertFalse(Files.exists(directory));
    }

    @Test
    void aNonEmptyDirectoryIsRefusedBeforeAnythingIsClaimed() throws IOException {
        Path directory = Files.createDirectory(root.resolve("graph"));
        Files.writeString(directory.resolve("keep"), "keep");

        assertThrows(UnsupportedFeatureException.class, () -> takeover().run(directory));

        assertEquals(List.of(), calls);
        assertEquals("keep", Files.readString(directory.resolve("keep")));
    }

    @Test
    void aFailedPrecheckStopsBeforeTheClaim() {
        Takeover<String> failing = new Takeover<>(
                () -> {
                    throw new IllegalStateException("the bucket holds no chain");
                },
                () -> {
                    calls.add("claim");
                    return 5;
                },
                directory -> new Restore.Restored(1, 1, 1),
                () -> 1,
                (directory, claimed, restored) -> "writer");

        assertThrows(IllegalStateException.class, () -> failing.run(root.resolve("graph")));

        assertEquals(List.of(), calls);
    }

    @Test
    void aFailedRestoreAfterTheClaimLeavesNoDirectoryAndPropagates() {
        Takeover<String> failing = new Takeover<>(
                () -> { },
                () -> 5,
                directory -> {
                    Files.createDirectories(directory);
                    throw new IllegalStateException("the snapshot is gone");
                },
                () -> 1,
                (directory, claimed, restored) -> "writer");
        Path directory = root.resolve("graph");

        assertThrows(IllegalStateException.class, () -> failing.run(directory));

        assertFalse(Files.exists(directory));
    }

    @Test
    void anOpenFailureThatIsNotAFenceCleansTheDirectoryAndPropagates() {
        failEveryOpen = new IllegalStateException("disk full");
        Path directory = root.resolve("graph");

        assertThrows(IllegalStateException.class, () -> takeover().run(directory));

        assertFalse(Files.exists(directory));
        assertEquals(1, restores.get());
    }
}
