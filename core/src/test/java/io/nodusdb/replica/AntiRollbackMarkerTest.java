package io.nodusdb.replica;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.error.CorruptLogException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AntiRollbackMarkerTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final ChainHash DIGEST_TEN = ChainHash.sha256(new byte[] {10});
    private static final ChainHash OTHER = ChainHash.sha256(new byte[] {99});

    @TempDir
    Path root;

    private static MarkerPosition at(long seq, ChainHash digest) {
        return new MarkerPosition(seq, digest, 2, seq * 100);
    }

    private Path markerFile() {
        return root.resolve(AntiRollbackMarker.FILE_NAME);
    }

    @Test
    void aDisabledMarkerRemembersNothingAndAcceptsEverything() {
        AntiRollbackMarker marker = AntiRollbackMarker.disabled();

        marker.requireHead(1, OTHER);
        marker.requireObject(1, OTHER);
        marker.requireBucketMayBeEmpty();
        marker.observe(at(5, DIGEST_TEN), 0);
        marker.flush();

        assertEquals(Optional.empty(), marker.remembered());
    }

    @Test
    void aFreshStateDirectoryStartsWithNothingRemembered() throws IOException {
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);

        assertEquals(Optional.empty(), marker.remembered());
        marker.requireBucketMayBeEmpty();
    }

    @Test
    void aPositionIsWrittenToTheFileAndFoundAgainAfterARestart() throws IOException {
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);

        marker.observe(at(10, DIGEST_TEN), 0);

        assertTrue(Files.isRegularFile(markerFile()));
        assertEquals(Optional.of(at(10, DIGEST_TEN)), AntiRollbackMarker.in(root).remembered());
    }

    @Test
    void aHeadBehindTheRememberedPositionIsARollback() throws IOException {
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);
        marker.observe(at(10, DIGEST_TEN), 0);

        ChainTrustException refused = assertThrows(ChainTrustException.class,
                () -> marker.requireHead(9, OTHER));

        assertTrue(refused.getMessage().contains("rolled back"), refused.getMessage());
    }

    @Test
    void aHeadAtTheRememberedSeqWithAnotherDigestIsAFork() throws IOException {
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);
        marker.observe(at(10, DIGEST_TEN), 0);

        ChainTrustException refused = assertThrows(ChainTrustException.class,
                () -> marker.requireHead(10, OTHER));

        assertTrue(refused.getMessage().contains("fork"), refused.getMessage());
    }

    @Test
    void aHeadAtOrBeyondTheRememberedPositionIsAccepted() throws IOException {
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);
        marker.observe(at(10, DIGEST_TEN), 0);

        marker.requireHead(10, DIGEST_TEN);
        marker.requireHead(11, OTHER);
        marker.requireHead(500, OTHER);
    }

    @Test
    void anObjectAtTheRememberedSeqMustCarryTheRememberedDigest() throws IOException {
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);
        marker.observe(at(10, DIGEST_TEN), 0);

        marker.requireObject(10, DIGEST_TEN);
        marker.requireObject(9, OTHER);
        marker.requireObject(11, OTHER);
        assertThrows(ChainTrustException.class, () -> marker.requireObject(10, OTHER));
    }

    @Test
    void anEmptyBucketIsARollbackOnceSomethingWasRemembered() throws IOException {
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);
        marker.observe(at(10, DIGEST_TEN), 0);

        assertThrows(ChainTrustException.class, marker::requireBucketMayBeEmpty);
    }

    @Test
    void writesAreLimitedToOnePerSecondAndFlushWritesThePendingPosition() throws IOException {
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);

        marker.observe(at(10, DIGEST_TEN), 0);
        marker.observe(at(11, OTHER), SECOND / 2);
        assertEquals(Optional.of(at(10, DIGEST_TEN)), MarkerFile.read(markerFile()));

        marker.observe(at(12, OTHER), SECOND);
        assertEquals(Optional.of(at(12, OTHER)), MarkerFile.read(markerFile()));

        marker.observe(at(13, DIGEST_TEN), SECOND + SECOND / 4);
        assertEquals(Optional.of(at(12, OTHER)), MarkerFile.read(markerFile()));
        marker.flush();
        assertEquals(Optional.of(at(13, DIGEST_TEN)), MarkerFile.read(markerFile()));
    }

    @Test
    void aFlushWithNothingNewWritesNothing() throws IOException {
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);
        marker.observe(at(10, DIGEST_TEN), 0);
        Files.delete(markerFile());

        marker.flush();

        assertFalse(Files.exists(markerFile()));
    }

    @Test
    void aPositionThatIsNotNewerThanTheFileIsIgnored() throws IOException {
        Path file = markerFile();
        MarkerFile.write(file, at(10, DIGEST_TEN));
        AntiRollbackMarker marker = AntiRollbackMarker.in(root);

        marker.observe(at(4, OTHER), 0);
        marker.observe(at(10, DIGEST_TEN), SECOND * 5);
        marker.flush();

        assertEquals(Optional.of(at(10, DIGEST_TEN)), MarkerFile.read(file));
    }

    @Test
    void aDamagedFileStopsTheOpenAndNamesTheFile() throws IOException {
        Files.write(markerFile(), new byte[] {1, 2, 3});

        CorruptLogException refused = assertThrows(CorruptLogException.class, () -> AntiRollbackMarker.in(root));

        assertTrue(refused.getMessage().contains(markerFile().toString()), refused.getMessage());
    }

    @Test
    void theStateDirectoryIsCreatedWhenItIsMissing() throws IOException {
        Path state = root.resolve("a").resolve("b");

        AntiRollbackMarker marker = AntiRollbackMarker.in(state);
        marker.observe(at(3, DIGEST_TEN), 0);

        assertTrue(Files.isRegularFile(state.resolve(AntiRollbackMarker.FILE_NAME)));
    }
}
