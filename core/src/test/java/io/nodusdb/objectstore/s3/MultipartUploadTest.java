package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.FatalStoreException;
import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultipartUploadTest {

    private static final long THRESHOLD = 2_500;
    private static final long PART = 1_000;

    @TempDir
    Path scratch;

    private FakeS3Server server;
    private S3ObjectStore store;
    private final List<Integer> pauses = new ArrayList<>();

    @BeforeEach
    void start() {
        server = FakeS3Server.start().minPartBytes(1);
        store = new S3ObjectStore(server.config(), new StaticCredentialsProvider(FakeS3Server.CREDENTIALS),
                Clock.systemUTC(), THRESHOLD, PART, pauses::add);
    }

    @AfterEach
    void stop() {
        store.close();
        server.close();
    }

    private Path file(int size, long seed) throws IOException {
        byte[] content = new byte[size];
        new Random(seed).nextBytes(content);
        Path file = scratch.resolve("upload-" + size + ".bin");
        Files.write(file, content);
        return file;
    }

    private static Predicate<FakeS3Server.Req> part(int number) {
        return request -> request.method().equals("PUT") && Integer.toString(number).equals(request.parameter("partNumber"));
    }

    private static final Predicate<FakeS3Server.Req> ANY_PART =
            request -> request.method().equals("PUT") && request.has("partNumber");
    private static final Predicate<FakeS3Server.Req> CREATE =
            request -> request.method().equals("POST") && request.has("uploads");
    private static final Predicate<FakeS3Server.Req> COMPLETE =
            request -> request.method().equals("POST") && request.has("uploadId");
    private static final Predicate<FakeS3Server.Req> ABORT =
            request -> request.method().equals("DELETE") && request.has("uploadId");

    @Test
    void aFileOverTheThresholdIsUploadedInPartsAndKeepsItsMetadata() throws IOException {
        Path file = file(3_500, 1);

        store.putFile("snap/big", file, Map.of("chain-seq", "21"));

        assertArrayEquals(Files.readAllBytes(file), server.content("snap/big"));
        assertEquals(Map.of("chain-seq", "21"), server.metadataOf("snap/big"));
        assertEquals(1, server.count(CREATE));
        assertEquals(4, server.count(ANY_PART));
        assertEquals(1, server.count(COMPLETE));
        assertEquals(0, server.count(ABORT));
        assertEquals(0, server.uploadCount());
        assertEquals(List.of(), pauses);
    }

    @Test
    void aFileAtTheThresholdIsOneRequestAndOneByteMoreIsMultipart() throws IOException {
        store.putFile("snap/at", file((int) THRESHOLD, 2), Map.of());
        assertEquals(0, server.count(CREATE));

        store.putFile("snap/over", file((int) THRESHOLD + 1, 3), Map.of());
        assertEquals(1, server.count(CREATE));
        assertEquals(THRESHOLD + 1, server.content("snap/over").length);
    }

    @Test
    void aFileThatFillsWholePartsEndsWithoutAnEmptyPart() throws IOException {
        Path file = file(3_000, 4);

        store.putFile("snap/exact", file, Map.of());

        assertEquals(3, server.count(ANY_PART));
        assertArrayEquals(Files.readAllBytes(file), server.content("snap/exact"));
    }

    @Test
    void aTransientPartFailureIsRetriedAndOnlyThatPartIsSentAgain() throws IOException {
        Path file = file(3_500, 5);
        server.failNext(part(2), 1, 503, "SlowDown");

        store.putFile("snap/retry", file, Map.of());

        assertArrayEquals(Files.readAllBytes(file), server.content("snap/retry"));
        assertEquals(2, server.count(part(2)));
        assertEquals(1, server.count(part(1)));
        assertEquals(1, server.count(part(3)));
        assertEquals(List.of(1), pauses);
    }

    @Test
    void aPartThatKeepsFailingAbortsTheUploadAndLeavesNothingBehind() throws IOException {
        Path file = file(3_500, 6);
        server.failNext(part(2), 100, 500, "InternalError");

        assertThrows(TransientStoreException.class, () -> store.putFile("snap/doomed", file, Map.of()));

        assertEquals(MultipartUploader.ATTEMPTS, server.count(part(2)));
        assertEquals(1, server.count(ABORT));
        assertEquals(0, server.uploadCount());
        assertNull(server.content("snap/doomed"));
        assertEquals(List.of(1, 2, 3, 4), pauses);
    }

    @Test
    void aFatalPartFailureIsNotRetriedButStillAborts() throws IOException {
        Path file = file(3_500, 7);
        server.failNext(part(1), 1, 403, "AccessDenied");

        assertThrows(FatalStoreException.class, () -> store.putFile("snap/denied", file, Map.of()));

        assertEquals(1, server.count(part(1)));
        assertEquals(1, server.count(ABORT));
        assertEquals(0, server.uploadCount());
        assertEquals(List.of(), pauses);
    }

    @Test
    void aFailureToStartTheUploadIsReportedWithoutAnyAbort() throws IOException {
        server.failNext(CREATE, 1, 403, "AccessDenied");

        assertThrows(FatalStoreException.class, () -> store.putFile("snap/none", file(3_500, 8), Map.of()));

        assertEquals(0, server.count(ABORT));
        assertEquals(0, server.count(ANY_PART));
    }

    @Test
    void aCompletionThatReportsAnErrorInsideA200IsRetried() throws IOException {
        Path file = file(3_500, 9);
        server.failNext(COMPLETE, 1, 200, "InternalError");

        store.putFile("snap/hidden-error", file, Map.of());

        assertEquals(2, server.count(COMPLETE));
        assertArrayEquals(Files.readAllBytes(file), server.content("snap/hidden-error"));
        assertEquals(List.of(1), pauses);
    }

    @Test
    void aCompletionWhoseResponseWasLostIsConfirmedByLookingAtTheObject() throws IOException {
        Path file = file(3_500, 10);
        server.dropNext(COMPLETE, 1, true);

        store.putFile("snap/lost", file, Map.of());

        assertEquals(2, server.count(COMPLETE));
        assertEquals(1, server.count(request -> request.method().equals("HEAD")));
        assertArrayEquals(Files.readAllBytes(file), server.content("snap/lost"));
    }

    @Test
    void aVanishedUploadWithNoObjectIsFatal() throws IOException {
        server.failNext(COMPLETE, 1, 404, "NoSuchUpload");

        FatalStoreException failure = assertThrows(FatalStoreException.class,
                () -> store.putFile("snap/gone", file(3_500, 11), Map.of()));

        assertTrue(failure.getMessage().contains("is gone"), failure.getMessage());
        assertNull(server.content("snap/gone"));
    }

    @Test
    void aVanishedUploadWhoseObjectHasAnotherSizeIsFatal() throws IOException {
        server.putRaw("snap/other", new byte[10]);
        server.failNext(COMPLETE, 1, 404, "NoSuchUpload");

        assertThrows(FatalStoreException.class, () -> store.putFile("snap/other", file(3_500, 12), Map.of()));
    }

    @Test
    void anUploadNeedingMoreThanTenThousandPartsGetsLargerParts() {
        assertEquals(PART, MultipartUploader.effectivePartBytes(PART, 5_000));
        assertEquals(PART, MultipartUploader.effectivePartBytes(PART, 10_000L * PART));
        long fiveTebibytes = 5L << 40;
        long effective = MultipartUploader.effectivePartBytes(64L << 20, fiveTebibytes);

        assertTrue(effective >= (fiveTebibytes + 9_999) / 10_000);
        assertEquals(0, effective % (1L << 20));
        assertTrue((fiveTebibytes + effective - 1) / effective <= MultipartUploader.MAX_PARTS);
    }

    @Test
    void aPartSmallerThanTheMinimumIsRefusedByAStrictServerAndAbortsCleanly() throws IOException {
        server.minPartBytes(2_000);

        assertThrows(FatalStoreException.class, () -> store.putFile("snap/small", file(3_500, 13), Map.of()));

        assertEquals(0, server.uploadCount());
    }

    @Test
    void staleUploadsAreAbortedAndRecentOrForeignOnesAreKept() {
        long now = 1_800_000_000_000L;
        long old = now - Duration.ofHours(48).toMillis();
        long recent = now - Duration.ofHours(1).toMillis();
        server.startUpload("snap/old", old);
        server.startUpload("snap/recent", recent);
        server.startUpload("other/old", old);

        int aborted = store.abortStaleUploads("snap/", Duration.ofHours(24), Instant.ofEpochMilli(now));

        assertEquals(1, aborted);
        assertEquals(2, server.uploadCount());
    }

    @Test
    void staleUploadsAreFoundAcrossListingPages() {
        long now = 1_800_000_000_000L;
        long old = now - Duration.ofHours(48).toMillis();
        server.uploadPageSize(2);
        for (int i = 0; i < 5; i++) {
            server.startUpload("snap/" + i, old);
        }

        int aborted = store.abortStaleUploads("snap/", Duration.ofHours(24), Instant.ofEpochMilli(now));

        assertEquals(5, aborted);
        assertEquals(0, server.uploadCount());
        assertEquals(3, server.count(request -> request.method().equals("GET") && request.has("uploads")));
    }

    @Test
    void abortingWithNothingStaleChangesNothing() {
        server.startUpload("snap/new", 1_800_000_000_000L);

        assertEquals(0, store.abortStaleUploads("snap/", Duration.ofHours(24), Instant.ofEpochMilli(1_800_000_000_000L)));
        assertEquals(1, server.uploadCount());
    }
}
