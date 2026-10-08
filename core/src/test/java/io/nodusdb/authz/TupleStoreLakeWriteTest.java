package io.nodusdb.authz;

import io.nodusdb.authz.schema.SchemaFixtures;
import io.nodusdb.error.ShipTimeoutException;
import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.error.StaleReadException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.ShipWatermark;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TupleStoreLakeWriteTest {

    private record Wait(long epoch, long lsn, long timeoutNanos) {
    }

    private static final class RecordingWatermark implements ShipWatermark {

        final List<Wait> waits = new ArrayList<>();
        Consumer<Wait> onWait = wait -> {
        };

        @Override
        public boolean configured() {
            return true;
        }

        @Override
        public long shippedLsn() {
            return -1;
        }

        @Override
        public void awaitShipped(long epoch, long lsn, long timeoutNanos) {
            Wait wait = new Wait(epoch, lsn, timeoutNanos);
            waits.add(wait);
            onWait.accept(wait);
        }
    }

    private final GraphKernel kernel = new GraphKernel();
    private final RecordingWatermark watermark = new RecordingWatermark();
    private final TupleStore store = TupleStore.open(kernel);

    private void shipped() {
        kernel.attachShipping(watermark);
        store.applySchema(SchemaFixtures.DOCUMENTS);
    }

    private static TupleTransaction grant(String object) {
        return new TupleTransaction().add(object, "viewer", "user:alice");
    }

    @Test
    void aLakeWriteWaitsForTheLsnItWasGiven() {
        shipped();

        Token token = store.write(grant("document:a"), Durability.LAKE);

        assertEquals(1, watermark.waits.size());
        Wait wait = watermark.waits.get(0);
        assertEquals(token.epoch(), wait.epoch());
        assertEquals(token.lsn(), wait.lsn());
        assertEquals(TupleStore.DEFAULT_SHIP_WAIT.toNanos(), wait.timeoutNanos());
        assertTrue(store.check("document:a", "view", "user:alice", token));
    }

    @Test
    void aLocalWriteNeverWaits() {
        shipped();

        store.write(grant("document:a"), Durability.LOCAL);
        store.write(grant("document:b"));
        store.add("document:c", "viewer", "user:alice");

        assertEquals(List.of(), watermark.waits);
    }

    @Test
    void theWaitHappensAfterTheWriterLockIsReleased() throws Exception {
        shipped();
        watermark.onWait = wait -> {
            CompletableFuture<Token> concurrent = CompletableFuture.supplyAsync(
                    () -> store.add("document:other", "viewer", "user:bob"));
            try {
                assertTrue(concurrent.get(10, TimeUnit.SECONDS).lsn() > wait.lsn());
            } catch (Exception e) {
                throw new AssertionError("a write could not proceed while a lake write waited", e);
            }
        };

        store.write(grant("document:a"), Durability.LAKE);

        assertTrue(store.check("document:other", "view", "user:bob"));
    }

    @Test
    void aTimeoutLeavesTheWriteAppliedLocally() {
        shipped();
        watermark.onWait = wait -> {
            throw new ShipTimeoutException("not shipped", wait.epoch(), wait.lsn());
        };

        ShipTimeoutException timeout = assertThrows(ShipTimeoutException.class,
                () -> store.write(grant("document:a"), Durability.LAKE));

        assertTrue(store.check("document:a", "view", "user:alice"));
        assertEquals(timeout.lsn(), kernel.appliedLsn());
        assertEquals(timeout.epoch(), kernel.epoch());
    }

    @Test
    void aTimeoutCanBeAskedForPerWrite() {
        shipped();

        store.write(grant("document:a"), Durability.LAKE, Duration.ofMillis(250));

        assertEquals(TimeUnit.MILLISECONDS.toNanos(250), watermark.waits.get(0).timeoutNanos());
    }

    @Test
    void aWriteThatChangesNothingWaitsForTheCurrentToken() {
        shipped();
        Token first = store.write(grant("document:a"), Durability.LOCAL);

        Token again = store.write(grant("document:a"), Durability.LAKE);

        assertEquals(first, again);
        assertEquals(List.of(new Wait(first.epoch(), first.lsn(), TupleStore.DEFAULT_SHIP_WAIT.toNanos())),
                watermark.waits);
    }

    @Test
    void aRejectedLakeWriteWritesAndWaitsForNothing() {
        shipped();
        Token before = store.token();
        TupleTransaction invalid = new TupleTransaction().add("document:a", "nosuchrelation", "user:alice");

        assertThrows(SchemaViolationException.class, () -> store.write(invalid, Durability.LAKE));

        assertEquals(before, store.token());
        assertEquals(List.of(), watermark.waits);
    }

    @Test
    void withoutShippingALakeWriteIsRefusedBeforeAnythingIsWritten() {
        store.applySchema(SchemaFixtures.DOCUMENTS);
        Token before = store.token();

        UnsupportedFeatureException refused = assertThrows(UnsupportedFeatureException.class,
                () -> store.write(grant("document:a"), Durability.LAKE));

        assertEquals("shipping is not configured for this graph", refused.getMessage());
        assertEquals(before, store.token());
        assertFalse(store.check("document:a", "view", "user:alice"));
    }

    @Test
    void awaitShippedWithoutShippingIsRefused() {
        store.applySchema(SchemaFixtures.DOCUMENTS);

        assertThrows(UnsupportedFeatureException.class,
                () -> store.awaitShipped(store.token(), Duration.ofSeconds(1)));
    }

    @Test
    void awaitShippedPassesTheTokenAndTheTimeoutThrough() {
        shipped();
        Token token = store.add("document:a", "viewer", "user:alice");

        store.awaitShipped(token, Duration.ofSeconds(2));

        assertEquals(List.of(new Wait(token.epoch(), token.lsn(), TimeUnit.SECONDS.toNanos(2))), watermark.waits);
    }

    @Test
    void awaitShippedRefusesATokenFromTheFutureWithoutWaiting() {
        shipped();
        Token token = store.add("document:a", "viewer", "user:alice");

        assertThrows(StaleReadException.class,
                () -> store.awaitShipped(new Token(token.epoch(), token.lsn() + 1), Duration.ofSeconds(1)));
        assertThrows(StaleReadException.class,
                () -> store.awaitShipped(new Token(token.epoch() + 1, 1), Duration.ofSeconds(1)));
        assertEquals(List.of(), watermark.waits);
    }

    @Test
    void awaitShippedRefusesANonPositiveTimeout() {
        shipped();
        Token token = store.token();

        assertThrows(IllegalArgumentException.class, () -> store.awaitShipped(token, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> store.awaitShipped(token, Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class, () -> store.awaitShipped(token, null));
        assertThrows(NullPointerException.class, () -> store.awaitShipped(null, Duration.ofSeconds(1)));
        assertEquals(List.of(), watermark.waits);
    }

    @Test
    void anAbsurdlyLongTimeoutIsClampedInsteadOfOverflowing() {
        shipped();
        Token token = store.token();

        store.awaitShipped(token, Duration.ofSeconds(Long.MAX_VALUE));
        store.awaitShipped(token, Duration.ofSeconds(Long.MAX_VALUE, 999_999_999));
        store.awaitShipped(token, Duration.ofDays(36_500));

        assertEquals(3, watermark.waits.size());
        for (Wait wait : watermark.waits) {
            assertEquals(TimeUnit.DAYS.toNanos(3_650), wait.timeoutNanos());
        }
    }

    @Test
    void aShortTimeoutIsNotRoundedUpToZero() {
        shipped();

        store.awaitShipped(store.token(), Duration.ofNanos(1));

        assertEquals(1, watermark.waits.get(0).timeoutNanos());
    }

    @Test
    void aKernelStartsWithNoShippingAndAttachesItOnce() {
        GraphKernel fresh = new GraphKernel();
        assertSame(ShipWatermark.NONE, fresh.shipWatermark());
        assertFalse(fresh.shipWatermark().configured());

        fresh.attachShipping(watermark);

        assertSame(watermark, fresh.shipWatermark());
        assertThrows(IllegalStateException.class, () -> fresh.attachShipping(new RecordingWatermark()));
        assertSame(watermark, fresh.shipWatermark());
        assertThrows(NullPointerException.class, () -> new GraphKernel().attachShipping(null));
    }
}
