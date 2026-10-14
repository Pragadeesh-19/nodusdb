package io.nodusdb.ship;

import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonNull;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.kernel.ErasureProgress;
import io.nodusdb.kernel.closure.ClosureStats;
import io.nodusdb.log.ShipWatermark;
import io.nodusdb.stats.ErasureReport;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShipStatsTest {

    private static final ClosureStats REACHABILITY = new ClosureStats(true, true, 3, 40, 1_048_576, 7, 2, 5, 1, 1,
            "");

    private static JsonObject parse(String json) {
        return JsonParser.parseObject(json);
    }

    private static String statsJson(long epoch, long appliedLsn, ShipWatermark watermark) {
        return ShipStats.json(epoch, appliedLsn, watermark, REACHABILITY, ErasureReport.NONE);
    }

    private static ShipState state() {
        return new ShipState(4, 40, 3, 1_000);
    }

    @Test
    void theReachabilityIndexIsReportedNextToShipping() {
        JsonObject reachability = parse(statsJson(2, 17, ShipWatermark.NONE))
                .requireObject("reachability");

        assertTrue(reachability.boolOr("enabled", false));
        assertEquals("building", reachability.requireString("state"));
        assertEquals(3, reachability.requireLong("dirty_nodes"));
        assertEquals(40, reachability.requireLong("closure_entries"));
        assertEquals(1_048_576, reachability.requireLong("entry_cap"));
        assertEquals(7, reachability.requireLong("incremental_updates"));
        assertEquals(2, reachability.requireLong("dirty_marks"));
        assertEquals(5, reachability.requireLong("rebuilt_nodes"));
        assertEquals(1, reachability.requireLong("refusals"));
        assertEquals(1, reachability.requireLong("full_passes"));
        assertEquals("", reachability.requireString("last_error"));
    }

    @Test
    void erasureReportsEachLayerAndWhetherItIsConfigured() {
        ErasureReport erasure = new ErasureReport(3, 812, List.of(new ErasureProgress.Layer("local", 812, false)),
                "");

        JsonObject report = parse(ShipStats.json(2, 900, ShipWatermark.NONE, REACHABILITY, erasure))
                .requireObject("erasure");

        assertEquals(3, report.requireLong("erased_symbols"));
        assertEquals(812, report.requireLong("newest_erase_lsn"));
        JsonObject local = report.requireObject("local");
        assertTrue(local.boolOr("configured", false));
        assertEquals(812, local.requireLong("covered_lsn"));
        assertFalse(local.boolOr("pending", true));
        assertFalse(report.requireObject("bucket").boolOr("configured", true));
        assertFalse(report.requireObject("lake").boolOr("configured", true));
        assertEquals("", report.requireString("last_error"));
    }

    @Test
    void aGraphWithoutShippingReportsOnlyThatItIsNotConfigured() {
        JsonObject stats = parse(statsJson(2, 17, ShipWatermark.NONE));

        assertEquals(2, stats.requireLong("epoch"));
        assertEquals(17, stats.requireLong("applied_lsn"));
        JsonObject shipping = stats.requireObject("shipping");
        assertFalse(shipping.boolOr("configured", true));
        assertEquals(1, shipping.members().size());
    }

    @Test
    void aFreshStateReportsItsStartingPositionAndNoAges() {
        JsonObject shipping = parse(statsJson(4, 52, state())).requireObject("shipping");

        assertTrue(shipping.boolOr("configured", false));
        assertEquals("STARTING", shipping.requireString("phase"));
        assertEquals(4, shipping.requireLong("epoch"));
        assertEquals(40, shipping.requireLong("shipped_lsn"));
        assertEquals(12, shipping.requireLong("lag_lsn"));
        assertEquals(3, shipping.requireLong("chain_seq"));
        assertEquals(1_000, shipping.requireLong("backlog_cap_bytes"));
        assertEquals("", shipping.requireString("last_error"));
        assertInstanceOf(JsonNull.class, shipping.get("last_commit_age_ms"));
        assertInstanceOf(JsonNull.class, shipping.requireObject("snapshot_references").get("age_ms"));
        assertEquals("DISABLED", shipping.requireObject("projection").requireString("phase"));
    }

    @Test
    void progressAndFailuresAreReflected() {
        ShipState state = state();
        state.shipped(60, 4, 512, System.nanoTime());
        state.backlog(300);
        state.failed("the store said \"no\"\nand then some");

        JsonObject shipping = parse(statsJson(4, 60, state)).requireObject("shipping");

        assertEquals("FAILED", shipping.requireString("phase"));
        assertEquals(60, shipping.requireLong("shipped_lsn"));
        assertEquals(0, shipping.requireLong("lag_lsn"));
        assertEquals(4, shipping.requireLong("chain_seq"));
        assertEquals(300, shipping.requireLong("backlog_bytes"));
        assertEquals(1, shipping.requireLong("consecutive_failures"));
        assertEquals("the store said \"no\"\nand then some", shipping.requireString("last_error"));
        assertTrue(shipping.requireLong("last_commit_age_ms") >= 0);
        assertEquals(1, shipping.requireLong("objects_shipped"));
        assertEquals(512, shipping.requireLong("bytes_shipped"));
    }

    @Test
    void lagNeverGoesNegativeWhenTheWatermarkIsAheadOfTheAppliedLsn() {
        ShipState state = state();
        state.shipped(90, 5, 10, 1);

        JsonObject shipping = parse(statsJson(4, 80, state)).requireObject("shipping");

        assertEquals(0, shipping.requireLong("lag_lsn"));
    }

    @Test
    void referencesRetentionAndProjectionBlocksCarryTheirCounters() {
        ShipState state = state();
        state.referenceCommitted(7, 55, System.nanoTime());
        state.retentionSwept(3, 2);
        state.retentionFailed("denied");

        JsonObject shipping = parse(statsJson(4, 60, state)).requireObject("shipping");

        JsonObject references = shipping.requireObject("snapshot_references");
        assertEquals(7, references.requireLong("seq"));
        assertEquals(55, references.requireLong("lsn"));
        assertTrue(references.requireLong("age_ms") >= 0);
        JsonObject retention = shipping.requireObject("retention");
        assertEquals(1, retention.requireLong("sweeps"));
        assertEquals(3, retention.requireLong("chain_objects_deleted"));
        assertEquals(2, retention.requireLong("snapshots_deleted"));
        assertEquals(1, retention.requireLong("failures"));
        assertEquals("denied", retention.requireString("last_error"));
    }

    @Test
    void eventsAreReportedWithSequenceNumbersAndAreNotConsumed() {
        ShipState state = state();
        state.failed("first");
        state.active();
        state.failed("second");

        JsonArray first = parse(statsJson(4, 60, state)).requireObject("shipping").requireArray("events");
        JsonArray again = parse(statsJson(4, 60, state)).requireObject("shipping").requireArray("events");

        assertEquals(2, first.size());
        assertEquals(1, ((JsonObject) first.get(0)).requireLong("seq"));
        assertEquals("shipping failed: first", ((JsonObject) first.get(0)).requireString("message"));
        assertEquals(2, ((JsonObject) first.get(1)).requireLong("seq"));
        assertEquals(first, again);
    }

    @Test
    void theOutputIsAlwaysAWellFormedDocumentForAnyErrorText() {
        ShipState state = state();
        state.failed("tab\there \u0001 control and 😀 emoji and \\ backslash");

        JsonObject shipping = parse(statsJson(1, 1, state)).requireObject("shipping");

        assertEquals("tab\there \u0001 control and 😀 emoji and \\ backslash",
                shipping.requireString("last_error"));
    }
}
