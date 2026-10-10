package io.nodusdb.replica;

import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonNull;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.kernel.GraphKernel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class FollowerStatsTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    private final GraphKernel kernel = GraphKernel.openReplica(GraphKernel.NO_MEMORY_LIMIT);
    private final FollowerState state = new FollowerState();

    private JsonObject parse(long now) {
        return JsonParser.parseObject(FollowerStats.json(kernel, state, now));
    }

    @Test
    void aNewFollowerReportsBootstrappingAtPositionZeroAndNoConfirmation() {
        JsonObject stats = parse(5 * SECOND);

        assertEquals(0, stats.requireLong("epoch"));
        assertEquals(0, stats.requireLong("applied_lsn"));
        assertFalse(stats.requireObject("shipping").boolOr("configured", true));
        JsonObject follower = stats.requireObject("follower");
        assertEquals("BOOTSTRAPPING", follower.requireString("phase"));
        assertEquals(0, follower.requireLong("chain_seq"));
        assertEquals(0, follower.requireLong("objects_applied"));
        assertEquals("", follower.requireString("last_error"));
        assertEquals("", follower.requireString("stall_reason"));
        assertInstanceOf(JsonNull.class, follower.get("since_confirmed_ms"));
        assertEquals(0, ((JsonArray) follower.get("events")).size());
    }

    @Test
    void theKernelPositionAndTheStateAreBothReported() {
        kernel.recordEpoch(3, 1, 0);
        kernel.restorePosition(40, 7);
        state.bootstrapped(40, 40, 4, 3);
        state.applied(5, 40, 3, 1_500);
        state.current(2 * SECOND);

        JsonObject stats = parse(5 * SECOND);

        assertEquals(3, stats.requireLong("epoch"));
        assertEquals(40, stats.requireLong("applied_lsn"));
        JsonObject follower = stats.requireObject("follower");
        assertEquals("CURRENT", follower.requireString("phase"));
        assertEquals(5, follower.requireLong("chain_seq"));
        assertEquals(40, follower.requireLong("snapshot_lsn"));
        assertEquals(1, follower.requireLong("objects_applied"));
        assertEquals(1_500, follower.requireLong("bytes_applied"));
        assertEquals(1, follower.requireLong("bootstraps"));
        assertEquals(3_000, follower.requireLong("since_confirmed_ms"));
    }

    @Test
    void aStalledFollowerReportsItsReasonAndItsEvents() {
        state.bootstrapped(10, 10, 2, 1);
        state.stalled("object 9 does not extend object 8");

        JsonObject follower = parse(SECOND).requireObject("follower");

        assertEquals("STALLED", follower.requireString("phase"));
        assertEquals("object 9 does not extend object 8", follower.requireString("stall_reason"));
        JsonArray events = follower.requireArray("events");
        assertEquals(1, events.size());
        assertEquals("stalled: object 9 does not extend object 8",
                ((JsonObject) events.get(0)).requireString("message"));
        assertEquals(1, ((JsonObject) events.get(0)).requireLong("seq"));
    }

    @Test
    void aLaggingFollowerReportsTheFailureCountAndTheLastError() {
        state.bootstrapped(10, 10, 2, 1);
        state.lagging("the store timed out");
        state.lagging("the store timed out again");

        JsonObject follower = parse(SECOND).requireObject("follower");

        assertEquals("LAGGING", follower.requireString("phase"));
        assertEquals(2, follower.requireLong("consecutive_failures"));
        assertEquals("the store timed out again", follower.requireString("last_error"));
    }
}
