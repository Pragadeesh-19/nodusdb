package io.nodusdb.replica;

import io.nodusdb.json.JsonWriter;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.ship.EventLog;

import java.util.concurrent.TimeUnit;

public final class FollowerStats {

    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

    private FollowerStats() {
    }

    public static String json(GraphKernel kernel, FollowerState state, long nowNanos) {
        FollowerState.Snapshot snapshot = state.snapshot(nowNanos);
        JsonWriter json = new JsonWriter();
        json.beginObject();
        json.name("epoch").value(kernel.epoch());
        json.name("applied_lsn").value(kernel.appliedLsn());
        json.name("shipping").beginObject().name("configured").value(false).endObject();
        json.name("follower").beginObject();
        json.name("phase").value(snapshot.phase().name());
        json.name("chain_seq").value(snapshot.chainSeq());
        json.name("snapshot_lsn").value(snapshot.snapshotLsn());
        json.name("objects_applied").value(snapshot.objectsApplied());
        json.name("bytes_applied").value(snapshot.bytesApplied());
        json.name("bootstraps").value(snapshot.bootstraps());
        json.name("consecutive_failures").value(snapshot.consecutiveFailures());
        json.name("last_error").value(snapshot.lastError());
        json.name("stall_reason").value(snapshot.stallReason());
        json.name("since_confirmed_ms");
        if (snapshot.nanosSinceConfirmed() == FollowerState.NEVER_CONFIRMED) {
            json.nullValue();
        } else {
            json.value(Math.max(0, snapshot.nanosSinceConfirmed()) / NANOS_PER_MILLI);
        }
        json.name("events").beginArray();
        for (EventLog.Event event : state.recentEvents()) {
            json.beginObject().name("seq").value(event.seq()).name("message").value(event.message()).endObject();
        }
        json.endArray();
        json.endObject();
        json.endObject();
        return json.toString();
    }
}
