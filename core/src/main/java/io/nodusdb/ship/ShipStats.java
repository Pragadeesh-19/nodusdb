package io.nodusdb.ship;

import io.nodusdb.json.JsonWriter;
import io.nodusdb.log.ShipWatermark;

public final class ShipStats {

    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final long NEVER = 0L;

    private ShipStats() {
    }

    public static String json(long epoch, long appliedLsn, ShipWatermark watermark) {
        JsonWriter json = new JsonWriter();
        json.beginObject();
        json.name("epoch").value(epoch);
        json.name("applied_lsn").value(appliedLsn);
        json.name("shipping");
        if (watermark instanceof ShipState state) {
            shipping(json, state, appliedLsn);
        } else {
            json.beginObject().name("configured").value(false).endObject();
        }
        json.endObject();
        return json.toString();
    }

    private static void shipping(JsonWriter json, ShipState state, long appliedLsn) {
        ShipState.Snapshot snapshot = state.snapshot();
        json.beginObject();
        json.name("configured").value(true);
        json.name("phase").value(snapshot.phase().name());
        json.name("epoch").value(snapshot.epoch());
        json.name("shipped_lsn").value(snapshot.shippedLsn());
        json.name("lag_lsn").value(Math.max(0, appliedLsn - snapshot.shippedLsn()));
        json.name("chain_seq").value(snapshot.chainSeq());
        json.name("backlog_bytes").value(snapshot.backlogBytes());
        json.name("backlog_cap_bytes").value(snapshot.backlogCapBytes());
        json.name("consecutive_failures").value(snapshot.consecutiveFailures());
        json.name("last_error").value(snapshot.lastError());
        json.name("objects_shipped").value(snapshot.objectsShipped());
        json.name("bytes_shipped").value(snapshot.bytesShipped());
        age(json, "last_commit_age_ms", snapshot.lastCommitNanos(), snapshot.nowNanos());
        references(json, snapshot.references(), snapshot.nowNanos());
        retention(json, snapshot.retention());
        projection(json, snapshot.projection(), snapshot.nowNanos());
        events(json, state);
        json.endObject();
    }

    private static void references(JsonWriter json, ShipState.ReferenceStatus references, long now) {
        json.name("snapshot_references").beginObject();
        json.name("seq").value(references.seq());
        json.name("lsn").value(references.lsn());
        age(json, "age_ms", references.committedNanos(), now);
        json.name("failures").value(references.failures());
        json.name("last_error").value(references.lastError());
        json.endObject();
    }

    private static void retention(JsonWriter json, ShipState.RetentionStatus retention) {
        json.name("retention").beginObject();
        json.name("sweeps").value(retention.sweeps());
        json.name("chain_objects_deleted").value(retention.chainObjectsDeleted());
        json.name("snapshots_deleted").value(retention.snapshotsDeleted());
        json.name("failures").value(retention.failures());
        json.name("last_error").value(retention.lastError());
        json.endObject();
    }

    private static void projection(JsonWriter json, ShipState.ProjectionStatus projection, long now) {
        json.name("projection").beginObject();
        json.name("phase").value(projection.phase().name());
        json.name("projected_lsn").value(projection.projectedLsn());
        json.name("projected_chain_seq").value(projection.projectedSeq());
        json.name("iceberg_snapshot_id").value(projection.snapshotId());
        age(json, "last_commit_age_ms", projection.lastCommitNanos(), now);
        json.name("commits").value(projection.commits());
        json.name("rows").value(projection.rows());
        json.name("failures").value(projection.failures());
        json.name("last_error").value(projection.lastError());
        json.endObject();
    }

    private static void events(JsonWriter json, ShipState state) {
        json.name("events").beginArray();
        for (EventLog.Event event : state.recentEvents()) {
            json.beginObject().name("seq").value(event.seq()).name("message").value(event.message()).endObject();
        }
        json.endArray();
    }

    private static void age(JsonWriter json, String name, long thenNanos, long nowNanos) {
        json.name(name);
        if (thenNanos == NEVER) {
            json.nullValue();
        } else {
            json.value(Math.max(0, nowNanos - thenNanos) / NANOS_PER_MILLI);
        }
    }
}
