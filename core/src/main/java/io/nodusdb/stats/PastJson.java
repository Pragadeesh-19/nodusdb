package io.nodusdb.stats;

import io.nodusdb.json.JsonWriter;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.past.PastReport;
import io.nodusdb.kernel.past.PastSource;

public final class PastJson {

    private PastJson() {
    }

    public static String json(GraphKernel kernel, PastReport report) {
        JsonWriter json = new JsonWriter();
        json.beginObject();
        json.name("epoch").value(report.position().epoch());
        json.name("applied_lsn").value(report.position().lsn());
        json.name("as_of").beginObject();
        json.name("epoch").value(report.position().epoch());
        json.name("lsn").value(report.position().lsn());
        json.name("commit_micros").value(report.commitMicros());
        json.name("snapshot_lsn").value(report.snapshotLsn());
        json.name("replayed_objects").value(report.replayedObjects());
        json.name("replayed_records").value(report.replayedRecords());
        json.name("build_ms").value(report.buildMillis());
        json.name("memory_bytes").value(report.memoryBytes());
        json.endObject();
        ReachabilityJson.write(json, kernel.closureStats());
        json.endObject();
        return json.toString();
    }

    public static void writePool(JsonWriter json, PastSource.PoolReport pool) {
        json.name("history").beginObject();
        json.name("configured").value(pool.configured());
        json.name("views").value(pool.openViews());
        json.name("used_bytes").value(pool.usedBytes());
        json.name("limit_bytes").value(pool.limitBytes());
        json.endObject();
    }
}
