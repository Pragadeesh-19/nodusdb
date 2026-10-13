package io.nodusdb.stats;

import io.nodusdb.json.JsonWriter;
import io.nodusdb.kernel.closure.ClosureStats;

public final class ReachabilityJson {

    private ReachabilityJson() {
    }

    public static void write(JsonWriter json, ClosureStats stats) {
        json.name("reachability").beginObject();
        json.name("enabled").value(stats.enabled());
        json.name("state").value(stats.building() ? "building" : "current");
        json.name("dirty_nodes").value(stats.dirtyNodes());
        json.name("closure_entries").value(stats.entries());
        json.name("entry_cap").value(stats.entryCap());
        json.name("incremental_updates").value(stats.incrementalUpdates());
        json.name("dirty_marks").value(stats.dirtyMarks());
        json.name("rebuilt_nodes").value(stats.rebuiltNodes());
        json.name("refusals").value(stats.refusals());
        json.name("full_passes").value(stats.fullPasses());
        json.name("last_error").value(stats.failure());
        json.endObject();
    }
}
