package io.nodusdb.stats;

import io.nodusdb.json.JsonWriter;

public final class WatchJson {

    private WatchJson() {
    }

    public static void write(JsonWriter json, WatchReport report) {
        json.name("watch").beginObject();
        json.name("enabled").value(report.enabled());
        json.name("buffer_changes").value(report.bufferChanges());
        json.name("serves_from_lsn").value(report.servesFromLsn());
        json.name("published_lsn").value(report.publishedLsn());
        json.name("fallback_readers").value(report.fallbackReaders());
        json.name("expired").value(report.expiredReads());
        json.endObject();
    }
}
