package io.nodusdb.stats;

import io.nodusdb.json.JsonWriter;
import io.nodusdb.kernel.ErasureProgress;

import java.util.List;

public final class ErasureJson {

    private static final List<String> LAYERS = List.of("local", "bucket", "lake");

    private ErasureJson() {
    }

    public static void write(JsonWriter json, ErasureReport report) {
        json.name("erasure").beginObject();
        json.name("erased_symbols").value(report.erasedSymbols());
        json.name("newest_erase_lsn").value(report.newestEraseLsn());
        for (String name : LAYERS) {
            layer(json, name, report.layers());
        }
        json.name("last_error").value(report.lastError());
        json.endObject();
    }

    private static void layer(JsonWriter json, String name, List<ErasureProgress.Layer> layers) {
        json.name(name).beginObject();
        ErasureProgress.Layer found = null;
        for (ErasureProgress.Layer layer : layers) {
            if (layer.name().equals(name)) {
                found = layer;
            }
        }
        json.name("configured").value(found != null);
        if (found != null) {
            json.name("covered_lsn").value(found.coveredLsn());
            json.name("pending").value(found.pending());
        }
        json.endObject();
    }
}
