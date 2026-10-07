package io.nodusdb.json;

import java.util.List;

public record JsonArray(List<JsonValue> items) implements JsonValue {

    public JsonArray {
        items = List.copyOf(items);
    }

    public int size() {
        return items.size();
    }

    public JsonValue get(int index) {
        return items.get(index);
    }
}
