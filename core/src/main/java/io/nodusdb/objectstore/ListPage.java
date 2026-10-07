package io.nodusdb.objectstore;

import java.util.List;

public record ListPage(List<ObjectInfo> entries, boolean truncated) {

    public ListPage {
        entries = List.copyOf(entries);
    }

    public String lastKey() {
        return entries.isEmpty() ? "" : entries.get(entries.size() - 1).key();
    }
}
