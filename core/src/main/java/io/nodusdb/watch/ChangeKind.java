package io.nodusdb.watch;

import io.nodusdb.kernel.changes.ChangeEntry;

public enum ChangeKind {

    ADD(1, "add"),
    REMOVE(2, "remove"),
    SCHEMA(3, "schema"),
    EPOCH(4, "epoch"),
    ERASE(5, "erase");

    private final int code;
    private final String event;

    ChangeKind(int code, String event) {
        this.code = code;
        this.event = event;
    }

    public int code() {
        return code;
    }

    public String event() {
        return event;
    }

    static ChangeKind ofEntry(int entryKind) {
        return switch (entryKind) {
            case ChangeEntry.ADD -> ADD;
            case ChangeEntry.REMOVE -> REMOVE;
            case ChangeEntry.SCHEMA -> SCHEMA;
            case ChangeEntry.EPOCH -> EPOCH;
            case ChangeEntry.ERASE -> ERASE;
            default -> throw new IllegalArgumentException("entry kind " + entryKind + " is not a change");
        };
    }
}
