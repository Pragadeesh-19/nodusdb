package io.nodusdb.log.record;

public enum RecordType {

    GRAPH_CONFIG(0x01),
    TUPLE_ADD(0x10),
    TUPLE_REMOVE(0x11),
    SYMBOL(0x20),
    SCHEMA(0x30),
    TXN_COMMIT(0x40),
    EPOCH(0x50),
    ERASE(0x60);

    private static final RecordType[] BY_CODE = new RecordType[256];

    static {
        for (RecordType type : values()) {
            BY_CODE[type.code] = type;
        }
    }

    private final int code;

    RecordType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public boolean isTuple() {
        return this == TUPLE_ADD || this == TUPLE_REMOVE;
    }

    public static RecordType fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }
}
