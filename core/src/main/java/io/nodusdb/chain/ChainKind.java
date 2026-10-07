package io.nodusdb.chain;

public enum ChainKind {

    RECORDS(1),
    SNAPSHOT_REF(2),
    REDACTION(3);

    private final int code;

    ChainKind(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static ChainKind fromCode(int code) {
        for (ChainKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        return null;
    }
}
