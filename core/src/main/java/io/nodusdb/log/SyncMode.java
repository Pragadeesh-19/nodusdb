package io.nodusdb.log;

public enum SyncMode {

    ASYNC(0),
    SYNC(1);

    private final int code;

    SyncMode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static SyncMode fromCode(int code) {
        for (SyncMode mode : values()) {
            if (mode.code == code) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown sync mode code: " + code);
    }
}
