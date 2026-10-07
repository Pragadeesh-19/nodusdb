package io.nodusdb.error;

public enum ErrorCode {

    FAILURE(-1),
    LOOKUP_FAILED(-2),
    MEMORY_LIMIT(-3),
    STALE_READ(-4),
    CHECK_DEPTH(-5),
    SCHEMA_VIOLATION(-6),
    LOG_BACKLOG(-7),
    WRITER_FENCED(-8),
    CORRUPT_LOG(-9),
    UNSUPPORTED(-10),
    UPGRADE_REQUIRED(-11),
    INDETERMINATE(-12),
    TOKEN_LOST(-13);

    private final int value;

    ErrorCode(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
