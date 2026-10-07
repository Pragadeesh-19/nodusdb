package io.nodusdb.error;

public final class SchemaViolationException extends NodusException {

    private static final long serialVersionUID = 1L;

    public SchemaViolationException(String message) {
        super(ErrorCode.SCHEMA_VIOLATION, message);
    }
}
