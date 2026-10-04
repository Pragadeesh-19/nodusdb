package io.nodusdb.kernel;

public enum KeyKind {

    UNSET(0),
    INTEGER(1),
    STRING(2);

    private final int code;

    KeyKind(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static KeyKind fromCode(int code) {
        for (KeyKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown key kind code: " + code);
    }
}
