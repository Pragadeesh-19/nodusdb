package io.nodusdb.lake.parquet;

import java.util.Objects;

public record ColumnSpec(String name, ColumnType type, int fieldId, boolean unique) {

    public static final int NO_FIELD_ID = 0;

    public ColumnSpec {
        Objects.requireNonNull(type, "type");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("a column needs a name");
        }
        if (fieldId < NO_FIELD_ID) {
            throw new IllegalArgumentException("a field id must not be negative: " + fieldId);
        }
        if (unique && type.variableWidth()) {
            throw new IllegalArgumentException("only a fixed-width column can be marked unique: " + name);
        }
    }

    public static ColumnSpec of(String name, ColumnType type) {
        return new ColumnSpec(name, type, NO_FIELD_ID, false);
    }

    public static ColumnSpec withFieldId(String name, ColumnType type, int fieldId) {
        return new ColumnSpec(name, type, fieldId, false);
    }

    public boolean hasFieldId() {
        return fieldId != NO_FIELD_ID;
    }
}
