package io.nodusdb.lake;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public record LakeSchema(List<Field> fields) {

    public enum Type {
        INT64, DOUBLE, INT32, UTF8
    }

    public static final String KEY_COLUMN = "key_hash";

    public static final LakeSchema KEYS_ONLY = new LakeSchema(List.of());

    private static final Pattern NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    public record Field(String name, Type type) {

        public Field {
            Objects.requireNonNull(type, "type");
            if (name == null || !NAME.matcher(name).matches()) {
                throw new IllegalArgumentException("column name must match [A-Za-z_][A-Za-z0-9_]*: " + name);
            }
        }
    }

    public LakeSchema {
        fields = List.copyOf(fields);
        Set<String> seen = new HashSet<>();
        seen.add(KEY_COLUMN);
        for (Field field : fields) {
            if (!seen.add(field.name())) {
                throw new IllegalArgumentException("duplicate column name: " + field.name());
            }
        }
    }

    public static LakeSchema parse(String spec) {
        if (spec.isEmpty()) {
            return new LakeSchema(List.of());
        }
        List<Field> fields = new ArrayList<>();
        for (String entry : spec.split(",", -1)) {
            int separator = entry.indexOf(':');
            if (separator < 0) {
                throw new IllegalArgumentException("column must be name:TYPE: " + entry);
            }
            fields.add(new Field(entry.substring(0, separator), Type.valueOf(entry.substring(separator + 1))));
        }
        return new LakeSchema(fields);
    }

    public DeltaMemTable.Schema memtableSchema() {
        int longs = 0;
        int ints = 0;
        int vars = 0;
        for (Field field : fields) {
            switch (field.type()) {
                case INT64, DOUBLE -> longs++;
                case INT32 -> ints++;
                case UTF8 -> vars++;
            }
        }
        return new DeltaMemTable.Schema(longs, ints, vars);
    }

    public int[] slots() {
        int[] slots = new int[fields.size()];
        int longs = 0;
        int ints = 0;
        int vars = 0;
        for (int i = 0; i < fields.size(); i++) {
            slots[i] = switch (fields.get(i).type()) {
                case INT64, DOUBLE -> longs++;
                case INT32 -> ints++;
                case UTF8 -> vars++;
            };
        }
        return slots;
    }

    String signature() {
        StringBuilder signature = new StringBuilder();
        for (Field field : fields) {
            signature.append(field.name()).append(':').append(field.type()).append(';');
        }
        return signature.toString();
    }
}
