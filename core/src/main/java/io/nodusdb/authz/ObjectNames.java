package io.nodusdb.authz;

import io.nodusdb.authz.schema.CompiledSchema;
import io.nodusdb.error.SchemaViolationException;

final class ObjectNames {

    private static final char SEPARATOR = ':';

    private ObjectNames() {
    }

    static int typeIndex(CompiledSchema model, String name) {
        int separator = name.indexOf(SEPARATOR);
        if (separator <= 0 || separator == name.length() - 1) {
            throw new SchemaViolationException("'" + name + "' must be written as type:id");
        }
        String type = name.substring(0, separator);
        int index = model.typeIndex(type);
        if (index == CompiledSchema.NO_DEF) {
            throw new SchemaViolationException("'" + name + "' names the unknown type '" + type + "'");
        }
        return index;
    }

    static void requireSchema(CompiledSchema model) {
        if (model.version() == 0) {
            throw new SchemaViolationException("apply a schema before using tuples");
        }
    }
}
