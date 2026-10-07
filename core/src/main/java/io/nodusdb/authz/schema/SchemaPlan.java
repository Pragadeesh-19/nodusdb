package io.nodusdb.authz.schema;

import java.util.List;

public record SchemaPlan(SchemaDocument document, byte[] canonicalDocument, byte[] digest, List<String> newSymbols,
                         int[] relationIds, int[] typeSymbols, int[] nameSymbols, int[] flags, RelationIds ids) {

    public SchemaPlan {
        newSymbols = List.copyOf(newSymbols);
    }

    public int version() {
        return document.version();
    }
}
