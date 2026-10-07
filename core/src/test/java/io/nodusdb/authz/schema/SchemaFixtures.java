package io.nodusdb.authz.schema;

import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.memory.MemoryBudget;
import io.nodusdb.kernel.symbols.SymbolTable;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;

public final class SchemaFixtures {

    public static final String DOCUMENTS = """
            schema 1
            type user
            type group {
              relation member: user | group#member
            }
            type folder {
              relation viewer: user | group#member
              relation parent: folder
              permission view = viewer + parent->view
            }
            type document {
              relation parent: folder
              relation editor: user | group#member
              relation viewer: user | group#member
              permission view = viewer + editor + parent->view
            }
            """;

    private SchemaFixtures() {
    }

    public static SymbolTable newSymbols() {
        return new SymbolTable(Arena.ofAuto(), MemoryBudget.unlimited());
    }

    public static RelationCatalog install(SchemaPlan plan, SymbolTable symbols) {
        for (String name : plan.newSymbols()) {
            byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
            symbols.append(symbols.size(), bytes, 0, bytes.length);
        }
        return RelationCatalog.of(plan.version(), plan.digest(), plan.canonicalDocument(), plan.relationIds(),
                plan.typeSymbols(), plan.nameSymbols(), plan.flags());
    }
}
