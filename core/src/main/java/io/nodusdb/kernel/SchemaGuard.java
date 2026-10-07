package io.nodusdb.kernel;

import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.kernel.adjacency.EdgeKey;
import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.log.record.RecordReader;

final class SchemaGuard {

    private static final int PINNING_FLAGS = RelationCatalog.TUPLESET | RelationCatalog.RETIRED;

    private SchemaGuard() {
    }

    static RelationCatalog parse(RecordReader record) {
        int count = record.schemaRelationCount();
        int[] ids = new int[count];
        int[] types = new int[count];
        int[] names = new int[count];
        int[] flags = new int[count];
        for (int i = 0; i < count; i++) {
            ids[i] = record.schemaRelationId(i);
            types[i] = record.schemaRelationTypeSymbol(i);
            names[i] = record.schemaRelationNameSymbol(i);
            flags[i] = record.schemaRelationFlags(i);
        }
        return RelationCatalog.of(record.schemaVersion(), record.schemaDigest(), record.schemaDocument(), ids,
                types, names, flags);
    }

    static void requireApplicable(GraphState state, RelationCatalog current, RelationCatalog next) {
        String problem = current.evolutionProblem(next);
        if (problem != null) {
            throw new SchemaViolationException(problem);
        }
        boolean[] pinned = new boolean[EdgeKey.MAX_RELATION + 1];
        boolean any = false;
        for (int i = 0; i < next.relationCount(); i++) {
            int id = next.idAt(i);
            boolean changes = current.has(id) && ((current.flagsOf(id) ^ next.flagsAt(i)) & PINNING_FLAGS) != 0;
            pinned[id] = changes;
            any |= changes;
        }
        if (any) {
            int inUse = firstRelationInUse(state, pinned);
            if (inUse >= 0) {
                throw new SchemaViolationException("relation " + inUse + " has stored tuples, so it cannot change "
                        + "between a plain and a tupleset relation or be retired; remove its tuples first");
            }
        }
    }

    private static int firstRelationInUse(GraphState state, boolean[] pinned) {
        for (Partition partition : Partition.values()) {
            EdgeTables tables = state.tables(partition);
            if (tables == null) {
                continue;
            }
            for (int node = 0; node < tables.capacity(); node++) {
                int degree = tables.degree(node);
                for (int i = 0; i < degree; i++) {
                    int relation = EdgeKey.relation(tables.outgoingKeyAt(node, i));
                    if (pinned[relation]) {
                        return relation;
                    }
                }
            }
        }
        return -1;
    }
}
