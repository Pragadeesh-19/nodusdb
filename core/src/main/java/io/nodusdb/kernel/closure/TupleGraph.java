package io.nodusdb.kernel.closure;

import io.nodusdb.kernel.adjacency.EdgeTables;
import io.nodusdb.kernel.catalog.RelationCatalog;

public interface TupleGraph {

    EdgeTables direct();

    EdgeTables indirectOrNull();

    RelationCatalog catalog();

    int nodeCapacity();
}
