package io.nodusdb.projection;

import io.nodusdb.kernel.EpochHistory;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.symbols.SymbolTable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public final class KernelNameResolver implements NameResolver {

    private final GraphKernel kernel;
    private RelationCatalog cachedCatalog;
    private Map<Integer, String> relationNames = Map.of();

    public KernelNameResolver(GraphKernel kernel) {
        this.kernel = kernel;
    }

    @Override
    public String symbol(int symbolId) {
        SymbolTable symbols = kernel.symbols();
        if (symbolId < 0 || symbolId >= symbols.size()) {
            return UNKNOWN_PREFIX + symbolId;
        }
        return new String(symbols.resolve(symbolId), StandardCharsets.UTF_8);
    }

    @Override
    public synchronized String relation(int relationId) {
        RelationCatalog catalog = kernel.catalog();
        if (catalog != cachedCatalog) {
            Map<Integer, String> names = new HashMap<>();
            for (int i = 0; i < catalog.relationCount(); i++) {
                names.put(catalog.idAt(i), symbol(catalog.nameSymbolAt(i)));
            }
            relationNames = names;
            cachedCatalog = catalog;
        }
        return relationNames.getOrDefault(relationId, UNKNOWN_PREFIX + relationId);
    }

    @Override
    public long tenureEpoch(long lsn) {
        EpochHistory history = kernel.epochHistory();
        long epoch = 0;
        for (int i = 0; i < history.size(); i++) {
            EpochHistory.Tenure tenure = history.tenureAt(i);
            if (tenure.firstLsn() <= lsn) {
                epoch = tenure.epoch();
            }
        }
        return epoch;
    }

    @Override
    public int schemaVersion() {
        return kernel.catalog().version();
    }
}
