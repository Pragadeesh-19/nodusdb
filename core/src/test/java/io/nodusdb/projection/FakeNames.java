package io.nodusdb.projection;

import java.util.HashMap;
import java.util.Map;

final class FakeNames implements NameResolver {

    final Map<Integer, String> symbols = new HashMap<>();
    final Map<Integer, String> relations = new HashMap<>();
    long epoch = 1;
    int version = 1;

    FakeNames symbol(int id, String text) {
        symbols.put(id, text);
        return this;
    }

    FakeNames relation(int id, String name) {
        relations.put(id, name);
        return this;
    }

    @Override
    public String symbol(int symbolId) {
        return symbols.getOrDefault(symbolId, UNKNOWN_PREFIX + symbolId);
    }

    @Override
    public String relation(int relationId) {
        return relations.getOrDefault(relationId, UNKNOWN_PREFIX + relationId);
    }

    @Override
    public long tenureEpoch(long lsn) {
        return epoch;
    }

    @Override
    public int schemaVersion() {
        return version;
    }
}
