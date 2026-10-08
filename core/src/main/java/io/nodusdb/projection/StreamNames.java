package io.nodusdb.projection;

import java.util.HashMap;
import java.util.Map;

final class StreamNames {

    private final NameResolver resolver;
    private final Map<Integer, String> symbols = new HashMap<>();
    private final Map<Integer, String> relations = new HashMap<>();
    private final Map<Integer, String> erased = new HashMap<>();

    StreamNames(NameResolver resolver) {
        this.resolver = resolver;
    }

    String symbol(int id) {
        String pseudonym = erased.get(id);
        if (pseudonym != null) {
            return pseudonym;
        }
        String defined = symbols.get(id);
        return defined != null ? defined : resolver.symbol(id);
    }

    String relation(int id) {
        String defined = relations.get(id);
        return defined != null ? defined : resolver.relation(id);
    }

    void defineSymbol(int id, String text) {
        symbols.put(id, text);
    }

    void defineRelation(int id, String name) {
        relations.put(id, name);
    }

    void erase(int id, String pseudonym) {
        erased.put(id, pseudonym);
        symbols.remove(id);
    }

    void forgetDefinitions() {
        symbols.clear();
        relations.clear();
    }
}
