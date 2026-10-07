package io.nodusdb.authz.schema;

import java.util.HashMap;
import java.util.Map;

public final class RelationIds {

    public static final int ABSENT = 0;

    private final Map<String, Integer> ids = new HashMap<>();

    public void put(String type, String relation, int id) {
        ids.put(key(type, relation), id);
    }

    public int idOf(String type, String relation) {
        return ids.getOrDefault(key(type, relation), ABSENT);
    }

    private static String key(String type, String relation) {
        return type + '\0' + relation;
    }
}
