package io.nodusdb.kernel.catalog;

import java.util.Arrays;

public final class RelationCatalog {

    public static final int MEMBERSHIP = 1;
    public static final int TUPLESET = 2;
    public static final int RETIRED = 4;
    public static final int KNOWN_FLAGS = MEMBERSHIP | TUPLESET | RETIRED;

    public static final RelationCatalog EMPTY = new RelationCatalog(0, new byte[32], new byte[0], new int[0],
            new int[0], new int[0], new int[0]);

    private static final int ABSENT = -1;

    private final int version;
    private final byte[] digest;
    private final byte[] document;
    private final int[] ids;
    private final int[] typeSymbols;
    private final int[] nameSymbols;
    private final int[] flags;
    private final int[] flagsById;

    private RelationCatalog(int version, byte[] digest, byte[] document, int[] ids, int[] typeSymbols,
                            int[] nameSymbols, int[] flags) {
        this.version = version;
        this.digest = digest;
        this.document = document;
        this.ids = ids;
        this.typeSymbols = typeSymbols;
        this.nameSymbols = nameSymbols;
        this.flags = flags;
        this.flagsById = indexFlags(ids, flags);
    }

    public static RelationCatalog of(int version, byte[] digest, byte[] document, int[] ids, int[] typeSymbols,
                                     int[] nameSymbols, int[] flags) {
        int count = ids.length;
        if (typeSymbols.length != count || nameSymbols.length != count || flags.length != count) {
            throw new IllegalArgumentException("relation columns differ in length");
        }
        for (int i = 0; i < count; i++) {
            if (ids[i] < 1 || ids[i] > 0xFFFF || (flags[i] & ~KNOWN_FLAGS) != 0) {
                throw new IllegalArgumentException("relation entry " + i + " is invalid");
            }
        }
        return new RelationCatalog(version, digest.clone(), document.clone(), ids.clone(), typeSymbols.clone(),
                nameSymbols.clone(), flags.clone());
    }

    public int version() {
        return version;
    }

    public byte[] digest() {
        return digest.clone();
    }

    public byte[] document() {
        return document.clone();
    }

    public int relationCount() {
        return ids.length;
    }

    public int idAt(int index) {
        return ids[index];
    }

    public int typeSymbolAt(int index) {
        return typeSymbols[index];
    }

    public int nameSymbolAt(int index) {
        return nameSymbols[index];
    }

    public int flagsAt(int index) {
        return flags[index];
    }

    public boolean has(int relation) {
        return relation >= 0 && relation < flagsById.length && flagsById[relation] != ABSENT;
    }

    public int flagsOf(int relation) {
        return has(relation) ? flagsById[relation] : 0;
    }

    public boolean isTupleset(int relation) {
        return (flagsOf(relation) & TUPLESET) != 0;
    }

    public boolean isRetired(int relation) {
        return (flagsOf(relation) & RETIRED) != 0;
    }

    public String evolutionProblem(RelationCatalog next) {
        if (next.version != version + 1) {
            return "schema version " + next.version + " does not follow version " + version;
        }
        for (int i = 0; i < ids.length; i++) {
            int index = next.indexOf(ids[i]);
            if (index < 0) {
                return "relation " + ids[i] + " is missing from the new schema";
            }
            if (next.typeSymbols[index] != typeSymbols[i] || next.nameSymbols[index] != nameSymbols[i]) {
                return "relation " + ids[i] + " changed its type or name";
            }
            boolean wasRetired = (flags[i] & RETIRED) != 0;
            if (wasRetired && (next.flags[index] & RETIRED) == 0) {
                return "relation " + ids[i] + " cannot be revived once retired";
            }
        }
        return null;
    }

    private int indexOf(int relation) {
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] == relation) {
                return i;
            }
        }
        return -1;
    }

    private static int[] indexFlags(int[] ids, int[] flags) {
        int max = 0;
        for (int id : ids) {
            max = Math.max(max, id);
        }
        int[] byId = new int[ids.length == 0 ? 0 : max + 1];
        Arrays.fill(byId, ABSENT);
        for (int i = 0; i < ids.length; i++) {
            byId[ids[i]] = flags[i];
        }
        return byId;
    }
}
