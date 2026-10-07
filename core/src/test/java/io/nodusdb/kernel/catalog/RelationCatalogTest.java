package io.nodusdb.kernel.catalog;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationCatalogTest {

    private static final byte[] DIGEST = new byte[32];
    private static final byte[] DOCUMENT = {1, 2, 3};

    private static RelationCatalog catalog(int version, int[] ids, int[] flags) {
        int[] types = new int[ids.length];
        int[] names = new int[ids.length];
        for (int i = 0; i < ids.length; i++) {
            types[i] = 10 + i;
            names[i] = 20 + i;
        }
        return RelationCatalog.of(version, DIGEST, DOCUMENT, ids, types, names, flags);
    }

    @Test
    void theEmptyCatalogKnowsNoRelation() {
        assertEquals(0, RelationCatalog.EMPTY.version());
        assertEquals(0, RelationCatalog.EMPTY.relationCount());
        assertFalse(RelationCatalog.EMPTY.has(1));
        assertFalse(RelationCatalog.EMPTY.isTupleset(1));
    }

    @Test
    void flagsAreReadBackByRelationId() {
        RelationCatalog catalog = catalog(1, new int[] {1, 5, 9},
                new int[] {RelationCatalog.MEMBERSHIP, RelationCatalog.TUPLESET, RelationCatalog.RETIRED});

        assertTrue(catalog.has(5));
        assertFalse(catalog.has(2));
        assertFalse(catalog.has(0));
        assertFalse(catalog.has(10));
        assertEquals(RelationCatalog.MEMBERSHIP, catalog.flagsOf(1));
        assertTrue(catalog.isTupleset(5));
        assertFalse(catalog.isTupleset(1));
        assertTrue(catalog.isRetired(9));
        assertEquals(0, catalog.flagsOf(2));
    }

    @Test
    void anInvalidEntryIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> catalog(1, new int[] {0}, new int[] {0}));
        assertThrows(IllegalArgumentException.class, () -> catalog(1, new int[] {0x10000}, new int[] {0}));
        assertThrows(IllegalArgumentException.class, () -> catalog(1, new int[] {1}, new int[] {8}));
        assertThrows(IllegalArgumentException.class, () -> RelationCatalog.of(1, DIGEST, DOCUMENT, new int[] {1},
                new int[0], new int[1], new int[1]));
    }

    @Test
    void theCatalogDoesNotShareItsArraysWithTheCaller() {
        int[] ids = {1};
        byte[] document = {7};
        RelationCatalog catalog = RelationCatalog.of(1, DIGEST, document, ids, new int[1], new int[1], new int[1]);

        ids[0] = 4;
        document[0] = 9;
        catalog.document()[0] = 5;

        assertEquals(1, catalog.idAt(0));
        assertEquals(7, catalog.document()[0]);
    }

    @Test
    void anEvolutionMustAddOneVersionAndKeepEveryRelation() {
        RelationCatalog current = catalog(1, new int[] {1, 2}, new int[] {0, 0});

        assertNull(current.evolutionProblem(catalog(2, new int[] {1, 2, 3}, new int[] {0, 0, 0})));
        assertTrue(current.evolutionProblem(catalog(3, new int[] {1, 2}, new int[] {0, 0})).contains("version"));
        assertTrue(current.evolutionProblem(catalog(2, new int[] {1}, new int[] {0})).contains("missing"));
    }

    @Test
    void aRelationKeepsItsTypeAndNameAndCannotBeRevived() {
        RelationCatalog current = catalog(1, new int[] {1}, new int[] {RelationCatalog.RETIRED});
        RelationCatalog renamed = RelationCatalog.of(2, DIGEST, DOCUMENT, new int[] {1}, new int[] {10},
                new int[] {99}, new int[] {RelationCatalog.RETIRED});

        assertTrue(current.evolutionProblem(renamed).contains("type or name"));
        assertTrue(current.evolutionProblem(catalog(2, new int[] {1}, new int[] {0})).contains("revived"));
    }
}
