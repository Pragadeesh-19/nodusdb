package io.nodusdb.kernel.adjacency;

import io.nodusdb.kernel.NodeIds;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeKeyTest {

    @Test
    void anUntypedKeyIsTheNodeIdItself() {
        long[] ids = {0, 1, 15, 16, 1 << 20, (int) NodeIds.MAX_NODE_ID};
        for (long id : ids) {
            assertEquals(id, EdgeKey.pack(0, 0, (int) id));
        }
    }

    @Test
    void everyFieldRoundTripsAtItsBoundaries() {
        int[] relations = {0, 1, 255, 256, 32_767, 32_768, 65_535};
        int[] ids = {0, 1, 1 << 30, (int) NodeIds.MAX_NODE_ID};
        for (int relation : relations) {
            for (int subjectRelation : relations) {
                for (int id : ids) {
                    long key = EdgeKey.pack(relation, subjectRelation, id);
                    assertEquals(relation, EdgeKey.relation(key));
                    assertEquals(subjectRelation, EdgeKey.subjectRelation(key));
                    assertEquals(id, EdgeKey.id(key));
                    assertTrue(key >= 0, "the sign bit stays clear");
                }
            }
        }
    }

    @Test
    void randomKeysRoundTrip() {
        Random random = new Random(3L);
        for (int i = 0; i < 100_000; i++) {
            int relation = random.nextInt(EdgeKey.MAX_RELATION + 1);
            int subjectRelation = random.nextInt(EdgeKey.MAX_RELATION + 1);
            int id = random.nextInt((int) NodeIds.MAX_NODE_ID + 1);
            long key = EdgeKey.pack(relation, subjectRelation, id);
            assertEquals(relation, EdgeKey.relation(key));
            assertEquals(subjectRelation, EdgeKey.subjectRelation(key));
            assertEquals(id, EdgeKey.id(key));
        }
    }

    @Test
    void keysOfDifferentRelationsAreDistinct() {
        assertNotEquals(EdgeKey.pack(1, 0, 5), EdgeKey.pack(2, 0, 5));
        assertNotEquals(EdgeKey.pack(1, 0, 5), EdgeKey.pack(1, 3, 5));
        assertNotEquals(EdgeKey.pack(1, 0, 5), EdgeKey.pack(1, 0, 6));
    }
}
