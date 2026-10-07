package io.nodusdb.lake.model;

import io.nodusdb.lake.buffer.DeltaMemTable;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LakeSchemaTest {

    @Test
    void parseReadsEachColumnTypeInOrder() {
        LakeSchema schema = LakeSchema.parse("amount:INT64,score:DOUBLE,status:INT32,name:UTF8");

        assertEquals(List.of(
                new LakeSchema.Field("amount", LakeSchema.Type.INT64),
                new LakeSchema.Field("score", LakeSchema.Type.DOUBLE),
                new LakeSchema.Field("status", LakeSchema.Type.INT32),
                new LakeSchema.Field("name", LakeSchema.Type.UTF8)), schema.fields());
    }

    @Test
    void slotsNumberColumnsWithinEachStorageClass() {
        LakeSchema schema = LakeSchema.parse("a:INT64,b:UTF8,c:DOUBLE,d:INT32,e:UTF8");

        assertArrayEquals(new int[] {0, 0, 1, 0, 1}, schema.slots());
        assertEquals(new DeltaMemTable.Schema(2, 1, 2), schema.memtableSchema());
    }

    @Test
    void emptySpecIsKeysOnly() {
        assertEquals(LakeSchema.KEYS_ONLY, LakeSchema.parse(""));
    }

    @Test
    void rejectsUnknownTypesAndMalformedEntries() {
        assertThrows(IllegalArgumentException.class, () -> LakeSchema.parse("a:FLOAT"));
        assertThrows(IllegalArgumentException.class, () -> LakeSchema.parse("a"));
    }

    @Test
    void rejectsDuplicateAndReservedNames() {
        assertThrows(IllegalArgumentException.class, () -> LakeSchema.parse("a:INT64,a:INT32"));
        assertThrows(IllegalArgumentException.class, () -> LakeSchema.parse("key_hash:INT64"));
    }

    @Test
    void rejectsNamesOutsideTheParquetSafeSet() {
        assertThrows(IllegalArgumentException.class, () -> LakeSchema.parse("bad name:INT64"));
        assertThrows(IllegalArgumentException.class, () -> LakeSchema.parse("1st:INT64"));
    }
}
