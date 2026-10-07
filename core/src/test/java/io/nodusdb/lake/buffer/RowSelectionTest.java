package io.nodusdb.lake.buffer;

import io.nodusdb.lake.UpsertArrays;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RowSelectionTest {

    @Test
    void emptySelectionHasNoRows() {
        try (RowSelection selection = RowSelection.of()) {
            assertEquals(0, selection.size());
        }
    }

    @Test
    void valuesKeepTheirOrder() {
        try (RowSelection selection = RowSelection.of(5, 1, 9)) {
            assertEquals(3, selection.size());
            assertEquals(5, selection.rowAt(0));
            assertEquals(1, selection.rowAt(1));
            assertEquals(9, selection.rowAt(2));
        }
    }

    @Test
    void kindSelectionListsMatchingRowsInMemtableOrder() {
        DeltaMemTable table = new DeltaMemTable(new DeltaMemTable.Schema(1, 0, 0), 4, 16);
        UpsertArrays.upsert(table, 1L, new long[] {10L}, new int[0], new byte[0], new int[0]);
        table.tombstone(2L);
        UpsertArrays.upsert(table, 3L, new long[] {30L}, new int[0], new byte[0], new int[0]);
        table.tombstone(4L);
        try (RowSelection inserts = RowSelection.ofKind(table, DeltaMemTable.INSERT);
             RowSelection tombstones = RowSelection.ofKind(table, DeltaMemTable.TOMBSTONE)) {
            assertEquals(2, inserts.size());
            assertEquals(0, inserts.rowAt(0));
            assertEquals(2, inserts.rowAt(1));
            assertEquals(2, tombstones.size());
            assertEquals(1, tombstones.rowAt(0));
            assertEquals(3, tombstones.rowAt(1));
        }
        table.close();
    }
}
