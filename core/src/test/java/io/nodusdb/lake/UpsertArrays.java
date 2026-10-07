package io.nodusdb.lake;

import io.nodusdb.lake.buffer.DeltaMemTable;

import java.lang.foreign.MemorySegment;

public final class UpsertArrays {

    private UpsertArrays() {
    }

    public static boolean upsert(DeltaMemTable table, long keyHash, long[] longValues, int[] intValues,
                          byte[] varCharValues, int[] varCharLengths) {
        return table.upsert(keyHash, MemorySegment.ofArray(longValues), MemorySegment.ofArray(intValues),
                MemorySegment.ofArray(varCharValues), MemorySegment.ofArray(varCharLengths));
    }

    public static void upsert(LakeTable table, long keyHash, long[] longValues, int[] intValues,
                       byte[] varCharValues, int[] varCharLengths) {
        table.upsert(keyHash, MemorySegment.ofArray(longValues), MemorySegment.ofArray(intValues),
                MemorySegment.ofArray(varCharValues), MemorySegment.ofArray(varCharLengths));
    }
}
