package io.nodusdb.lake;

import java.lang.foreign.MemorySegment;

final class UpsertArrays {

    private UpsertArrays() {
    }

    static boolean upsert(DeltaMemTable table, long keyHash, long[] longValues, int[] intValues,
                          byte[] varCharValues, int[] varCharLengths) {
        return table.upsert(keyHash, MemorySegment.ofArray(longValues), MemorySegment.ofArray(intValues),
                MemorySegment.ofArray(varCharValues), MemorySegment.ofArray(varCharLengths));
    }

    static void upsert(LakeTable table, long keyHash, long[] longValues, int[] intValues,
                       byte[] varCharValues, int[] varCharLengths) {
        table.upsert(keyHash, MemorySegment.ofArray(longValues), MemorySegment.ofArray(intValues),
                MemorySegment.ofArray(varCharValues), MemorySegment.ofArray(varCharLengths));
    }
}
