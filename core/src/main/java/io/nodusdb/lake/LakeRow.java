package io.nodusdb.lake;

public record LakeRow(long keyHash, long[] longValues, int[] intValues, byte[][] varCharValues) {
}
