package io.nodusdb.lake.model;

public record LakeRow(long keyHash, long[] longValues, int[] intValues, byte[][] varCharValues) {
}
