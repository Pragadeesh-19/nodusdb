package io.nodusdb.capi;

import io.nodusdb.lake.ColumnarRows;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.word.WordFactory;

final class NativeColumns implements ColumnarRows {

    private final int rows;
    private final CLongPointer keys;
    private final CLongPointer[] longs;
    private final CIntPointer[] ints;
    private final CIntPointer[] offsets;
    private final CCharPointer[] data;

    NativeColumns(int rows, CLongPointer keys, int longCount, CLongPointer longAddresses,
                  int intCount, CLongPointer intAddresses,
                  int varCharCount, CLongPointer offsetAddresses, CLongPointer dataAddresses) {
        this.rows = rows;
        this.keys = keys;
        this.longs = new CLongPointer[longCount];
        for (int c = 0; c < longCount; c++) {
            longs[c] = WordFactory.pointer(longAddresses.read(c));
        }
        this.ints = new CIntPointer[intCount];
        for (int c = 0; c < intCount; c++) {
            ints[c] = WordFactory.pointer(intAddresses.read(c));
        }
        this.offsets = new CIntPointer[varCharCount];
        this.data = new CCharPointer[varCharCount];
        for (int c = 0; c < varCharCount; c++) {
            offsets[c] = WordFactory.pointer(offsetAddresses.read(c));
            data[c] = WordFactory.pointer(dataAddresses.read(c));
        }
    }

    @Override
    public int rowCount() {
        return rows;
    }

    @Override
    public int longColumnCount() {
        return longs.length;
    }

    @Override
    public int intColumnCount() {
        return ints.length;
    }

    @Override
    public int varCharColumnCount() {
        return offsets.length;
    }

    @Override
    public long key(int row) {
        return keys.read(row);
    }

    @Override
    public long longValue(int column, int row) {
        return longs[column].read(row);
    }

    @Override
    public int intValue(int column, int row) {
        return ints[column].read(row);
    }

    @Override
    public int varCharStart(int column, int row) {
        return offsets[column].read(row);
    }

    @Override
    public void copyVarChar(int column, int start, int length, byte[] destination, int destinationOffset) {
        CCharPointer source = data[column];
        for (int i = 0; i < length; i++) {
            destination[destinationOffset + i] = source.read(start + i);
        }
    }
}
