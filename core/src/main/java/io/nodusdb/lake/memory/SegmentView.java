package io.nodusdb.lake.memory;

import io.nodusdb.kernel.memory.NativeMemory;

import java.lang.foreign.MemorySegment;

public final class SegmentView {

    private MemorySegment segment = MemorySegment.NULL;
    private long address = NativeMemory.NOT_NATIVE;
    private long size;

    public SegmentView bind(MemorySegment target) {
        segment = target;
        address = NativeMemory.readableAddress(target);
        size = target.byteSize();
        return this;
    }

    public MemorySegment segment() {
        return segment;
    }

    public int getIntAtIndex(int index) {
        return NativeMemory.getInt(segment, address, size, (long) index << 2);
    }

    public long getLongAtIndex(int index) {
        return NativeMemory.getLong(segment, address, size, (long) index << 3);
    }

    public byte getByte(long offset) {
        return NativeMemory.getByte(segment, address, size, offset);
    }
}
