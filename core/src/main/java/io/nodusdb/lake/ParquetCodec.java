package io.nodusdb.lake;

import java.lang.foreign.MemorySegment;

public enum ParquetCodec {

    UNCOMPRESSED(0) {
        @Override
        MemorySegment compress(MemorySegment payload, NativeSink out, SnappyCompressor snappy) {
            return payload;
        }
    },

    SNAPPY(1) {
        @Override
        MemorySegment compress(MemorySegment payload, NativeSink out, SnappyCompressor snappy) {
            long start = out.length();
            snappy.compress(payload, out);
            return out.segment().asSlice(start);
        }
    };

    final int thriftId;

    ParquetCodec(int thriftId) {
        this.thriftId = thriftId;
    }

    abstract MemorySegment compress(MemorySegment payload, NativeSink out, SnappyCompressor snappy);
}
