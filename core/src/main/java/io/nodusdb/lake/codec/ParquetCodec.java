package io.nodusdb.lake.codec;

import java.lang.foreign.MemorySegment;

public enum ParquetCodec {

    UNCOMPRESSED(0) {
        @Override
        public MemorySegment compress(MemorySegment payload, NativeSink out, SnappyCompressor snappy) {
            return payload;
        }
    },

    SNAPPY(1) {
        @Override
        public MemorySegment compress(MemorySegment payload, NativeSink out, SnappyCompressor snappy) {
            long start = out.length();
            snappy.compress(payload, out);
            return out.segment().asSlice(start);
        }
    };

    public final int thriftId;

    ParquetCodec(int thriftId) {
        this.thriftId = thriftId;
    }

    public abstract MemorySegment compress(MemorySegment payload, NativeSink out, SnappyCompressor snappy);
}
