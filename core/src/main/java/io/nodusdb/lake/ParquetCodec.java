package io.nodusdb.lake;

public enum ParquetCodec {

    UNCOMPRESSED(0) {
        @Override
        byte[] compress(byte[] payload) {
            return payload;
        }
    },

    SNAPPY(1) {
        @Override
        byte[] compress(byte[] payload) {
            return SnappyCodec.compress(payload);
        }
    };

    final int thriftId;

    ParquetCodec(int thriftId) {
        this.thriftId = thriftId;
    }

    abstract byte[] compress(byte[] payload);
}
