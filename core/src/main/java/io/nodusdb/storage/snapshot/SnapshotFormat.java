package io.nodusdb.storage.snapshot;

final class SnapshotFormat {

    static final int MAGIC = 0x4E4F4453;
    static final short VERSION = 3;
    static final int HEADER_BYTES = 48;
    static final int HEADER_CHECKSUMMED_BYTES = 40;
    static final int SECTION_HEADER_BYTES = 20;
    static final int CHUNK_BYTES = 1 << 20;
    static final int SALT_BYTES = 32;
    static final int DIGEST_BYTES = 32;
    static final int RELATION_ENTRY_BYTES = 12;

    static final int KIND_CONFIG = 1;
    static final int KIND_SCHEMA = 2;
    static final int KIND_SYMBOLS = 3;
    static final int KIND_SALT = 4;
    static final int KIND_EPOCH_HISTORY = 5;
    static final int KIND_DIRECT_OUT = 6;
    static final int KIND_DIRECT_IN = 7;
    static final int KIND_INDIRECT_OUT = 8;
    static final int KIND_INDIRECT_IN = 9;
    static final int SECTION_COUNT = 9;

    private SnapshotFormat() {
    }
}
