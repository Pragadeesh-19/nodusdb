package io.nodusdb.log.record;

public final class RecordFormat {

    public static final int LENGTH_OFFSET = 0;
    public static final int TYPE_OFFSET = 4;
    public static final int FLAGS_OFFSET = 5;
    public static final int RESERVED_OFFSET = 6;
    public static final int LSN_OFFSET = 8;
    public static final int PAYLOAD_OFFSET = 16;
    public static final int CHECKSUM_BYTES = 4;
    public static final int ALIGNMENT = 8;

    public static final int FLAG_AUTOCOMMIT = 0x01;

    public static final int MIN_RECORD_BYTES = 32;
    public static final int READ_LIMIT_BYTES = 16 << 20;
    public static final int WRITE_LIMIT_BYTES = 1 << 20;

    public static final int CONFIG_BYTES = 32;
    public static final int CONFIG_KIND_OFFSET = 16;

    public static final int TUPLE_BYTES = 32;
    public static final int TUPLE_AUTOCOMMIT_BYTES = 40;
    public static final int TUPLE_OBJECT_OFFSET = 16;
    public static final int TUPLE_RELATION_OFFSET = 20;
    public static final int TUPLE_SUBJECT_RELATION_OFFSET = 22;
    public static final int TUPLE_SUBJECT_OFFSET = 24;
    public static final int TUPLE_COMMIT_TIME_OFFSET = 28;

    public static final int SYMBOL_ID_OFFSET = 16;
    public static final int SYMBOL_LENGTH_OFFSET = 20;
    public static final int SYMBOL_BYTES_OFFSET = 24;
    public static final int SYMBOL_FIXED_BYTES = 28;

    public static final int SCHEMA_VERSION_OFFSET = 16;
    public static final int SCHEMA_DIGEST_OFFSET = 20;
    public static final int SCHEMA_DIGEST_BYTES = 32;
    public static final int SCHEMA_DOCUMENT_LENGTH_OFFSET = 52;
    public static final int SCHEMA_RELATION_COUNT_OFFSET = 56;
    public static final int SCHEMA_RELATIONS_OFFSET = 60;
    public static final int SCHEMA_RELATION_BYTES = 12;

    public static final int COMMIT_BYTES = 48;
    public static final int COMMIT_FIRST_LSN_OFFSET = 16;
    public static final int COMMIT_COUNT_OFFSET = 24;
    public static final int COMMIT_RESERVED_OFFSET = 28;
    public static final int COMMIT_TIME_OFFSET = 32;

    public static final int EPOCH_BYTES = 48;
    public static final int EPOCH_NUMBER_OFFSET = 16;
    public static final int EPOCH_KEY_OFFSET = 24;
    public static final int EPOCH_RESERVED_OFFSET = 28;
    public static final int EPOCH_HANDOFF_OFFSET = 32;

    public static final int ERASE_BYTES = 56;
    public static final int ERASE_SYMBOL_OFFSET = 16;
    public static final int ERASE_RESERVED_OFFSET = 20;
    public static final int ERASE_PSEUDONYM_OFFSET = 24;
    public static final int ERASE_PSEUDONYM_BYTES = 24;

    public static final int MAX_RELATION = 0xFFFF;

    private RecordFormat() {
    }

    public static int align(int bytes) {
        return (bytes + ALIGNMENT - 1) & -ALIGNMENT;
    }

    public static int symbolBytes(int length) {
        return align(SYMBOL_FIXED_BYTES + length);
    }

    public static int schemaBytes(int relationCount, int documentLength) {
        return align(SCHEMA_RELATIONS_OFFSET + relationCount * SCHEMA_RELATION_BYTES + documentLength
                + CHECKSUM_BYTES);
    }
}
