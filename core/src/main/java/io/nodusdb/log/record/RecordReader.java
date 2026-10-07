package io.nodusdb.log.record;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static io.nodusdb.log.record.RecordFormat.CHECKSUM_BYTES;
import static io.nodusdb.log.record.RecordFormat.COMMIT_BYTES;
import static io.nodusdb.log.record.RecordFormat.COMMIT_COUNT_OFFSET;
import static io.nodusdb.log.record.RecordFormat.COMMIT_FIRST_LSN_OFFSET;
import static io.nodusdb.log.record.RecordFormat.COMMIT_RESERVED_OFFSET;
import static io.nodusdb.log.record.RecordFormat.COMMIT_TIME_OFFSET;
import static io.nodusdb.log.record.RecordFormat.CONFIG_BYTES;
import static io.nodusdb.log.record.RecordFormat.CONFIG_KIND_OFFSET;
import static io.nodusdb.log.record.RecordFormat.EPOCH_BYTES;
import static io.nodusdb.log.record.RecordFormat.EPOCH_HANDOFF_OFFSET;
import static io.nodusdb.log.record.RecordFormat.EPOCH_KEY_OFFSET;
import static io.nodusdb.log.record.RecordFormat.EPOCH_NUMBER_OFFSET;
import static io.nodusdb.log.record.RecordFormat.EPOCH_RESERVED_OFFSET;
import static io.nodusdb.log.record.RecordFormat.ERASE_BYTES;
import static io.nodusdb.log.record.RecordFormat.ERASE_PSEUDONYM_BYTES;
import static io.nodusdb.log.record.RecordFormat.ERASE_PSEUDONYM_OFFSET;
import static io.nodusdb.log.record.RecordFormat.ERASE_RESERVED_OFFSET;
import static io.nodusdb.log.record.RecordFormat.ERASE_SYMBOL_OFFSET;
import static io.nodusdb.log.record.RecordFormat.FLAGS_OFFSET;
import static io.nodusdb.log.record.RecordFormat.FLAG_AUTOCOMMIT;
import static io.nodusdb.log.record.RecordFormat.LENGTH_OFFSET;
import static io.nodusdb.log.record.RecordFormat.LSN_OFFSET;
import static io.nodusdb.log.record.RecordFormat.MIN_RECORD_BYTES;
import static io.nodusdb.log.record.RecordFormat.READ_LIMIT_BYTES;
import static io.nodusdb.log.record.RecordFormat.RESERVED_OFFSET;
import static io.nodusdb.log.record.RecordFormat.SCHEMA_DIGEST_BYTES;
import static io.nodusdb.log.record.RecordFormat.SCHEMA_DIGEST_OFFSET;
import static io.nodusdb.log.record.RecordFormat.SCHEMA_DOCUMENT_LENGTH_OFFSET;
import static io.nodusdb.log.record.RecordFormat.SCHEMA_RELATIONS_OFFSET;
import static io.nodusdb.log.record.RecordFormat.SCHEMA_RELATION_BYTES;
import static io.nodusdb.log.record.RecordFormat.SCHEMA_RELATION_COUNT_OFFSET;
import static io.nodusdb.log.record.RecordFormat.SCHEMA_VERSION_OFFSET;
import static io.nodusdb.log.record.RecordFormat.SYMBOL_BYTES_OFFSET;
import static io.nodusdb.log.record.RecordFormat.SYMBOL_ID_OFFSET;
import static io.nodusdb.log.record.RecordFormat.SYMBOL_LENGTH_OFFSET;
import static io.nodusdb.log.record.RecordFormat.TUPLE_AUTOCOMMIT_BYTES;
import static io.nodusdb.log.record.RecordFormat.TUPLE_BYTES;
import static io.nodusdb.log.record.RecordFormat.TUPLE_COMMIT_TIME_OFFSET;
import static io.nodusdb.log.record.RecordFormat.TUPLE_OBJECT_OFFSET;
import static io.nodusdb.log.record.RecordFormat.TUPLE_RELATION_OFFSET;
import static io.nodusdb.log.record.RecordFormat.TUPLE_SUBJECT_OFFSET;
import static io.nodusdb.log.record.RecordFormat.TUPLE_SUBJECT_RELATION_OFFSET;
import static io.nodusdb.log.record.RecordFormat.TYPE_OFFSET;

public final class RecordReader {

    private static final int RELATION_FLAGS_MASK = 0x7;

    private final CRC32C crc = new CRC32C();
    private ByteBuffer buffer;
    private int start;
    private int end;
    private int position;
    private String reason = "";

    public RecordReader wrap(ByteBuffer buffer, int start, int end) {
        this.buffer = buffer;
        this.start = start;
        this.end = end;
        this.position = start;
        return this;
    }

    public boolean hasRecord() {
        return position < end;
    }

    public int position() {
        return position;
    }

    public int endOfInput() {
        return end;
    }

    public void advance() {
        position += length();
    }

    public void copyRecordTo(ByteBuffer destination, int offset) {
        buffer.get(position, destination.array(), destination.arrayOffset() + offset, length());
    }

    public void seek(int newPosition) {
        position = newPosition;
    }

    public void rewind() {
        position = start;
    }

    public String invalidReason() {
        return reason;
    }

    public Verdict inspect() {
        int remaining = end - position;
        if (remaining < Integer.BYTES) {
            return Verdict.INCOMPLETE;
        }
        int length = buffer.getInt(position + LENGTH_OFFSET);
        if (length < MIN_RECORD_BYTES || length > READ_LIMIT_BYTES || (length & (RecordFormat.ALIGNMENT - 1)) != 0) {
            return invalid("record length " + length + " is out of range");
        }
        if (remaining < length) {
            return Verdict.INCOMPLETE;
        }
        RecordType type = RecordType.fromCode(buffer.get(position + TYPE_OFFSET) & 0xFF);
        if (type == null) {
            return invalid("unknown record type " + (buffer.get(position + TYPE_OFFSET) & 0xFF));
        }
        int flags = buffer.get(position + FLAGS_OFFSET) & 0xFF;
        if ((flags & ~FLAG_AUTOCOMMIT) != 0 || (flags != 0 && !type.isTuple())) {
            return invalid("flags " + flags + " are not valid for " + type);
        }
        if (buffer.getShort(position + RESERVED_OFFSET) != 0) {
            return invalid("reserved bytes of the envelope are not zero");
        }
        int checksumAt = position + length - CHECKSUM_BYTES;
        if (Checksums.crc32c(crc, buffer, position, checksumAt) != buffer.getInt(checksumAt)) {
            return invalid("checksum mismatch");
        }
        String problem = structureProblem(type, flags, length);
        return problem == null ? Verdict.VALID : invalid(problem);
    }

    public int length() {
        return buffer.getInt(position + LENGTH_OFFSET);
    }

    public RecordType type() {
        return RecordType.fromCode(buffer.get(position + TYPE_OFFSET) & 0xFF);
    }

    public long lsn() {
        return buffer.getLong(position + LSN_OFFSET);
    }

    public boolean autocommit() {
        return (buffer.get(position + FLAGS_OFFSET) & FLAG_AUTOCOMMIT) != 0;
    }

    public boolean isCommitPoint() {
        return type() == RecordType.TXN_COMMIT || autocommit();
    }

    public int object() {
        return buffer.getInt(position + TUPLE_OBJECT_OFFSET);
    }

    public int relation() {
        return buffer.getShort(position + TUPLE_RELATION_OFFSET) & 0xFFFF;
    }

    public int subjectRelation() {
        return buffer.getShort(position + TUPLE_SUBJECT_RELATION_OFFSET) & 0xFFFF;
    }

    public int subject() {
        return buffer.getInt(position + TUPLE_SUBJECT_OFFSET);
    }

    public long commitMicros() {
        return type() == RecordType.TXN_COMMIT
                ? buffer.getLong(position + COMMIT_TIME_OFFSET)
                : buffer.getLong(position + TUPLE_COMMIT_TIME_OFFSET);
    }

    public long commitFirstLsn() {
        return buffer.getLong(position + COMMIT_FIRST_LSN_OFFSET);
    }

    public int commitRecordCount() {
        return buffer.getInt(position + COMMIT_COUNT_OFFSET);
    }

    public int keyKindCode() {
        return buffer.get(position + CONFIG_KIND_OFFSET);
    }

    public int symbolId() {
        return buffer.getInt(position + SYMBOL_ID_OFFSET);
    }

    public int symbolLength() {
        return buffer.getInt(position + SYMBOL_LENGTH_OFFSET);
    }

    public byte[] symbolBytes() {
        byte[] bytes = new byte[symbolLength()];
        buffer.get(position + SYMBOL_BYTES_OFFSET, bytes);
        return bytes;
    }

    public int schemaVersion() {
        return buffer.getInt(position + SCHEMA_VERSION_OFFSET);
    }

    public byte[] schemaDigest() {
        byte[] digest = new byte[SCHEMA_DIGEST_BYTES];
        buffer.get(position + SCHEMA_DIGEST_OFFSET, digest);
        return digest;
    }

    public int schemaRelationCount() {
        return buffer.getInt(position + SCHEMA_RELATION_COUNT_OFFSET);
    }

    public int schemaRelationId(int index) {
        return buffer.getShort(relationEntry(index)) & 0xFFFF;
    }

    public int schemaRelationTypeSymbol(int index) {
        return buffer.getInt(relationEntry(index) + 2);
    }

    public int schemaRelationNameSymbol(int index) {
        return buffer.getInt(relationEntry(index) + 6);
    }

    public int schemaRelationFlags(int index) {
        return buffer.getShort(relationEntry(index) + 10) & 0xFFFF;
    }

    public byte[] schemaDocument() {
        byte[] document = new byte[buffer.getInt(position + SCHEMA_DOCUMENT_LENGTH_OFFSET)];
        buffer.get(relationEntry(schemaRelationCount()), document);
        return document;
    }

    public long epochNumber() {
        return buffer.getLong(position + EPOCH_NUMBER_OFFSET);
    }

    public int epochWriterKey() {
        return buffer.getInt(position + EPOCH_KEY_OFFSET);
    }

    public long epochHandoffLsn() {
        return buffer.getLong(position + EPOCH_HANDOFF_OFFSET);
    }

    public int eraseSymbolId() {
        return buffer.getInt(position + ERASE_SYMBOL_OFFSET);
    }

    public byte[] erasePseudonym() {
        byte[] pseudonym = new byte[ERASE_PSEUDONYM_BYTES];
        buffer.get(position + ERASE_PSEUDONYM_OFFSET, pseudonym);
        return pseudonym;
    }

    private int relationEntry(int index) {
        return position + SCHEMA_RELATIONS_OFFSET + index * SCHEMA_RELATION_BYTES;
    }

    private Verdict invalid(String message) {
        reason = message;
        return Verdict.INVALID;
    }

    private String structureProblem(RecordType type, int flags, int length) {
        return switch (type) {
            case TUPLE_ADD, TUPLE_REMOVE -> tupleProblem(flags, length);
            case GRAPH_CONFIG -> configProblem(length);
            case SYMBOL -> symbolProblem(length);
            case SCHEMA -> schemaProblem(length);
            case TXN_COMMIT -> commitProblem(length);
            case EPOCH -> epochProblem(length);
            case ERASE -> eraseProblem(length);
        };
    }

    private String tupleProblem(int flags, int length) {
        int expected = flags == 0 ? TUPLE_BYTES : TUPLE_AUTOCOMMIT_BYTES;
        if (length != expected) {
            return "tuple record has length " + length + ", expected " + expected;
        }
        if (object() < 0 || subject() < 0) {
            return "tuple names a negative node";
        }
        return null;
    }

    private String configProblem(int length) {
        int kind = keyKindCode();
        if (length != CONFIG_BYTES || (kind != 1 && kind != 2)) {
            return "graph config has an unknown key kind or length";
        }
        return zeroed(CONFIG_KIND_OFFSET + 1, length - CHECKSUM_BYTES) ? null : "graph config padding is not zero";
    }

    private String symbolProblem(int length) {
        int size = symbolLength();
        if (symbolId() < 0 || size < 0 || size > length || length != RecordFormat.symbolBytes(size)) {
            return "symbol record has an inconsistent length";
        }
        return zeroed(SYMBOL_BYTES_OFFSET + size, length - CHECKSUM_BYTES) ? null : "symbol padding is not zero";
    }

    private String schemaProblem(int length) {
        int relations = schemaRelationCount();
        int documentLength = buffer.getInt(position + SCHEMA_DOCUMENT_LENGTH_OFFSET);
        if (relations < 0 || documentLength < 0 || schemaVersion() < 0
                || (long) length != schemaBytesChecked(relations, documentLength)) {
            return "schema record has an inconsistent length";
        }
        for (int i = 0; i < relations; i++) {
            if ((schemaRelationFlags(i) & ~RELATION_FLAGS_MASK) != 0) {
                return "schema relation " + i + " has unknown flags";
            }
        }
        int documentEnd = relationEntry(relations) + documentLength;
        return zeroed(documentEnd, length - CHECKSUM_BYTES) ? null : "schema padding is not zero";
    }

    private static long schemaBytesChecked(int relations, int documentLength) {
        long raw = SCHEMA_RELATIONS_OFFSET + (long) relations * SCHEMA_RELATION_BYTES + documentLength
                + CHECKSUM_BYTES;
        return (raw + RecordFormat.ALIGNMENT - 1) & -(long) RecordFormat.ALIGNMENT;
    }

    private String commitProblem(int length) {
        if (length != COMMIT_BYTES || commitRecordCount() < 1 || commitFirstLsn() < 1) {
            return "commit record is malformed";
        }
        if (buffer.getInt(position + COMMIT_RESERVED_OFFSET) != 0
                || !zeroed(COMMIT_TIME_OFFSET + Long.BYTES, length - CHECKSUM_BYTES)) {
            return "commit record padding is not zero";
        }
        return null;
    }

    private String epochProblem(int length) {
        if (length != EPOCH_BYTES || epochNumber() < 1) {
            return "epoch record is malformed";
        }
        if (buffer.getInt(position + EPOCH_RESERVED_OFFSET) != 0
                || !zeroed(EPOCH_HANDOFF_OFFSET + Long.BYTES, length - CHECKSUM_BYTES)) {
            return "epoch record padding is not zero";
        }
        return null;
    }

    private String eraseProblem(int length) {
        if (length != ERASE_BYTES || eraseSymbolId() < 0) {
            return "erase record is malformed";
        }
        if (buffer.getInt(position + ERASE_RESERVED_OFFSET) != 0
                || !zeroed(ERASE_PSEUDONYM_OFFSET + ERASE_PSEUDONYM_BYTES, length - CHECKSUM_BYTES)) {
            return "erase record padding is not zero";
        }
        return null;
    }

    private boolean zeroed(int fromOffset, int toOffset) {
        for (int offset = fromOffset; offset < toOffset; offset++) {
            if (buffer.get(position + offset) != 0) {
                return false;
            }
        }
        return true;
    }
}
