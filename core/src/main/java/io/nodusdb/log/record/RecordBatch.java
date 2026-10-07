package io.nodusdb.log.record;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;

import static io.nodusdb.log.record.RecordFormat.CHECKSUM_BYTES;
import static io.nodusdb.log.record.RecordFormat.COMMIT_BYTES;
import static io.nodusdb.log.record.RecordFormat.COMMIT_COUNT_OFFSET;
import static io.nodusdb.log.record.RecordFormat.COMMIT_FIRST_LSN_OFFSET;
import static io.nodusdb.log.record.RecordFormat.COMMIT_TIME_OFFSET;
import static io.nodusdb.log.record.RecordFormat.CONFIG_BYTES;
import static io.nodusdb.log.record.RecordFormat.CONFIG_KIND_OFFSET;
import static io.nodusdb.log.record.RecordFormat.EPOCH_BYTES;
import static io.nodusdb.log.record.RecordFormat.EPOCH_HANDOFF_OFFSET;
import static io.nodusdb.log.record.RecordFormat.EPOCH_KEY_OFFSET;
import static io.nodusdb.log.record.RecordFormat.EPOCH_NUMBER_OFFSET;
import static io.nodusdb.log.record.RecordFormat.ERASE_BYTES;
import static io.nodusdb.log.record.RecordFormat.ERASE_PSEUDONYM_BYTES;
import static io.nodusdb.log.record.RecordFormat.ERASE_PSEUDONYM_OFFSET;
import static io.nodusdb.log.record.RecordFormat.ERASE_SYMBOL_OFFSET;
import static io.nodusdb.log.record.RecordFormat.FLAGS_OFFSET;
import static io.nodusdb.log.record.RecordFormat.FLAG_AUTOCOMMIT;
import static io.nodusdb.log.record.RecordFormat.LENGTH_OFFSET;
import static io.nodusdb.log.record.RecordFormat.LSN_OFFSET;
import static io.nodusdb.log.record.RecordFormat.MAX_RELATION;
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

public final class RecordBatch {

    private enum State { EMPTY, OPEN, AUTOCOMMIT, COMMITTED }

    private static final int INITIAL_BYTES = 256;

    private final CRC32C crc = new CRC32C();
    private ByteBuffer buffer = ByteBuffer.allocate(INITIAL_BYTES);
    private int count;
    private State state = State.EMPTY;
    private boolean sealed;

    public void clear() {
        buffer.clear();
        count = 0;
        state = State.EMPTY;
        sealed = false;
    }

    public int count() {
        return count;
    }

    public int size() {
        return buffer.position();
    }

    public boolean isEmpty() {
        return state == State.EMPTY;
    }

    public boolean isCommitted() {
        return state == State.AUTOCOMMIT || state == State.COMMITTED;
    }

    public boolean isSealed() {
        return sealed;
    }

    public ByteBuffer bytes() {
        return buffer;
    }

    public RecordReader reader() {
        return new RecordReader().wrap(buffer, 0, buffer.position());
    }

    public void autocommitTuple(RecordType type, int object, int relation, int subjectRelation, int subject) {
        requireState(State.EMPTY);
        int start = begin(type, TUPLE_AUTOCOMMIT_BYTES, FLAG_AUTOCOMMIT);
        putTuple(start, object, relation, subjectRelation, subject);
        state = State.AUTOCOMMIT;
    }

    public void tuple(RecordType type, int object, int relation, int subjectRelation, int subject) {
        requireOpenable();
        int start = begin(type, TUPLE_BYTES, 0);
        putTuple(start, object, relation, subjectRelation, subject);
        state = State.OPEN;
    }

    public void graphConfig(int keyKindCode) {
        requireOpenable();
        int start = begin(RecordType.GRAPH_CONFIG, CONFIG_BYTES, 0);
        buffer.put(start + CONFIG_KIND_OFFSET, (byte) keyKindCode);
        state = State.OPEN;
    }

    public void symbol(int id, byte[] utf8, int offset, int length) {
        requireOpenable();
        if (id < 0 || length < 0 || length > RecordFormat.WRITE_LIMIT_BYTES - RecordFormat.SYMBOL_FIXED_BYTES) {
            throw new IllegalArgumentException("symbol " + id + " of " + length + " bytes cannot be recorded");
        }
        int start = begin(RecordType.SYMBOL, RecordFormat.symbolBytes(length), 0);
        buffer.putInt(start + SYMBOL_ID_OFFSET, id);
        buffer.putInt(start + SYMBOL_LENGTH_OFFSET, length);
        buffer.put(start + SYMBOL_BYTES_OFFSET, utf8, offset, length);
        state = State.OPEN;
    }

    public void schema(int version, byte[] digest, int[] relationIds, int[] typeSymbols, int[] nameSymbols,
                       int[] flags, byte[] document) {
        requireOpenable();
        if (digest.length != SCHEMA_DIGEST_BYTES) {
            throw new IllegalArgumentException("schema digest must be " + SCHEMA_DIGEST_BYTES + " bytes");
        }
        int relations = relationIds.length;
        int length = RecordFormat.schemaBytes(relations, document.length);
        if (length > RecordFormat.WRITE_LIMIT_BYTES) {
            throw new IllegalArgumentException("schema of " + length + " bytes exceeds the record limit");
        }
        int start = begin(RecordType.SCHEMA, length, 0);
        buffer.putInt(start + SCHEMA_VERSION_OFFSET, version);
        buffer.put(start + SCHEMA_DIGEST_OFFSET, digest);
        buffer.putInt(start + SCHEMA_DOCUMENT_LENGTH_OFFSET, document.length);
        buffer.putInt(start + SCHEMA_RELATION_COUNT_OFFSET, relations);
        int entry = start + SCHEMA_RELATIONS_OFFSET;
        for (int i = 0; i < relations; i++) {
            if (relationIds[i] < 0 || relationIds[i] > MAX_RELATION) {
                throw new IllegalArgumentException("relation id out of range: " + relationIds[i]);
            }
            buffer.putShort(entry, (short) relationIds[i]);
            buffer.putInt(entry + 2, typeSymbols[i]);
            buffer.putInt(entry + 6, nameSymbols[i]);
            buffer.putShort(entry + 10, (short) flags[i]);
            entry += SCHEMA_RELATION_BYTES;
        }
        buffer.put(entry, document);
        state = State.OPEN;
    }

    public void epoch(long epoch, int writerKey, long handoffLsn) {
        requireOpenable();
        int start = begin(RecordType.EPOCH, EPOCH_BYTES, 0);
        buffer.putLong(start + EPOCH_NUMBER_OFFSET, epoch);
        buffer.putInt(start + EPOCH_KEY_OFFSET, writerKey);
        buffer.putLong(start + EPOCH_HANDOFF_OFFSET, handoffLsn);
        state = State.OPEN;
    }

    public void erase(int symbolId, byte[] pseudonym) {
        requireOpenable();
        if (pseudonym.length != ERASE_PSEUDONYM_BYTES) {
            throw new IllegalArgumentException("pseudonym must be " + ERASE_PSEUDONYM_BYTES + " bytes");
        }
        int start = begin(RecordType.ERASE, ERASE_BYTES, 0);
        buffer.putInt(start + ERASE_SYMBOL_OFFSET, symbolId);
        buffer.put(start + ERASE_PSEUDONYM_OFFSET, pseudonym);
        state = State.OPEN;
    }

    public void commit() {
        requireState(State.OPEN);
        begin(RecordType.TXN_COMMIT, COMMIT_BYTES, 0);
        state = State.COMMITTED;
    }

    public void seal(long firstLsn, long commitMicros) {
        if (!isCommitted()) {
            throw new IllegalStateException("a batch must be committed before it is sealed");
        }
        if (sealed) {
            throw new IllegalStateException("batch is already sealed");
        }
        int end = buffer.position();
        int position = 0;
        long lsn = firstLsn;
        while (position < end) {
            int length = buffer.getInt(position + LENGTH_OFFSET);
            RecordType type = RecordType.fromCode(buffer.get(position + TYPE_OFFSET) & 0xFF);
            buffer.putLong(position + LSN_OFFSET, lsn);
            if (type == RecordType.TXN_COMMIT) {
                buffer.putLong(position + COMMIT_FIRST_LSN_OFFSET, firstLsn);
                buffer.putInt(position + COMMIT_COUNT_OFFSET, count - 1);
                buffer.putLong(position + COMMIT_TIME_OFFSET, commitMicros);
            } else if (state == State.AUTOCOMMIT) {
                buffer.putLong(position + TUPLE_COMMIT_TIME_OFFSET, commitMicros);
            }
            int checksumAt = position + length - CHECKSUM_BYTES;
            buffer.putInt(checksumAt, Checksums.crc32c(crc, buffer, position, checksumAt));
            position += length;
            lsn++;
        }
        sealed = true;
    }

    private void putTuple(int start, int object, int relation, int subjectRelation, int subject) {
        if (relation < 0 || relation > MAX_RELATION || subjectRelation < 0 || subjectRelation > MAX_RELATION) {
            throw new IllegalArgumentException("relation out of range: " + relation + ", " + subjectRelation);
        }
        buffer.putInt(start + TUPLE_OBJECT_OFFSET, object);
        buffer.putShort(start + TUPLE_RELATION_OFFSET, (short) relation);
        buffer.putShort(start + TUPLE_SUBJECT_RELATION_OFFSET, (short) subjectRelation);
        buffer.putInt(start + TUPLE_SUBJECT_OFFSET, subject);
    }

    private int begin(RecordType type, int length, int flags) {
        int start = buffer.position();
        ensureCapacity(start + length);
        Arrays.fill(buffer.array(), buffer.arrayOffset() + start, buffer.arrayOffset() + start + length, (byte) 0);
        buffer.putInt(start + LENGTH_OFFSET, length);
        buffer.put(start + TYPE_OFFSET, (byte) type.code());
        buffer.put(start + FLAGS_OFFSET, (byte) flags);
        buffer.position(start + length);
        count++;
        sealed = false;
        return start;
    }

    private void ensureCapacity(int required) {
        if (required <= buffer.capacity()) {
            return;
        }
        int capacity = Math.max(required, buffer.capacity() * 2);
        ByteBuffer grown = ByteBuffer.allocate(capacity);
        buffer.flip();
        grown.put(buffer);
        buffer = grown;
    }

    private void requireOpenable() {
        if (state != State.EMPTY && state != State.OPEN) {
            throw new IllegalStateException("batch is already committed");
        }
    }

    private void requireState(State expected) {
        if (state != expected) {
            throw new IllegalStateException("batch is " + state + ", expected " + expected);
        }
    }
}
