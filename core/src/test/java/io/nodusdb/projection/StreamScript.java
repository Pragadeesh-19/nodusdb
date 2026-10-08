package io.nodusdb.projection;

import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordType;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

final class StreamScript {

    record Chunk(byte[] records, long lsnFirst, long lsnLast) {

        Chunk then(Chunk next) {
            ByteArrayOutputStream joined = new ByteArrayOutputStream();
            joined.writeBytes(records);
            joined.writeBytes(next.records);
            return new Chunk(joined.toByteArray(), lsnFirst, next.lsnLast);
        }
    }

    private long nextLsn;

    StreamScript(long firstLsn) {
        this.nextLsn = firstLsn;
    }

    long nextLsn() {
        return nextLsn;
    }

    Chunk transaction(long commitMicros, Consumer<RecordBatch> fill) {
        RecordBatch batch = new RecordBatch();
        fill.accept(batch);
        batch.commit();
        return seal(batch, commitMicros);
    }

    Chunk autocommit(long commitMicros, RecordType type, int object, int relation, int subjectRelation, int subject) {
        RecordBatch batch = new RecordBatch();
        batch.autocommitTuple(type, object, relation, subjectRelation, subject);
        return seal(batch, commitMicros);
    }

    static void symbol(RecordBatch batch, int id, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        batch.symbol(id, bytes, 0, bytes.length);
    }

    private Chunk seal(RecordBatch batch, long commitMicros) {
        long first = nextLsn;
        batch.seal(first, commitMicros);
        nextLsn += batch.count();
        return new Chunk(RecordFixtures.copyOf(batch), first, nextLsn - 1);
    }
}
