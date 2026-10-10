package io.nodusdb.kernel;

import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordType;

import java.io.ByteArrayOutputStream;
import java.util.function.Consumer;

final class ReplicatedStream {

    private static final long MICROS_STEP = 1_000;

    record Chunk(byte[] records, long first, long last) {

        void applyTo(GraphKernel kernel) {
            kernel.applyReplicated(records, first, last);
        }

        Chunk withRecords(byte[] changed) {
            return new Chunk(changed, first, last);
        }
    }

    private final ByteArrayOutputStream pending = new ByteArrayOutputStream();
    private long pendingFirst;
    private long nextLsn;
    private long lastMicros;

    ReplicatedStream() {
        this(1);
    }

    ReplicatedStream(long firstLsn) {
        this.nextLsn = firstLsn;
        this.pendingFirst = firstLsn;
        this.lastMicros = RecordFixtures.COMMIT_MICROS;
    }

    long nextLsn() {
        return nextLsn;
    }

    long lastLsn() {
        return nextLsn - 1;
    }

    long lastCommitMicros() {
        return lastMicros;
    }

    ReplicatedStream add(int object, int subject) {
        return autocommit(RecordType.TUPLE_ADD, object, 0, 0, subject);
    }

    ReplicatedStream remove(int object, int subject) {
        return autocommit(RecordType.TUPLE_REMOVE, object, 0, 0, subject);
    }

    ReplicatedStream autocommit(RecordType type, int object, int relation, int subjectRelation, int subject) {
        RecordBatch batch = new RecordBatch();
        batch.autocommitTuple(type, object, relation, subjectRelation, subject);
        return seal(batch);
    }

    ReplicatedStream transaction(Consumer<RecordBatch> records) {
        RecordBatch batch = new RecordBatch();
        records.accept(batch);
        batch.commit();
        return seal(batch);
    }

    Chunk drain() {
        Chunk chunk = new Chunk(pending.toByteArray(), pendingFirst, nextLsn - 1);
        pending.reset();
        pendingFirst = nextLsn;
        return chunk;
    }

    private ReplicatedStream seal(RecordBatch batch) {
        lastMicros += MICROS_STEP;
        batch.seal(nextLsn, lastMicros);
        nextLsn += batch.count();
        byte[] bytes = RecordFixtures.copyOf(batch);
        pending.write(bytes, 0, bytes.length);
        return this;
    }
}
