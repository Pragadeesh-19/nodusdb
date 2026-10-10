package io.nodusdb.kernel;

import io.nodusdb.kernel.concurrency.WriteSequence;
import io.nodusdb.log.record.MalformedTransactionException;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.TransactionTracker;
import io.nodusdb.log.record.Verdict;

import java.nio.ByteBuffer;
import java.util.Arrays;

final class ReplicaWriter {

    private static final int INITIAL_TRANSACTIONS = 16;

    private final GraphState state;
    private final RecordApplier applier;
    private final WriteReservation reservation;
    private final WriteSequence sequence;
    private final RecordReader reader = new RecordReader();
    private final TransactionTracker tracker = new TransactionTracker();
    private int[] ends = new int[INITIAL_TRANSACTIONS];

    ReplicaWriter(GraphState state, RecordApplier applier, WriteReservation reservation) {
        this.state = state;
        this.applier = applier;
        this.reservation = reservation;
        this.sequence = state.sequence();
    }

    void apply(byte[] records, long lsnFirst, long lsnLast) {
        ByteBuffer bytes = ByteBuffer.wrap(records);
        int transactions = split(bytes, records.length, lsnFirst, lsnLast);
        int start = 0;
        for (int i = 0; i < transactions; i++) {
            applyTransaction(bytes, start, ends[i]);
            start = ends[i];
        }
    }

    private int split(ByteBuffer bytes, int length, long lsnFirst, long lsnLast) {
        if (length == 0) {
            throw new IllegalArgumentException("there are no records to apply");
        }
        long next = state.appliedLsn() + 1;
        if (lsnFirst != next) {
            throw new IllegalArgumentException("records start at LSN " + lsnFirst + " but LSN " + next + " is next");
        }
        tracker.reset();
        reader.wrap(bytes, 0, length);
        int transactions = 0;
        long lsn = lsnFirst;
        while (reader.hasRecord()) {
            requireValid();
            if (reader.lsn() != lsn) {
                throw new IllegalArgumentException("expected LSN " + lsn + " but found " + reader.lsn()
                        + " at offset " + reader.position());
            }
            boolean commitPoint = accept();
            lsn++;
            reader.advance();
            if (commitPoint) {
                ends = recordEnd(transactions++, reader.position());
            }
        }
        if (tracker.open()) {
            throw new IllegalArgumentException("the records end inside a transaction");
        }
        if (lsn - 1 != lsnLast) {
            throw new IllegalArgumentException("the records end at LSN " + (lsn - 1) + " but the range says "
                    + lsnLast);
        }
        return transactions;
    }

    private void requireValid() {
        Verdict verdict = reader.inspect();
        if (verdict != Verdict.VALID) {
            throw new IllegalArgumentException("the record at offset " + reader.position() + " is "
                    + (verdict == Verdict.INCOMPLETE ? "cut short" : reader.invalidReason()));
        }
    }

    private boolean accept() {
        try {
            return tracker.accept(reader);
        } catch (MalformedTransactionException malformed) {
            throw new IllegalArgumentException(malformed.getMessage() + " at offset " + reader.position(),
                    malformed);
        }
    }

    private int[] recordEnd(int index, int end) {
        int[] target = ends;
        if (index == target.length) {
            target = Arrays.copyOf(target, target.length * 2);
        }
        target[index] = end;
        return target;
    }

    private void applyTransaction(ByteBuffer bytes, int start, int end) {
        reservation.prepareReplicated(bytes, start, end);
        sequence.beginWrite();
        try {
            reservation.reserve();
            replay(bytes, start, end);
        } finally {
            sequence.endWrite();
        }
    }

    private void replay(ByteBuffer bytes, int start, int end) {
        reader.wrap(bytes, start, end);
        try {
            while (reader.hasRecord()) {
                applier.apply(reader);
                if (reader.isCommitPoint()) {
                    state.lastCommitMicros(reader.commitMicros());
                    state.appliedLsn(reader.lsn());
                }
                reader.advance();
            }
        } catch (RuntimeException | Error e) {
            state.fault();
            throw e;
        }
    }
}
