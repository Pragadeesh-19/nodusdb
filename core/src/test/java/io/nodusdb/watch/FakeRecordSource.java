package io.nodusdb.watch;

import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.RecordSource;
import io.nodusdb.log.RecordStream;
import io.nodusdb.log.RecordsGoneException;
import io.nodusdb.log.record.RecordReader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

final class FakeRecordSource implements RecordSource {

    private record Transaction(long first, long last, byte[] bytes) {
    }

    private final String name;
    private final List<Transaction> transactions = new ArrayList<>();
    private int transactionsPerBatch = 1;
    private int batchesBeforeGone = Integer.MAX_VALUE;
    private boolean withholding;
    private long heldFrom = 1;
    private int opens;
    private int closes;

    FakeRecordSource(String name) {
        this.name = name;
    }

    FakeRecordSource holding(byte[] records) {
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(records), 0, records.length);
        int start = 0;
        long first = -1;
        while (reader.hasRecord()) {
            if (first < 0) {
                first = reader.lsn();
            }
            boolean commit = reader.isCommitPoint();
            long lsn = reader.lsn();
            reader.advance();
            if (commit) {
                transactions.add(new Transaction(first, lsn, Arrays.copyOfRange(records, start, reader.position())));
                start = reader.position();
                first = -1;
            }
        }
        return this;
    }

    FakeRecordSource heldFrom(long lsn) {
        this.heldFrom = lsn;
        return this;
    }

    FakeRecordSource batchesOf(int transactions) {
        this.transactionsPerBatch = transactions;
        return this;
    }

    FakeRecordSource goneAfter(int batches) {
        this.batchesBeforeGone = batches;
        return this;
    }

    FakeRecordSource withholding() {
        this.withholding = true;
        return this;
    }

    int opens() {
        return opens;
    }

    int closes() {
        return closes;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Optional<RecordStream> open(long afterLsn) {
        if (afterLsn + 1 < heldFrom) {
            return Optional.empty();
        }
        opens++;
        return Optional.of(new Stream(afterLsn));
    }

    private final class Stream implements RecordStream {

        private int next;
        private int served;

        Stream(long afterLsn) {
            while (next < transactions.size() && transactions.get(next).last() <= afterLsn) {
                next++;
            }
        }

        @Override
        public LogTailReader.Batch next(int maxBytes) {
            if (withholding || next >= transactions.size()) {
                return null;
            }
            if (served++ >= batchesBeforeGone) {
                throw new RecordsGoneException("the " + name + " dropped its records");
            }
            int end = Math.min(transactions.size(), next + transactionsPerBatch);
            byte[][] parts = new byte[end - next][];
            int length = 0;
            for (int i = next; i < end; i++) {
                parts[i - next] = transactions.get(i).bytes();
                length += parts[i - next].length;
            }
            byte[] bytes = new byte[length];
            int at = 0;
            for (byte[] part : parts) {
                System.arraycopy(part, 0, bytes, at, part.length);
                at += part.length;
            }
            long first = transactions.get(next).first();
            long last = transactions.get(end - 1).last();
            next = end;
            return new LogTailReader.Batch(first, last, bytes, (int) (last - first + 1));
        }

        @Override
        public void close() {
            closes++;
        }
    }
}
