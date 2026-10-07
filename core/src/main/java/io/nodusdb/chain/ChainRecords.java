package io.nodusdb.chain;

import io.nodusdb.log.record.MalformedTransactionException;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.TransactionTracker;
import io.nodusdb.log.record.Verdict;

import java.nio.ByteBuffer;

public final class ChainRecords {

    private ChainRecords() {
    }

    public static void verify(ChainBody.Records body) {
        byte[] records = body.records();
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(records), 0, records.length);
        TransactionTracker transaction = new TransactionTracker();
        long expected = body.lsnFirst();
        while (reader.hasRecord()) {
            Verdict verdict = reader.inspect();
            if (verdict != Verdict.VALID) {
                throw new ChainFormatException("the record at offset " + reader.position() + " is "
                        + (verdict == Verdict.INCOMPLETE ? "cut short" : reader.invalidReason()));
            }
            if (reader.lsn() != expected) {
                throw new ChainFormatException("expected LSN " + expected + " but found " + reader.lsn()
                        + " at offset " + reader.position());
            }
            try {
                transaction.accept(reader);
            } catch (MalformedTransactionException malformed) {
                throw new ChainFormatException(malformed.getMessage() + " at offset " + reader.position());
            }
            expected++;
            reader.advance();
        }
        if (transaction.open()) {
            throw new ChainFormatException("the records end inside a transaction");
        }
        if (expected - 1 != body.lsnLast()) {
            throw new ChainFormatException("the records end at LSN " + (expected - 1) + " but the header says "
                    + body.lsnLast());
        }
    }
}
