package io.nodusdb.replica;

import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.log.record.RecordReader;

import java.nio.ByteBuffer;
import java.util.Arrays;

final class RecordSlice {

    private static final byte[] NOTHING = new byte[0];

    private RecordSlice() {
    }

    static byte[] from(byte[] records, long firstLsn) {
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(records), 0, records.length);
        boolean previousEndedTransaction = true;
        while (reader.hasRecord()) {
            if (reader.lsn() >= firstLsn) {
                if (!previousEndedTransaction) {
                    throw new ChainTrustException("LSN " + firstLsn + " falls inside a transaction");
                }
                return reader.position() == 0 ? Arrays.copyOf(records, records.length)
                        : Arrays.copyOfRange(records, reader.position(), records.length);
            }
            previousEndedTransaction = reader.isCommitPoint();
            reader.advance();
        }
        return NOTHING;
    }
}
