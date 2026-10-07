package io.nodusdb.log.record;

public final class TransactionTracker {

    private long firstLsn;
    private int records;

    public boolean accept(RecordReader record) {
        if (record.autocommit()) {
            if (records != 0) {
                throw new MalformedTransactionException("a single-record commit interrupts an open transaction");
            }
            return true;
        }
        if (record.type() == RecordType.TXN_COMMIT) {
            boolean consistent = records > 0 && record.commitRecordCount() == records
                    && record.commitFirstLsn() == firstLsn && record.lsn() == firstLsn + records;
            if (!consistent) {
                throw new MalformedTransactionException("the commit record does not match the records before it");
            }
            reset();
            return true;
        }
        if (records == 0) {
            firstLsn = record.lsn();
        }
        records++;
        return false;
    }

    public boolean open() {
        return records != 0;
    }

    public int records() {
        return records;
    }

    public long firstLsn() {
        return firstLsn;
    }

    public void reset() {
        records = 0;
        firstLsn = 0;
    }
}
