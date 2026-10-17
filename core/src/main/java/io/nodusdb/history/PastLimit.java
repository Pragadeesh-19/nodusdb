package io.nodusdb.history;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.kernel.past.PastTarget;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.replica.ReplayLimit;

import java.nio.ByteBuffer;

final class PastLimit implements ReplayLimit {

    private final PastTarget target;
    private boolean reached;

    PastLimit(PastTarget target) {
        this.target = target;
    }

    PastTarget target() {
        return target;
    }

    boolean reached() {
        return reached;
    }

    void reachedAt(long appliedLsn) {
        if (target instanceof PastTarget.AtToken token && appliedLsn >= token.token().lsn()) {
            reached = true;
        }
    }

    @Override
    public long lastLsn(ChainBody.Records records, long appliedLsn) {
        byte[] bytes = records.records();
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length);
        long last = appliedLsn;
        while (reader.hasRecord()) {
            if (reader.isCommitPoint()) {
                if (!admits(reader.lsn(), reader.commitMicros())) {
                    reached = true;
                    return last;
                }
                last = reader.lsn();
            }
            reader.advance();
        }
        if (target instanceof PastTarget.AtToken token && records.lsnLast() >= token.token().lsn()) {
            reached = true;
        }
        return last;
    }

    private boolean admits(long commitLsn, long commitMicros) {
        return switch (target) {
            case PastTarget.AtToken token -> commitLsn <= token.token().lsn();
            case PastTarget.AtTime time -> commitMicros <= time.micros();
        };
    }
}
