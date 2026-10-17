package io.nodusdb.replica;

import io.nodusdb.chain.ChainBody;

@FunctionalInterface
public interface ReplayLimit {

    ReplayLimit ALL = (records, appliedLsn) -> records.lsnLast();

    long lastLsn(ChainBody.Records records, long appliedLsn);
}
