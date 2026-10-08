package io.nodusdb.projection;

public interface NameResolver {

    String UNKNOWN_PREFIX = "#";

    String symbol(int symbolId);

    String relation(int relationId);

    long tenureEpoch(long lsn);

    int schemaVersion();
}
