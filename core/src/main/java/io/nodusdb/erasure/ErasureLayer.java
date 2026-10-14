package io.nodusdb.erasure;

public interface ErasureLayer {

    String name();

    long coveredLsn();

    boolean covers(long requiredLsn, int erasedSymbols);

    void advance(long targetLsn, boolean due);
}
