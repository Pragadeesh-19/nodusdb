package io.nodusdb.kernel.past;

import io.nodusdb.kernel.GraphKernel;

import java.time.Duration;

public interface PastView extends AutoCloseable {

    GraphKernel kernel();

    PastReport report();

    void refreshErasures();

    void advance(PastTarget target, Duration wait);

    boolean isClosed();

    @Override
    void close();
}
