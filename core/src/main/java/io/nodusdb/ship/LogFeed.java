package io.nodusdb.ship;

import io.nodusdb.log.LogStore;
import io.nodusdb.log.LogTailReader;

import java.io.IOException;

public final class LogFeed implements AutoCloseable {

    private final LogStore log;
    private final LogTailReader tail;

    public LogFeed(LogStore log, LogTailReader tail) {
        this.log = log;
        this.tail = tail;
    }

    public LogTailReader.Batch next(int maxBytes) throws IOException {
        if (log.durableLsn() < log.lastLsn()) {
            log.force();
        }
        return tail.read(log.durableLsn(), maxBytes);
    }

    public long unreadBytes() throws IOException {
        return tail.unreadBytes();
    }

    @Override
    public void close() throws IOException {
        tail.close();
    }
}
