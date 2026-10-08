package io.nodusdb.ship;

import io.nodusdb.log.LogStore;
import io.nodusdb.log.LogTailReader;

import java.io.IOException;
import java.util.function.BooleanSupplier;

public final class LogFeed implements AutoCloseable {

    private final LogStore log;
    private final LogTailReader tail;
    private final BooleanSupplier forceWanted;

    public LogFeed(LogStore log, LogTailReader tail) {
        this(log, tail, () -> true);
    }

    public LogFeed(LogStore log, LogTailReader tail, BooleanSupplier forceWanted) {
        this.log = log;
        this.tail = tail;
        this.forceWanted = forceWanted;
    }

    public LogTailReader.Batch next(int maxBytes) throws IOException {
        if (log.durableLsn() < log.lastLsn() && forceWanted.getAsBoolean()) {
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
