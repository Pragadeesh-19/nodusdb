package io.nodusdb.log;

import java.io.IOException;

public interface RecordStream extends AutoCloseable {

    LogTailReader.Batch next(int maxBytes) throws IOException;

    @Override
    void close() throws IOException;
}
