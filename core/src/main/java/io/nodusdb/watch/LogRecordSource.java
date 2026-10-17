package io.nodusdb.watch;

import io.nodusdb.error.CorruptLogException;
import io.nodusdb.log.LogStore;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.RecordSource;
import io.nodusdb.log.RecordStream;
import io.nodusdb.log.RecordsGoneException;
import io.nodusdb.log.io.LogFileSystem;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public final class LogRecordSource implements RecordSource {

    public static final String NAME = "log";

    private final LogFileSystem files;
    private final LogStore log;

    public LogRecordSource(LogFileSystem files, LogStore log) {
        this.files = Objects.requireNonNull(files, "files");
        this.log = Objects.requireNonNull(log, "log");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Optional<RecordStream> open(long afterLsn) {
        if (afterLsn < 0 || afterLsn + 1 < log.oldestLsn()) {
            return Optional.empty();
        }
        return Optional.of(new Stream(new LogTailReader(files, afterLsn)));
    }

    private final class Stream implements RecordStream {

        private final LogTailReader reader;

        Stream(LogTailReader reader) {
            this.reader = reader;
        }

        @Override
        public LogTailReader.Batch next(int maxBytes) throws IOException {
            requireHeld();
            try {
                return reader.read(log.durableLsn(), maxBytes);
            } catch (CorruptLogException failure) {
                requireHeld();
                throw failure;
            }
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        private void requireHeld() {
            long oldest = log.oldestLsn();
            if (reader.nextLsn() < oldest) {
                throw new RecordsGoneException("the local log was trimmed to LSN " + oldest + " before LSN "
                        + reader.nextLsn() + " was read");
            }
        }
    }
}
