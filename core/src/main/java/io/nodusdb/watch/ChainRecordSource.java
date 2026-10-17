package io.nodusdb.watch;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.log.LogTailReader;
import io.nodusdb.log.RecordSource;
import io.nodusdb.log.RecordStream;
import io.nodusdb.log.RecordsGoneException;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

public final class ChainRecordSource implements RecordSource {

    public static final String NAME = "chain";

    private static final long NOTHING_READ = -1;

    private final ChainFetch chain;
    private final ChainLocator locator;

    public ChainRecordSource(ObjectStore store, ChainFetch chain) {
        this.chain = Objects.requireNonNull(chain, "chain");
        this.locator = new ChainLocator(Objects.requireNonNull(store, "store"), chain);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Optional<RecordStream> open(long afterLsn) {
        if (afterLsn < 0) {
            return Optional.empty();
        }
        long seq = locator.locate(afterLsn + 1);
        return seq == ChainLocator.NOT_HELD ? Optional.empty() : Optional.of(new Stream(seq, afterLsn + 1));
    }

    private final class Stream implements RecordStream {

        private final long targetLsn;
        private long seq;
        private long lastLsn = NOTHING_READ;

        Stream(long seq, long targetLsn) {
            this.seq = seq;
            this.targetLsn = targetLsn;
        }

        @Override
        public LogTailReader.Batch next(int maxBytes) {
            while (true) {
                Optional<ChainObject> object = chain.fetch(seq);
                if (object.isEmpty()) {
                    requireNotDeleted();
                    return null;
                }
                seq++;
                if (!(object.get().body() instanceof ChainBody.Records records)) {
                    continue;
                }
                requireContiguous(records);
                lastLsn = records.lsnLast();
                if (records.lsnLast() >= targetLsn) {
                    return new LogTailReader.Batch(records.lsnFirst(), records.lsnLast(), records.records(),
                            Math.toIntExact(records.lsnLast() - records.lsnFirst() + 1));
                }
            }
        }

        @Override
        public void close() {
        }

        private void requireContiguous(ChainBody.Records records) {
            if (lastLsn == NOTHING_READ && records.lsnFirst() > targetLsn) {
                throw new RecordsGoneException("the chain resumes at LSN " + records.lsnFirst() + ", after LSN "
                        + targetLsn);
            }
            if (lastLsn != NOTHING_READ && records.lsnFirst() != lastLsn + 1) {
                throw new RecordsGoneException("chain object " + (seq - 1) + " starts at LSN " + records.lsnFirst()
                        + " but LSN " + (lastLsn + 1) + " is next");
            }
        }

        private void requireNotDeleted() {
            OptionalLong oldest = chain.oldestSeq();
            if (oldest.isPresent() && oldest.getAsLong() > seq) {
                throw new RecordsGoneException("retention deleted chain object " + seq + " before it was read");
            }
            if (chain.hasObjectAfter(seq)) {
                throw new RecordsGoneException("chain object " + seq + " is missing although later objects exist");
            }
        }
    }
}
