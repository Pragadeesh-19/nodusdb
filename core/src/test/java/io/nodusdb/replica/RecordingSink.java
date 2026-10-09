package io.nodusdb.replica;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class RecordingSink implements ReplicaSink {

    record Applied(long first, long last, int bytes) {
    }

    final List<Long> loaded = new ArrayList<>();
    final List<Applied> applied = new ArrayList<>();
    long overrideLoadedLsn = -1;
    RuntimeException failOnApply;

    @Override
    public long load(Path snapshotFile) throws IOException {
        String text = Files.readString(snapshotFile, StandardCharsets.UTF_8);
        long lsn = overrideLoadedLsn >= 0 ? overrideLoadedLsn : Long.parseLong(text.substring("snapshot ".length()));
        loaded.add(lsn);
        return lsn;
    }

    @Override
    public void apply(byte[] records, long lsnFirst, long lsnLast) {
        if (failOnApply != null) {
            throw failOnApply;
        }
        applied.add(new Applied(lsnFirst, lsnLast, records.length));
    }

    long lastAppliedLsn() {
        return applied.isEmpty() ? -1 : applied.get(applied.size() - 1).last();
    }
}
