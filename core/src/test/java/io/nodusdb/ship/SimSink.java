package io.nodusdb.ship;

import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.replica.ReplicaSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;

final class SimSink implements ReplicaSink {

    private static final String STATE_PREFIX = "state ";
    private static final String TEXT_PREFIX = "snapshot ";

    private final Set<Long> edges = new TreeSet<>();
    private long appliedLsn;
    private boolean loaded;

    static long edge(int object, int subject) {
        return ((long) object << 32) | (subject & 0xFFFFFFFFL);
    }

    Set<Long> edges() {
        return Set.copyOf(edges);
    }

    long appliedLsn() {
        return appliedLsn;
    }

    boolean loaded() {
        return loaded;
    }

    @Override
    public long load(Path snapshotFile) throws IOException {
        String text = Files.readString(snapshotFile, StandardCharsets.UTF_8);
        String[] lines = text.split("\n");
        edges.clear();
        if (lines[0].startsWith(STATE_PREFIX)) {
            appliedLsn = Long.parseLong(lines[0].substring(STATE_PREFIX.length()));
            for (int i = 1; i < lines.length; i++) {
                if (!lines[i].isEmpty()) {
                    edges.add(Long.parseLong(lines[i]));
                }
            }
        } else if (lines[0].startsWith(TEXT_PREFIX)) {
            appliedLsn = Long.parseLong(lines[0].substring(TEXT_PREFIX.length()));
        } else {
            throw new IOException("an unknown snapshot content: " + lines[0]);
        }
        loaded = true;
        return appliedLsn;
    }

    @Override
    public void apply(byte[] records, long lsnFirst, long lsnLast) {
        if (!loaded) {
            throw new IllegalStateException("records were applied before a snapshot was loaded");
        }
        if (lsnFirst != appliedLsn + 1) {
            throw new IllegalStateException("records start at LSN " + lsnFirst + " after LSN " + appliedLsn);
        }
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(records), 0, records.length);
        while (reader.hasRecord()) {
            if (reader.type() == RecordType.TUPLE_ADD) {
                edges.add(edge(reader.object(), reader.subject()));
            } else if (reader.type() == RecordType.TUPLE_REMOVE) {
                edges.remove(edge(reader.object(), reader.subject()));
            }
            reader.advance();
        }
        appliedLsn = lsnLast;
    }
}
