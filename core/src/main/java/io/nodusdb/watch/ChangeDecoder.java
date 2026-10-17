package io.nodusdb.watch;

import io.nodusdb.kernel.EpochHistory;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.changes.ChangeBuffer;
import io.nodusdb.kernel.changes.ChangeEntry;
import io.nodusdb.projection.KernelNameResolver;

import java.util.ArrayList;
import java.util.List;

final class ChangeDecoder {

    private static final String NONE = "";

    private final GraphKernel kernel;
    private final KernelNameResolver names;

    ChangeDecoder(GraphKernel kernel) {
        this.kernel = kernel;
        this.names = new KernelNameResolver(kernel);
    }

    List<Change> decode(ChangeBuffer buffer) {
        List<Change> changes = new ArrayList<>(buffer.changes());
        boolean strings = kernel.keyKind() == KeyKind.STRING;
        int transactionStart = 0;
        for (int i = 0; i < buffer.size(); i++) {
            if (buffer.kind(i) != ChangeEntry.COMMIT) {
                continue;
            }
            Token token = tokenOf(buffer.lsn(i));
            long commitMicros = buffer.payload(i);
            for (int entry = transactionStart; entry < i; entry++) {
                changes.add(change(buffer, entry, token, commitMicros, strings));
            }
            transactionStart = i + 1;
        }
        if (transactionStart != buffer.size()) {
            throw new IllegalStateException("the changes end inside a transaction");
        }
        return changes;
    }

    Token tokenOf(long commitLsn) {
        return new Token(epochOf(commitLsn), commitLsn);
    }

    private long epochOf(long lsn) {
        EpochHistory history = kernel.epochHistory();
        if (history.size() == 0) {
            return kernel.epoch();
        }
        long epoch = history.tenureAt(0).epoch();
        for (int i = 1; i < history.size(); i++) {
            EpochHistory.Tenure tenure = history.tenureAt(i);
            if (tenure.firstLsn() <= lsn) {
                epoch = tenure.epoch();
            }
        }
        return epoch;
    }

    private Change change(ChangeBuffer buffer, int entry, Token token, long commitMicros, boolean strings) {
        ChangeKind kind = ChangeKind.ofEntry(buffer.kind(entry));
        long lsn = buffer.lsn(entry);
        long payload = buffer.payload(entry);
        long meta = buffer.meta(entry);
        return switch (kind) {
            case ADD, REMOVE -> new Change(kind, lsn, token, commitMicros,
                    node(ChangeEntry.object(payload), strings), relation(ChangeEntry.relation(meta)),
                    node(ChangeEntry.subject(payload), strings), relation(ChangeEntry.subjectRelation(meta)), NONE,
                    Change.NO_SCHEMA_VERSION);
            case SCHEMA -> new Change(kind, lsn, token, commitMicros, NONE, NONE, NONE, NONE, NONE,
                    Math.toIntExact(payload));
            case EPOCH -> new Change(kind, lsn, token, commitMicros, NONE, NONE, NONE, NONE, NONE,
                    Change.NO_SCHEMA_VERSION);
            case ERASE -> new Change(kind, lsn, token, commitMicros, NONE, NONE, NONE, NONE,
                    names.symbol(Math.toIntExact(payload)), Change.NO_SCHEMA_VERSION);
        };
    }

    private String node(int id, boolean strings) {
        return strings ? names.symbol(id) : Integer.toString(id);
    }

    private String relation(int id) {
        return id == 0 ? NONE : names.relation(id);
    }
}
