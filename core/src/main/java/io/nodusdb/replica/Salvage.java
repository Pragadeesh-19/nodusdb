package io.nodusdb.replica;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.Keyring;
import io.nodusdb.config.StoreFactory;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.ship.ChainFetch;
import io.nodusdb.ship.ChainFetch.Trust;
import io.nodusdb.ship.ChainHead;
import io.nodusdb.storage.GraphSurvey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

public final class Salvage {

    private record Handoff(long lsn, boolean provisional) {
    }

    private static final long NONE_UNAVAILABLE = 0;

    private final ObjectStore store;
    private final Keyring keyring;

    public Salvage(ObjectStore store, Keyring keyring) {
        this.store = Objects.requireNonNull(store, "store");
        this.keyring = Objects.requireNonNull(keyring, "keyring");
    }

    public static SalvageReport salvage(FollowerConfig config, Path directory) throws IOException {
        Objects.requireNonNull(config, "config");
        Keyring keyring = TrustedKeys.load(config.trust());
        ObjectStore store = StoreFactory.open(config.storage()).store();
        try {
            return new Salvage(store, keyring).salvage(directory);
        } finally {
            store.close();
        }
    }

    public SalvageReport salvage(Path directory) throws IOException {
        Objects.requireNonNull(directory, "directory");
        GraphSurvey.Result survey = GraphSurvey.of(directory);
        Handoff handoff = handoff(survey.latestEpoch());
        List<GraphSurvey.Transaction> lost = survey.transactions().stream()
                .filter(transaction -> transaction.firstLsn() > handoff.lsn())
                .toList();
        long unavailable = survey.snapshotLsn() > handoff.lsn() ? survey.snapshotLsn() : NONE_UNAVAILABLE;
        return new SalvageReport(handoff.lsn(), handoff.provisional(), survey.lastLsn(), unavailable, lost);
    }

    private Handoff handoff(long localEpoch) {
        ChainFetch fetch = new ChainFetch(store, keyring, Trust.REQUIRED);
        ChainHead.Found head = new ChainHead(store, keyring, Trust.REQUIRED).find().orElseThrow(
                () -> new IllegalStateException("the object store holds no chain to compare this directory with"));
        long headSeq = head.object().seq();
        long lowestLater = 0;
        for (long seq = headSeq; seq >= 1; seq--) {
            Optional<ChainObject> object = fetch.fetch(seq);
            if (object.isEmpty() || object.get().epoch() <= localEpoch) {
                break;
            }
            lowestLater = seq;
        }
        for (long seq = lowestLater; lowestLater > 0 && seq <= headSeq; seq++) {
            OptionalLong recorded = firstLaterEpochHandoff(fetch.require(seq), localEpoch);
            if (recorded.isPresent()) {
                return new Handoff(recorded.getAsLong(), false);
            }
        }
        return new Handoff(head.cursor().lastLsn(), true);
    }

    private static OptionalLong firstLaterEpochHandoff(ChainObject object, long localEpoch) {
        if (!(object.body() instanceof ChainBody.Records records)) {
            return OptionalLong.empty();
        }
        byte[] bytes = records.records();
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length);
        while (reader.hasRecord()) {
            if (reader.type() == RecordType.EPOCH && reader.epochNumber() > localEpoch) {
                return OptionalLong.of(reader.epochHandoffLsn());
            }
            reader.advance();
        }
        return OptionalLong.empty();
    }
}
