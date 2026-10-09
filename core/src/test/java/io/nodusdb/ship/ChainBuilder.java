package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainHeader;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.Keyring;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFixtures;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.objectstore.ObjectStore;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Map;

public final class ChainBuilder {

    public static final int KEY_ID = 3;

    private static final KeyPair PAIR = KeyFiles.generate();

    private final ObjectStore store;
    private final long nonce;
    private long seq;
    private ChainHash digest = ChainHash.ZERO;
    private long epoch;
    private long lastLsn = -1;

    public ChainBuilder(ObjectStore store, long nonce) {
        this.store = store;
        this.nonce = nonce;
    }

    public static KeyPair keyPair() {
        return PAIR;
    }

    public static SigningKey signingKey() {
        return new SigningKey(KEY_ID, PAIR.getPrivate());
    }

    public static Keyring keyring() {
        return Keyring.single(KEY_ID, PAIR.getPublic());
    }

    public static byte[] transaction(long firstLsn, int tuples) {
        RecordBatch batch = new RecordBatch();
        for (int i = 0; i < tuples; i++) {
            batch.tuple(RecordType.TUPLE_ADD, i, 1, 0, i + 1);
        }
        batch.commit();
        batch.seal(firstLsn, RecordFixtures.COMMIT_MICROS);
        return RecordFixtures.copyOf(batch);
    }

    public long seq() {
        return seq;
    }

    public long lastLsn() {
        return lastLsn;
    }

    public ChainHash digest() {
        return digest;
    }

    public ChainBuilder epoch(long newEpoch) {
        this.epoch = newEpoch;
        return this;
    }

    public ChainObject snapshotRef(long lsn, long floor) {
        String path = ChainLayout.snapshotKey(lsn);
        store.putIfAbsent(path, ("snapshot " + lsn).getBytes(StandardCharsets.UTF_8),
                Map.of(ChainHead.FLOOR_METADATA, Long.toString(floor)));
        ChainObject object = ChainCodec.seal(next(ChainKind.SNAPSHOT_REF), new ChainBody.SnapshotRef(path,
                ChainHash.sha256(("snapshot " + lsn).getBytes(StandardCharsets.UTF_8)), lsn), signingKey());
        publish(object);
        if (lastLsn < 0) {
            lastLsn = lsn;
        }
        return object;
    }

    ChainObject snapshotRefWithoutSnapshot(long lsn) {
        ChainObject object = ChainCodec.seal(next(ChainKind.SNAPSHOT_REF), new ChainBody.SnapshotRef(
                ChainLayout.snapshotKey(lsn), ChainHash.ZERO, lsn), signingKey());
        publish(object);
        if (lastLsn < 0) {
            lastLsn = lsn;
        }
        return object;
    }

    public ChainObject records(int tuples) {
        long first = lastLsn + 1;
        byte[] records = transaction(first, tuples);
        long last = first + tuples;
        ChainObject object = ChainCodec.seal(next(ChainKind.RECORDS), new ChainBody.Records(first, last, records),
                signingKey());
        publish(object);
        lastLsn = last;
        return object;
    }

    public ChainObject records(byte[] raw, long lsnFirst, long lsnLast) {
        ChainObject object = ChainCodec.seal(next(ChainKind.RECORDS), new ChainBody.Records(lsnFirst, lsnLast, raw),
                signingKey());
        publish(object);
        lastLsn = lsnLast;
        return object;
    }

    ChainBuilder many(int objects, int tuples) {
        for (int i = 0; i < objects; i++) {
            records(tuples);
        }
        return this;
    }

    private ChainHeader next(ChainKind kind) {
        return new ChainHeader(kind, seq + 1, epoch, nonce, digest, KEY_ID);
    }

    private void publish(ChainObject object) {
        store.put(ChainLayout.chainKey(object.seq()), object.encoded());
        seq = object.seq();
        digest = object.digest();
    }
}
