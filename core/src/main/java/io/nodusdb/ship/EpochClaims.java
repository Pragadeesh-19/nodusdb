package io.nodusdb.ship;

import io.nodusdb.chain.ChainLayout;
import io.nodusdb.error.WriterFencedException;
import io.nodusdb.json.JsonWriter;
import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.PutResult;

import java.nio.charset.StandardCharsets;
import java.util.function.LongSupplier;

public final class EpochClaims {

    private final ObjectStore store;
    private final long writerNonce;
    private final int keyId;
    private final LongSupplier clockMicros;

    public EpochClaims(ObjectStore store, long writerNonce, int keyId, LongSupplier clockMicros) {
        this.store = store;
        this.writerNonce = writerNonce;
        this.keyId = keyId;
        this.clockMicros = clockMicros;
    }

    public long claim(long firstCandidate, int attempts) {
        for (int offset = 0; offset < attempts; offset++) {
            long epoch = firstCandidate + offset;
            PutResult result = store.putIfAbsent(ChainLayout.epochKey(epoch), document(epoch));
            if (result == PutResult.CREATED) {
                return epoch;
            }
        }
        throw new WriterFencedException("no epoch from " + firstCandidate + " to " + (firstCandidate + attempts - 1)
                + " could be claimed; other writers hold them");
    }

    public boolean supersededBy(long epoch) {
        return store.exists(ChainLayout.epochKey(epoch + 1));
    }

    private byte[] document(long epoch) {
        return new JsonWriter().beginObject()
                .name("epoch").value(epoch)
                .name("writer_nonce").value(Long.toHexString(writerNonce))
                .name("key_id").value(keyId)
                .name("claimed_micros").value(clockMicros.getAsLong())
                .endObject().toBytes();
    }
}
