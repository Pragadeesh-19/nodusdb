package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;
import io.nodusdb.chain.ChainFormatException;
import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainHeader;
import io.nodusdb.chain.ChainKind;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainTrustException;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.chain.Keyring;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.ship.ChainFetch.Trust;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.security.KeyPair;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainFetchTest {

    private final MemoryObjectStore store = new MemoryObjectStore();
    private final ChainBuilder chain = new ChainBuilder(store, 1).epoch(2);

    private ChainFetch fetch(Keyring keyring, Trust trust) {
        return new ChainFetch(store, keyring, trust);
    }

    private ChainObject forged(long seq, ChainBody body, SigningKey key) {
        ChainObject object = ChainCodec.seal(new ChainHeader(ChainKind.RECORDS, seq, 2, 1, ChainHash.ZERO,
                ChainBuilder.KEY_ID), body, key);
        store.put(ChainLayout.chainKey(seq), object.encoded());
        return object;
    }

    @ParameterizedTest
    @EnumSource(Trust.class)
    void anObjectThatIsNotThereIsAbsent(Trust trust) {
        assertEquals(Optional.empty(), fetch(ChainBuilder.keyring(), trust).fetch(1));
    }

    @ParameterizedTest
    @EnumSource(Trust.class)
    void aValidObjectFromATrustedSignerIsReturnedIntact(Trust trust) {
        ChainObject written = chain.snapshotRef(100, 1);
        ChainObject records = chain.records(3);

        ChainFetch reader = fetch(ChainBuilder.keyring(), trust);

        assertEquals(written.digest(), reader.fetch(1).orElseThrow().digest());
        assertEquals(records.digest(), reader.fetch(2).orElseThrow().digest());
    }

    @Test
    void anUnknownSignerIsAcceptedOnTheWriterSideAndRefusedWhenTrustIsRequired() {
        chain.snapshotRef(100, 1);
        Keyring withoutTheKey = Keyring.empty();

        assertEquals(1, fetch(withoutTheKey, Trust.WHEN_KEY_KNOWN).fetch(1).orElseThrow().seq());
        ChainTrustException refused = assertThrows(ChainTrustException.class,
                () -> fetch(withoutTheKey, Trust.REQUIRED).fetch(1));
        assertTrue(refused.getMessage().contains("no trusted key has the id " + ChainBuilder.KEY_ID),
                refused.getMessage());
    }

    @ParameterizedTest
    @EnumSource(Trust.class)
    void aSignatureFromAnotherKeyIsRefusedWhenThePublicKeyIsKnown(Trust trust) {
        KeyPair stranger = KeyFiles.generate();
        forged(1, new ChainBody.Records(1, 3, ChainBuilder.transaction(1, 2)),
                new SigningKey(ChainBuilder.KEY_ID, stranger.getPrivate()));

        ChainTrustException refused = assertThrows(ChainTrustException.class,
                () -> fetch(ChainBuilder.keyring(), trust).fetch(1));

        assertTrue(refused.getMessage().contains("does not verify"), refused.getMessage());
    }

    @ParameterizedTest
    @EnumSource(Trust.class)
    void anObjectStoredUnderTheWrongSequenceNumberIsRefused(Trust trust) {
        chain.snapshotRef(100, 1);
        chain.records(2);
        store.put(ChainLayout.chainKey(7), store.get(ChainLayout.chainKey(2)).orElseThrow());

        ChainTrustException refused = assertThrows(ChainTrustException.class,
                () -> fetch(ChainBuilder.keyring(), trust).fetch(7));

        assertTrue(refused.getMessage().contains("carries sequence number 2"), refused.getMessage());
    }

    @ParameterizedTest
    @EnumSource(Trust.class)
    void recordsThatDoNotParseAreRefusedEvenWithAValidSignature(Trust trust) {
        forged(1, new ChainBody.Records(5, 9, new byte[]{1, 2, 3}), ChainBuilder.signingKey());

        assertThrows(ChainFormatException.class, () -> fetch(ChainBuilder.keyring(), trust).fetch(1));
    }

    @ParameterizedTest
    @EnumSource(Trust.class)
    void everySingleByteFlipIsRefusedWhenTheSignerIsKnown(Trust trust) {
        chain.snapshotRef(100, 1);
        chain.records(2);
        byte[] original = store.get(ChainLayout.chainKey(2)).orElseThrow();
        ChainFetch reader = fetch(ChainBuilder.keyring(), trust);

        for (int i = 0; i < original.length; i++) {
            byte[] flipped = Arrays.copyOf(original, original.length);
            flipped[i] ^= 0x01;
            store.put(ChainLayout.chainKey(2), flipped);
            int position = i;
            assertThrows(RuntimeException.class, () -> reader.fetch(2), "byte " + position + " flipped");
        }
    }

    @ParameterizedTest
    @EnumSource(Trust.class)
    void everyTruncationIsRefused(Trust trust) {
        chain.snapshotRef(100, 1);
        chain.records(2);
        byte[] original = store.get(ChainLayout.chainKey(2)).orElseThrow();
        ChainFetch reader = fetch(ChainBuilder.keyring(), trust);

        for (int length = 0; length < original.length; length++) {
            store.put(ChainLayout.chainKey(2), Arrays.copyOf(original, length));
            int cut = length;
            assertThrows(RuntimeException.class, () -> reader.fetch(2), "cut to " + cut);
        }
    }

    @Test
    void theWriterSideKeepsAcceptingAnObjectWhoseSignerItDoesNotKnowButStillChecksTheRecords() {
        forged(1, new ChainBody.Records(5, 9, new byte[]{1, 2, 3}), ChainBuilder.signingKey());

        assertThrows(ChainFormatException.class, () -> fetch(Keyring.empty(), Trust.WHEN_KEY_KNOWN).fetch(1));
    }
}
