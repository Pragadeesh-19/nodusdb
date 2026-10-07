package io.nodusdb.chain;

import java.security.KeyPair;

final class ChainFixtures {

    static final int KEY_ID = 5;
    static final long EPOCH = 3;
    static final long NONCE = 0x0123456789ABCDEFL;

    private static final KeyPair PAIR = KeyFiles.generate();
    private static final KeyPair OTHER_PAIR = KeyFiles.generate();

    private ChainFixtures() {
    }

    static SigningKey signingKey() {
        return new SigningKey(KEY_ID, PAIR.getPrivate());
    }

    static SigningKey otherSigningKey() {
        return new SigningKey(KEY_ID, OTHER_PAIR.getPrivate());
    }

    static Keyring keyring() {
        return Keyring.single(KEY_ID, PAIR.getPublic());
    }

    static ChainHeader header(ChainKind kind, long seq) {
        return new ChainHeader(kind, seq, EPOCH, NONCE, ChainHash.sha256(new byte[]{1, 2, 3}), KEY_ID);
    }

    static ChainHeader header(ChainKind kind, long seq, ChainHash prev, long epoch) {
        return new ChainHeader(kind, seq, epoch, NONCE, prev, KEY_ID);
    }

    static byte[] opaqueRecords() {
        return new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
    }

    static ChainObject snapshotRef(long seq, ChainHash prev, long epoch, long lsn) {
        return ChainCodec.decode(ChainCodec.encode(header(ChainKind.SNAPSHOT_REF, seq, prev, epoch),
                new ChainBody.SnapshotRef("_nodus/snapshots/" + String.format("%020d", lsn) + ".nsnap",
                        ChainHash.sha256(new byte[]{9}), lsn), signingKey()));
    }

    static ChainObject opaqueRecordsObject(long seq, ChainHash prev, long epoch, long first, long last) {
        return ChainCodec.decode(ChainCodec.encode(header(ChainKind.RECORDS, seq, prev, epoch),
                new ChainBody.Records(first, last, opaqueRecords()), signingKey()));
    }

    static byte[] sampleRecordsObject() {
        return ChainCodec.encode(header(ChainKind.RECORDS, 7), new ChainBody.Records(10, 12, opaqueRecords()),
                signingKey());
    }

    static byte[] sampleSnapshotObject() {
        return ChainCodec.encode(header(ChainKind.SNAPSHOT_REF, 8),
                new ChainBody.SnapshotRef("_nodus/snapshots/00000000000000000042.nsnap",
                        ChainHash.sha256(new byte[]{9}), 42), signingKey());
    }
}
