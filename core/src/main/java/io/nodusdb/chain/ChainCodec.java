package io.nodusdb.chain;

import io.nodusdb.objectstore.ObjectKeys;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class ChainCodec {

    public static final int MAGIC = 0x4E43484E;
    public static final int VERSION = 1;
    public static final int HEADER_BYTES = 72;
    public static final int TRAILER_BYTES = ChainHash.BYTES + ChainSignature.BYTES;
    public static final int MAX_OBJECT_BYTES = 32 << 20;
    public static final int MAX_PATH_BYTES = 512;

    private static final int RECORDS_FIXED_BYTES = 16;
    private static final int SNAPSHOT_FIXED_BYTES = 2 + ChainHash.BYTES + 8;

    private ChainCodec() {
    }

    public static byte[] encode(ChainHeader header, ChainBody body, SigningKey key) {
        if (header.kind() != body.kind()) {
            throw new IllegalArgumentException("the header says " + header.kind() + " but the body is "
                    + body.kind());
        }
        if (header.keyId() != key.keyId()) {
            throw new IllegalArgumentException("the header names key " + header.keyId() + " but the signer is key "
                    + key.keyId());
        }
        byte[] bodyBytes = encodeBody(body);
        int signedLength = HEADER_BYTES + bodyBytes.length;
        if (signedLength + TRAILER_BYTES > MAX_OBJECT_BYTES) {
            throw new IllegalArgumentException("a chain object is limited to " + MAX_OBJECT_BYTES + " bytes");
        }
        ByteBuffer out = ByteBuffer.allocate(signedLength + TRAILER_BYTES).order(ByteOrder.BIG_ENDIAN);
        out.putInt(MAGIC).putShort((short) VERSION).put((byte) header.kind().code()).put((byte) 0);
        out.putLong(header.seq()).putLong(header.epoch()).putLong(header.writerNonce());
        header.prev().writeTo(out);
        out.putInt(header.keyId()).putInt(0);
        out.put(bodyBytes);
        ChainHash digest = ChainHash.sha256(out.array(), 0, signedLength);
        digest.writeTo(out);
        out.put(key.sign(digest));
        return out.array();
    }

    public static ChainObject decode(byte[] bytes) {
        if (bytes.length < HEADER_BYTES + TRAILER_BYTES) {
            throw new ChainFormatException("a chain object holds at least " + (HEADER_BYTES + TRAILER_BYTES)
                    + " bytes, got " + bytes.length);
        }
        if (bytes.length > MAX_OBJECT_BYTES) {
            throw new ChainFormatException("a chain object is limited to " + MAX_OBJECT_BYTES + " bytes");
        }
        ByteBuffer in = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        ChainHeader header = decodeHeader(in);
        int signedLength = bytes.length - TRAILER_BYTES;
        ChainHash digest = ChainHash.of(Arrays.copyOfRange(bytes, signedLength, signedLength + ChainHash.BYTES));
        if (!ChainHash.sha256(bytes, 0, signedLength).equals(digest)) {
            throw new ChainFormatException("the stored digest does not match the object content");
        }
        byte[] signature = Arrays.copyOfRange(bytes, signedLength + ChainHash.BYTES, bytes.length);
        ChainBody body = decodeBody(header.kind(), bytes, HEADER_BYTES, signedLength);
        return new ChainObject(header, body, digest, signature, bytes);
    }

    private static ChainHeader decodeHeader(ByteBuffer in) {
        if (in.getInt() != MAGIC) {
            throw new ChainFormatException("this is not a chain object: the magic bytes are wrong");
        }
        int version = in.getShort() & 0xFFFF;
        if (version != VERSION) {
            throw new ChainFormatException("chain object version " + version + " is not supported");
        }
        int kindCode = in.get() & 0xFF;
        if (in.get() != 0) {
            throw new ChainFormatException("a reserved header byte is not zero");
        }
        long seq = in.getLong();
        long epoch = in.getLong();
        long nonce = in.getLong();
        byte[] prev = new byte[ChainHash.BYTES];
        in.get(prev);
        int keyId = in.getInt();
        if (in.getInt() != 0) {
            throw new ChainFormatException("a reserved header field is not zero");
        }
        ChainKind kind = ChainKind.fromCode(kindCode);
        if (kind == null) {
            throw new ChainFormatException("unknown chain object kind " + kindCode);
        }
        if (seq < 1 || epoch < 0 || keyId < 0) {
            throw new ChainFormatException("the header holds a negative or zero sequence, epoch or key id");
        }
        return new ChainHeader(kind, seq, epoch, nonce, ChainHash.of(prev), keyId);
    }

    private static byte[] encodeBody(ChainBody body) {
        return switch (body) {
            case ChainBody.Records records -> {
                ByteBuffer out = ByteBuffer.allocate(RECORDS_FIXED_BYTES + records.records().length);
                out.putLong(records.lsnFirst()).putLong(records.lsnLast()).put(records.records());
                yield out.array();
            }
            case ChainBody.SnapshotRef ref -> {
                ObjectKeys.requireKey(ref.path());
                byte[] path = ref.path().getBytes(StandardCharsets.UTF_8);
                ByteBuffer out = ByteBuffer.allocate(SNAPSHOT_FIXED_BYTES + path.length);
                out.putShort((short) path.length).put(path);
                ref.sha256().writeTo(out);
                out.putLong(ref.lsn());
                yield out.array();
            }
        };
    }

    private static ChainBody decodeBody(ChainKind kind, byte[] bytes, int from, int to) {
        ByteBuffer in = ByteBuffer.wrap(bytes, from, to - from).order(ByteOrder.BIG_ENDIAN);
        return switch (kind) {
            case RECORDS -> decodeRecords(in, bytes, to);
            case SNAPSHOT_REF -> decodeSnapshotRef(in);
            case REDACTION -> throw new ChainFormatException("redaction objects are not supported by this version");
        };
    }

    private static ChainBody decodeRecords(ByteBuffer in, byte[] bytes, int end) {
        if (in.remaining() <= RECORDS_FIXED_BYTES) {
            throw new ChainFormatException("a records object holds at least one record");
        }
        long first = in.getLong();
        long last = in.getLong();
        if (first < 1 || last < first) {
            throw new ChainFormatException("the record range " + first + ".." + last + " is not valid");
        }
        return new ChainBody.Records(first, last, Arrays.copyOfRange(bytes, in.position(), end));
    }

    private static ChainBody decodeSnapshotRef(ByteBuffer in) {
        if (in.remaining() < SNAPSHOT_FIXED_BYTES) {
            throw new ChainFormatException("a snapshot reference is cut short");
        }
        int pathLength = in.getShort() & 0xFFFF;
        if (pathLength == 0 || pathLength > MAX_PATH_BYTES || in.remaining() != pathLength + ChainHash.BYTES + 8) {
            throw new ChainFormatException("the snapshot reference has an invalid path length");
        }
        byte[] path = new byte[pathLength];
        in.get(path);
        byte[] hash = new byte[ChainHash.BYTES];
        in.get(hash);
        long lsn = in.getLong();
        if (lsn < 0) {
            throw new ChainFormatException("the snapshot LSN is negative");
        }
        return new ChainBody.SnapshotRef(strictPath(path), ChainHash.of(hash), lsn);
    }

    private static String strictPath(byte[] path) {
        try {
            String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(path)).toString();
            return ObjectKeys.requireKey(text);
        } catch (CharacterCodingException | IllegalArgumentException e) {
            throw new ChainFormatException("the snapshot reference path is not a valid object key");
        }
    }
}
