package io.nodusdb.chain;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;

public final class ChainHash {

    public static final int BYTES = 32;
    public static final ChainHash ZERO = new ChainHash(new byte[BYTES]);

    private final byte[] bytes;

    private ChainHash(byte[] bytes) {
        this.bytes = bytes;
    }

    public static ChainHash of(byte[] bytes) {
        if (bytes.length != BYTES) {
            throw new IllegalArgumentException("a hash holds " + BYTES + " bytes, got " + bytes.length);
        }
        return new ChainHash(bytes.clone());
    }

    public static ChainHash parse(String hex) {
        return of(HexFormat.of().parseHex(hex));
    }

    public static ChainHash sha256(byte[] data, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(data, offset, length);
            return new ChainHash(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    public static ChainHash sha256(byte[] data) {
        return sha256(data, 0, data.length);
    }

    public byte[] toBytes() {
        return bytes.clone();
    }

    public String hex() {
        return HexFormat.of().formatHex(bytes);
    }

    void writeTo(ByteBuffer target) {
        target.put(bytes);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ChainHash hash && Arrays.equals(bytes, hash.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return hex();
    }
}
