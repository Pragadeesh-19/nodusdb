package io.nodusdb.chain;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;

public final class ChainHash {

    public static final int BYTES = 32;

    public static final ChainHash ZERO = new ChainHash(new byte[BYTES]);

    private static final int FILE_BUFFER_BYTES = 1 << 20;

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
        return accumulator().update(data, offset, length).finish();
    }

    public static ChainHash sha256(Path file) throws IOException {
        Accumulator hash = accumulator();
        byte[] buffer = new byte[FILE_BUFFER_BYTES];
        try (InputStream in = Files.newInputStream(file)) {
            int read;
            while ((read = in.read(buffer)) > 0) {
                hash.update(buffer, 0, read);
            }
        }
        return hash.finish();
    }

    public static Accumulator accumulator() {
        try {
            return new Accumulator(MessageDigest.getInstance("SHA-256"));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    public static final class Accumulator {

        private final MessageDigest digest;

        private Accumulator(MessageDigest digest) {
            this.digest = digest;
        }

        public Accumulator update(byte[] data, int offset, int length) {
            digest.update(data, offset, length);
            return this;
        }

        public ChainHash finish() {
            return new ChainHash(digest.digest());
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
