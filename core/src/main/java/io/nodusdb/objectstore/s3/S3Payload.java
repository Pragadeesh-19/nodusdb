package io.nodusdb.objectstore.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.http.HttpRequest;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

sealed interface S3Payload permits S3Payload.Bytes, S3Payload.FileSlice {

    long length();

    String sha256Hex();

    HttpRequest.BodyPublisher publisher();

    record Bytes(byte[] content) implements S3Payload {

        static final Bytes EMPTY = new Bytes(new byte[0]);

        @Override
        public long length() {
            return content.length;
        }

        @Override
        public String sha256Hex() {
            return SigV4Signer.sha256Hex(content);
        }

        @Override
        public HttpRequest.BodyPublisher publisher() {
            return content.length == 0
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(content);
        }
    }

    record FileSlice(Path file, long offset, long length) implements S3Payload {

        private static final int CHUNK_BYTES = 1 << 20;

        public FileSlice {
            if (offset < 0 || length < 0) {
                throw new IllegalArgumentException("a slice needs a non-negative offset and length");
            }
        }

        @Override
        public String sha256Hex() {
            if (length == 0) {
                return SigV4Signer.sha256Hex(new byte[0]);
            }
            try (InputStream in = open()) {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] buffer = new byte[CHUNK_BYTES];
                int read;
                while ((read = in.read(buffer)) > 0) {
                    digest.update(buffer, 0, read);
                }
                return HexFormat.of().formatHex(digest.digest());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 is unavailable", e);
            }
        }

        @Override
        public HttpRequest.BodyPublisher publisher() {
            return length == 0
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.fromPublisher(
                            HttpRequest.BodyPublishers.ofInputStream(this::open), length);
        }

        InputStream open() {
            try {
                return new SliceInputStream(FileChannel.open(file, StandardOpenOption.READ), offset, length);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
