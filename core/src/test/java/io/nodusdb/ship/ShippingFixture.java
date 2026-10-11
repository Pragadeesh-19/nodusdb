package io.nodusdb.ship;

import io.nodusdb.chain.KeyFiles;
import io.nodusdb.objectstore.DirectoryObjectStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

public final class ShippingFixture {

    private static final long BREAK_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long BREAK_RETRY_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final Path bucket;
    private final Path privateKey;
    private final Path publicKey;
    private final String ship;
    private final String iceberg;

    private ShippingFixture(Path bucket, Path privateKey, Path publicKey, String ship, String iceberg) {
        this.bucket = bucket;
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.ship = ship;
        this.iceberg = iceberg;
    }

    public static ShippingFixture in(Path root) {
        try {
            Path keys = Files.createDirectories(root.resolve("keys"));
            Path privateKey = keys.resolve("signing.pem");
            Path publicKey = keys.resolve("signing.pub");
            if (!Files.exists(privateKey)) {
                KeyFiles.writePrivate(privateKey, ChainBuilder.keyPair().getPrivate());
                KeyFiles.writePublic(publicKey, ChainBuilder.keyPair().getPublic());
            }
            return new ShippingFixture(root.resolve("bucket"), privateKey, publicKey, "\"interval_ms\":10",
                    "\"enabled\":false");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ShippingFixture withShip(String members) {
        return new ShippingFixture(bucket, privateKey, publicKey, members, iceberg);
    }

    public ShippingFixture withSigning(Path newPrivateKey, Path newPublicKey) {
        return new ShippingFixture(bucket, newPrivateKey, newPublicKey, ship, iceberg);
    }

    public ShippingFixture withIceberg(String members) {
        return new ShippingFixture(bucket, privateKey, publicKey, ship, members);
    }

    public Path privateKeyFile() {
        return privateKey;
    }

    public Path publicKeyFile() {
        return publicKey;
    }

    public Path bucket() {
        return bucket;
    }

    public DirectoryObjectStore store() {
        return new DirectoryObjectStore(bucket);
    }

    public void breakStore() {
        long deadline = System.nanoTime() + BREAK_DEADLINE_NANOS;
        while (true) {
            try {
                deleteTree(bucket);
                Files.writeString(bucket, "not a directory");
                return;
            } catch (IOException | UncheckedIOException raced) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("the bucket directory could not be replaced", raced);
                }
                LockSupport.parkNanos(BREAK_RETRY_NANOS);
            }
        }
    }

    private static void deleteTree(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            for (Path entry : (Iterable<Path>) walk.sorted(Comparator.reverseOrder())::iterator) {
                Files.deleteIfExists(entry);
            }
        }
    }

    public String json() {
        return "{\"store\":{\"type\":\"directory\",\"directory\":\"" + escape(bucket) + "\"},"
                + "\"signing\":{\"key_file\":\"" + escape(privateKey) + "\",\"key_id\":" + ChainBuilder.KEY_ID
                + ",\"public_key_file\":\"" + escape(publicKey) + "\"},"
                + "\"ship\":{" + ship + "},\"iceberg\":{" + iceberg + "}}";
    }

    public String followerJson() {
        return "{\"store\":{\"type\":\"directory\",\"directory\":\"" + escape(bucket) + "\"},"
                + "\"trust\":{\"key_id\":" + ChainBuilder.KEY_ID + ",\"public_key_file\":\"" + escape(publicKey)
                + "\"}}";
    }

    public ShippingConfig config() {
        return ShippingConfig.parse(json());
    }

    private static String escape(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "\\\\");
    }
}
