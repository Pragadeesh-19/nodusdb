package io.nodusdb.chain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class KeyFilesTest {

    private static final HexFormat HEX = HexFormat.of();
    private static final String RFC8032_SEED = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
    private static final String RFC8032_PUBLIC = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
    private static final String RFC8032_SIGNATURE = "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e0652249015"
            + "55fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b";

    @TempDir
    Path scratch;

    private static byte[] sign(PrivateKey key, byte[] message) throws GeneralSecurityException {
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(key);
        signer.update(message);
        return signer.sign();
    }

    @Test
    void aRawSeedAndRawPublicKeyReproduceTheRfc8032FirstVector() throws GeneralSecurityException {
        PrivateKey key = KeyFiles.privateKey(HEX.parseHex(RFC8032_SEED));
        PublicKey publicKey = KeyFiles.publicKey(HEX.parseHex(RFC8032_PUBLIC));

        byte[] signature = sign(key, new byte[0]);

        assertEquals(RFC8032_SIGNATURE, HEX.formatHex(signature));
        Signature verifier = Signature.getInstance("Ed25519");
        verifier.initVerify(publicKey);
        assertTrue(verifier.verify(signature));
        assertEquals(RFC8032_PUBLIC, HEX.formatHex(KeyFiles.rawPublic(publicKey)));
    }

    @Test
    void rawPublicKeysRoundTripForManyKeysIncludingBothSignsOfX() {
        boolean sawOdd = false;
        boolean sawEven = false;
        for (int i = 0; i < 200; i++) {
            PublicKey generated = KeyFiles.generate().getPublic();
            byte[] raw = KeyFiles.rawPublic(generated);
            sawOdd |= (raw[31] & 0x80) != 0;
            sawEven |= (raw[31] & 0x80) == 0;

            assertEquals(generated, KeyFiles.publicKey(raw));
            assertArrayEquals(raw, KeyFiles.rawPublic(KeyFiles.publicKey(raw)));
        }
        assertTrue(sawOdd && sawEven, "the generated keys must cover both signs of x");
    }

    @Test
    void aPrivateKeyRoundTripsThroughItsPemFile() throws IOException, GeneralSecurityException {
        KeyPair pair = KeyFiles.generate();
        Path file = scratch.resolve("signing.pem");

        KeyFiles.writePrivate(file, pair.getPrivate());

        String text = Files.readString(file, StandardCharsets.US_ASCII);
        assertTrue(text.startsWith("-----BEGIN PRIVATE KEY-----\n"), text);
        assertTrue(text.endsWith("-----END PRIVATE KEY-----\n"), text);
        PrivateKey loaded = KeyFiles.readPrivate(file);
        assertArrayEquals(sign(pair.getPrivate(), new byte[]{1}), sign(loaded, new byte[]{1}));
    }

    @Test
    void aPublicKeyRoundTripsThroughItsPemFile() throws IOException {
        KeyPair pair = KeyFiles.generate();
        Path file = scratch.resolve("public.pem");

        KeyFiles.writePublic(file, pair.getPublic());

        assertTrue(Files.readString(file).startsWith("-----BEGIN PUBLIC KEY-----\n"));
        assertEquals(pair.getPublic(), KeyFiles.readPublic(file));
    }

    @Test
    void derEncodedKeysAreAcceptedToo() {
        KeyPair pair = KeyFiles.generate();

        assertEquals(pair.getPublic(), KeyFiles.publicKey(pair.getPublic().getEncoded()));
        assertEquals(pair.getPrivate(), KeyFiles.privateKey(pair.getPrivate().getEncoded()));
    }

    @Test
    void aRawSeedFileIsLoadedByLength() throws IOException {
        byte[] seed = HEX.parseHex(RFC8032_SEED);
        Path file = scratch.resolve("seed.key");
        Files.write(file, seed);

        PrivateKey key = KeyFiles.readPrivate(file);

        assertEquals(PrivateKeyFingerprint.of(KeyFiles.privateKey(seed)), PrivateKeyFingerprint.of(key));
    }

    @Test
    void aKeyFileIsNeverOverwritten() throws IOException {
        KeyPair pair = KeyFiles.generate();
        Path file = scratch.resolve("signing.pem");
        KeyFiles.writePrivate(file, pair.getPrivate());
        byte[] before = Files.readAllBytes(file);

        assertThrows(FileAlreadyExistsException.class, () -> KeyFiles.writePrivate(file, KeyFiles.generate().getPrivate()));
        assertThrows(FileAlreadyExistsException.class, () -> {
            Path publicFile = scratch.resolve("public.pem");
            KeyFiles.writePublic(publicFile, pair.getPublic());
            KeyFiles.writePublic(publicFile, pair.getPublic());
        });

        assertArrayEquals(before, Files.readAllBytes(file));
    }

    @Test
    void aPrivateKeyFileIsReadableByItsOwnerOnlyWhereTheFileSystemSupportsIt() throws IOException {
        Path file = scratch.resolve("signing.pem");
        assumeTrue(file.getFileSystem().supportedFileAttributeViews().contains("posix"));

        KeyFiles.writePrivate(file, KeyFiles.generate().getPrivate());

        assertEquals(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
                Files.getPosixFilePermissions(file));
    }

    @Test
    void garbageIsRefusedWithoutEchoingItsContent() throws IOException {
        String secret = "do-not-print-this-value";
        byte[] garbage = (secret + secret).getBytes(StandardCharsets.UTF_8);
        Path file = scratch.resolve("garbage.key");
        Files.write(file, garbage);

        IllegalArgumentException privateRefused = assertThrows(IllegalArgumentException.class,
                () -> KeyFiles.readPrivate(file));
        IllegalArgumentException publicRefused = assertThrows(IllegalArgumentException.class,
                () -> KeyFiles.readPublic(file));

        assertFalse(privateRefused.getMessage().contains(secret), privateRefused.getMessage());
        assertFalse(publicRefused.getMessage().contains(secret), publicRefused.getMessage());
        assertTrue(privateRefused.getMessage().contains("garbage.key"), privateRefused.getMessage());
    }

    @Test
    void theWrongLengthWrongLabelAndTruncatedKeysAreRefused() {
        KeyPair pair = KeyFiles.generate();
        String publicPem = new String(pemOf("PUBLIC KEY", pair.getPublic().getEncoded()), StandardCharsets.US_ASCII);

        assertThrows(IllegalArgumentException.class, () -> KeyFiles.privateKey(new byte[31]));
        assertThrows(IllegalArgumentException.class, () -> KeyFiles.privateKey(new byte[33]));
        assertThrows(IllegalArgumentException.class, () -> KeyFiles.privateKey(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> KeyFiles.publicKey(new byte[31]));
        assertThrows(IllegalArgumentException.class, () -> KeyFiles.privateKey(publicPem.getBytes(StandardCharsets.US_ASCII)));
        assertThrows(IllegalArgumentException.class, () -> KeyFiles.publicKey(
                pemOf("PRIVATE KEY", pair.getPublic().getEncoded())));
        byte[] der = pair.getPrivate().getEncoded();
        assertThrows(IllegalArgumentException.class, () -> KeyFiles.privateKey(Arrays.copyOf(der, der.length - 3)));
        assertThrows(IllegalArgumentException.class, () -> KeyFiles.privateKey(
                "-----BEGIN PRIVATE KEY-----\n!!!notbase64!!!\n-----END PRIVATE KEY-----\n".getBytes(StandardCharsets.US_ASCII)));
    }

    @Test
    void generatedKeysAreDistinct() {
        assertFalse(Arrays.equals(KeyFiles.rawPublic(KeyFiles.generate().getPublic()),
                KeyFiles.rawPublic(KeyFiles.generate().getPublic())));
    }

    @Test
    void aKeyringFindsKeysByIdAndReportsItsSize() {
        PublicKey first = KeyFiles.generate().getPublic();
        PublicKey second = KeyFiles.generate().getPublic();

        Keyring ring = Keyring.of(Map.of(1, first, 2, second));

        assertEquals(2, ring.size());
        assertEquals(first, ring.find(1).orElseThrow());
        assertEquals(second, ring.find(2).orElseThrow());
        assertTrue(ring.find(3).isEmpty());
        assertEquals(0, Keyring.empty().size());
    }

    private static byte[] pemOf(String label, byte[] der) {
        return ("-----BEGIN " + label + "-----\n" + Base64.getMimeEncoder(64, "\n".getBytes())
                .encodeToString(der) + "\n-----END " + label + "-----\n").getBytes(StandardCharsets.US_ASCII);
    }

    private static final class PrivateKeyFingerprint {

        static String of(PrivateKey key) {
            return HEX.formatHex(ChainHash.sha256(key.getEncoded()).toBytes());
        }
    }
}
