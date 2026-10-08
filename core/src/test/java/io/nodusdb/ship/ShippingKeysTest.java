package io.nodusdb.ship;

import io.nodusdb.chain.KeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShippingKeysTest {

    @TempDir
    Path directory;

    private Path privateFile(KeyPair pair, String name) throws IOException {
        Path file = directory.resolve(name);
        KeyFiles.writePrivate(file, pair.getPrivate());
        return file;
    }

    private Path publicFile(KeyPair pair, String name) throws IOException {
        Path file = directory.resolve(name);
        KeyFiles.writePublic(file, pair.getPublic());
        return file;
    }

    @Test
    void aPrivateKeyAloneGivesAnEmptyKeyring() throws IOException {
        KeyPair pair = KeyFiles.generate();

        ShippingKeys.Loaded loaded = ShippingKeys.load(
                new ShippingConfig.Signing(privateFile(pair, "private.pem"), 7, null));

        assertEquals(7, loaded.signing().keyId());
        assertEquals(0, loaded.keyring().size());
    }

    @Test
    void aMatchingPublicKeyIsTrustedUnderTheConfiguredId() throws IOException {
        KeyPair pair = KeyFiles.generate();

        ShippingKeys.Loaded loaded = ShippingKeys.load(new ShippingConfig.Signing(privateFile(pair, "private.pem"), 3,
                publicFile(pair, "public.pem")));

        assertEquals(1, loaded.keyring().size());
        assertEquals(pair.getPublic(), loaded.keyring().find(3).orElseThrow());
        assertFalse(loaded.keyring().find(4).isPresent());
    }

    @Test
    void aRawSeedIsAccepted() throws IOException {
        KeyPair pair = KeyFiles.generate();
        byte[] encoded = pair.getPrivate().getEncoded();
        byte[] seed = new byte[KeyFiles.RAW_BYTES];
        System.arraycopy(encoded, encoded.length - KeyFiles.RAW_BYTES, seed, 0, KeyFiles.RAW_BYTES);
        Path file = directory.resolve("seed.key");
        Files.write(file, seed);

        ShippingKeys.Loaded loaded = ShippingKeys.load(new ShippingConfig.Signing(file, 1, null));

        assertEquals(1, loaded.signing().keyId());
    }

    @Test
    void aPublicKeyFromAnotherPairIsRefused() throws IOException {
        KeyPair signing = KeyFiles.generate();
        KeyPair stranger = KeyFiles.generate();

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> ShippingKeys.load(new ShippingConfig.Signing(privateFile(signing, "private.pem"), 1,
                        publicFile(stranger, "public.pem"))));

        assertEquals("the public key does not belong to the signing key", refused.getMessage());
    }

    @Test
    void aMissingKeyFileIsNamed() {
        Path absent = directory.resolve("absent.pem");

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> ShippingKeys.load(new ShippingConfig.Signing(absent, 1, null)));

        assertTrue(refused.getMessage().contains("absent.pem"), refused.getMessage());
    }

    @Test
    void aMissingPublicKeyFileIsNamed() throws IOException {
        KeyPair pair = KeyFiles.generate();
        Path priv = privateFile(pair, "private.pem");

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> ShippingKeys.load(new ShippingConfig.Signing(priv, 1, directory.resolve("gone.pem"))));

        assertTrue(refused.getMessage().contains("gone.pem"), refused.getMessage());
    }

    @Test
    void aGarbageKeyFileIsRefusedWithoutEchoingItsContent() throws IOException {
        Path file = directory.resolve("garbage.pem");
        Files.write(file, "super-secret-garbage-content".getBytes(StandardCharsets.US_ASCII));

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> ShippingKeys.load(new ShippingConfig.Signing(file, 1, null)));

        assertTrue(refused.getMessage().contains("garbage.pem"), refused.getMessage());
        assertFalse(refused.getMessage().contains("super-secret"), refused.getMessage());
    }

    @Test
    void aPublicKeyFileHoldingAPrivateKeyIsRefused() throws IOException {
        KeyPair pair = KeyFiles.generate();
        Path swapped = privateFile(pair, "swapped.pem");

        assertThrows(IllegalArgumentException.class,
                () -> ShippingKeys.load(new ShippingConfig.Signing(swapped, 1, swapped)));
    }
}
