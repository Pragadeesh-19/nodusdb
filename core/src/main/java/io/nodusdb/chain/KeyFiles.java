package io.nodusdb.chain;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.EdECPublicKey;
import java.security.spec.EdECPoint;
import java.security.spec.EdECPrivateKeySpec;
import java.security.spec.EdECPublicKeySpec;
import java.security.spec.NamedParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public final class KeyFiles {

    public static final int RAW_BYTES = 32;

    private static final String PRIVATE_LABEL = "PRIVATE KEY";
    private static final String PUBLIC_LABEL = "PUBLIC KEY";
    private static final String PEM_START = "-----BEGIN ";
    private static final int PEM_LINE_BYTES = 64;
    private static final int SIGN_BIT = 0x80;
    private static final String OWNER_ONLY = "rw-------";

    private KeyFiles() {
    }

    public static KeyPair generate() {
        try {
            return KeyPairGenerator.getInstance(ChainSignature.ALGORITHM).generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Ed25519 is unavailable", e);
        }
    }

    public static PrivateKey privateKey(byte[] content) {
        try {
            KeyFactory factory = KeyFactory.getInstance(ChainSignature.ALGORITHM);
            if (content.length == RAW_BYTES) {
                return factory.generatePrivate(new EdECPrivateKeySpec(NamedParameterSpec.ED25519, content.clone()));
            }
            return factory.generatePrivate(new PKCS8EncodedKeySpec(unwrap(content, PRIVATE_LABEL)));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalArgumentException("not an Ed25519 private key: expected a raw 32-byte seed or PKCS#8");
        }
    }

    public static PublicKey publicKey(byte[] content) {
        try {
            KeyFactory factory = KeyFactory.getInstance(ChainSignature.ALGORITHM);
            if (content.length == RAW_BYTES) {
                return factory.generatePublic(new EdECPublicKeySpec(NamedParameterSpec.ED25519, point(content)));
            }
            return factory.generatePublic(new X509EncodedKeySpec(unwrap(content, PUBLIC_LABEL)));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalArgumentException("not an Ed25519 public key: expected 32 raw bytes or X.509");
        }
    }

    public static PrivateKey readPrivate(Path file) throws IOException {
        try {
            return privateKey(Files.readAllBytes(file));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(file.getFileName() + ": " + e.getMessage());
        }
    }

    public static PublicKey readPublic(Path file) throws IOException {
        try {
            return publicKey(Files.readAllBytes(file));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(file.getFileName() + ": " + e.getMessage());
        }
    }

    public static void writePrivate(Path file, PrivateKey key) throws IOException {
        create(file);
        Files.write(file, pem(PRIVATE_LABEL, key.getEncoded()).getBytes(StandardCharsets.US_ASCII));
    }

    public static void writePublic(Path file, PublicKey key) throws IOException {
        Files.write(file, pem(PUBLIC_LABEL, key.getEncoded()).getBytes(StandardCharsets.US_ASCII),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    public static byte[] rawPublic(PublicKey key) {
        EdECPoint point = ((EdECPublicKey) key).getPoint();
        byte[] bigEndian = point.getY().toByteArray();
        byte[] raw = new byte[RAW_BYTES];
        for (int i = 0; i < RAW_BYTES && i < bigEndian.length; i++) {
            raw[i] = bigEndian[bigEndian.length - 1 - i];
        }
        if (point.isXOdd()) {
            raw[RAW_BYTES - 1] |= (byte) SIGN_BIT;
        }
        return raw;
    }

    private static EdECPoint point(byte[] raw) {
        byte[] littleEndian = raw.clone();
        boolean xOdd = (littleEndian[RAW_BYTES - 1] & SIGN_BIT) != 0;
        littleEndian[RAW_BYTES - 1] &= (byte) ~SIGN_BIT;
        byte[] bigEndian = new byte[RAW_BYTES];
        for (int i = 0; i < RAW_BYTES; i++) {
            bigEndian[i] = littleEndian[RAW_BYTES - 1 - i];
        }
        return new EdECPoint(xOdd, new BigInteger(1, bigEndian));
    }

    private static void create(Path file) throws IOException {
        try {
            Files.createFile(file, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(OWNER_ONLY)));
        } catch (UnsupportedOperationException noPosix) {
            Files.createFile(file);
        } catch (FileAlreadyExistsException exists) {
            throw new FileAlreadyExistsException(file.toString(), null, "a key file is never overwritten");
        }
    }

    private static byte[] unwrap(byte[] content, String label) {
        String text = new String(content, StandardCharsets.ISO_8859_1);
        if (!text.startsWith(PEM_START)) {
            return content;
        }
        String begin = PEM_START + label + "-----";
        String end = "-----END " + label + "-----";
        int from = text.indexOf(begin);
        int to = text.indexOf(end);
        if (from != 0 || to < begin.length()) {
            throw new IllegalArgumentException("the PEM label is not " + label);
        }
        return Base64.getMimeDecoder().decode(text.substring(begin.length(), to).strip());
    }

    private static String pem(String label, byte[] der) {
        return PEM_START + label + "-----\n"
                + Base64.getMimeEncoder(PEM_LINE_BYTES, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + label + "-----\n";
    }
}
