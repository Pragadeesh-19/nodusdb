package io.nodusdb.chain;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;

public final class SignedRecord {

    public record Opened(int keyId, byte[] payload) {
    }

    private static final char SEPARATOR = '.';
    private static final int PARTS = 3;
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private SignedRecord() {
    }

    public static String seal(SigningKey key, String domain, byte[] payload) {
        byte[] signature = key.signMessage(message(domain, payload));
        return ENCODER.encodeToString(payload) + SEPARATOR + ENCODER.encodeToString(signature) + SEPARATOR
                + key.keyId();
    }

    public static Opened open(Keyring keyring, String domain, String sealed) {
        String[] parts = sealed.split("\\.", -1);
        if (parts.length != PARTS) {
            throw new ChainFormatException("a signed record has " + PARTS + " parts, found " + parts.length);
        }
        byte[] payload = decode(parts[0]);
        byte[] signature = decode(parts[1]);
        int keyId = parseKeyId(parts[2]);
        PublicKey key = keyring.find(keyId).orElseThrow(() -> new ChainTrustException(
                "no trusted key has the id " + keyId));
        if (!verifies(key, message(domain, payload), signature)) {
            throw new ChainTrustException("the signature of the record does not verify");
        }
        return new Opened(keyId, payload);
    }

    public static byte[] payloadWithoutVerification(String sealed) {
        String[] parts = sealed.split("\\.", -1);
        if (parts.length != PARTS) {
            throw new ChainFormatException("a signed record has " + PARTS + " parts, found " + parts.length);
        }
        return decode(parts[0]);
    }

    private static byte[] message(String domain, byte[] payload) {
        byte[] prefix = (domain + '\0').getBytes(StandardCharsets.UTF_8);
        byte[] digest = ChainHash.sha256(payload).toBytes();
        byte[] message = new byte[prefix.length + digest.length];
        System.arraycopy(prefix, 0, message, 0, prefix.length);
        System.arraycopy(digest, 0, message, prefix.length, digest.length);
        return message;
    }

    private static boolean verifies(PublicKey key, byte[] message, byte[] signature) {
        try {
            Signature verifier = Signature.getInstance(ChainSignature.ALGORITHM);
            verifier.initVerify(key);
            verifier.update(message);
            return verifier.verify(signature);
        } catch (GeneralSecurityException e) {
            return false;
        }
    }

    private static byte[] decode(String text) {
        try {
            return DECODER.decode(text);
        } catch (IllegalArgumentException e) {
            throw new ChainFormatException("a signed record part is not valid base64");
        }
    }

    private static int parseKeyId(String text) {
        try {
            int keyId = Integer.parseInt(text);
            if (keyId < 0) {
                throw new ChainFormatException("a key id must not be negative");
            }
            return keyId;
        } catch (NumberFormatException e) {
            throw new ChainFormatException("a signed record key id is not a number");
        }
    }
}
