package io.nodusdb.chain;

import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;
import io.nodusdb.json.JsonValue;
import io.nodusdb.json.JsonNumber;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

final class ChainCorpus {

    record Entry(String name, ChainHeader header, ChainBody body, SigningKey key, PublicKey publicKey,
                 byte[] object, ChainHash digest, byte[] signature) {
    }

    private ChainCorpus() {
    }

    static List<Entry> load() throws IOException {
        try (InputStream in = ChainCorpus.class.getResourceAsStream("/chain/corpus.json")) {
            JsonArray entries = JsonParser.parseObject(in.readAllBytes()).requireArray("entries");
            List<Entry> loaded = new ArrayList<>();
            for (JsonValue value : entries.items()) {
                loaded.add(entry((JsonObject) value));
            }
            return loaded;
        }
    }

    private static Entry entry(JsonObject json) {
        HexFormat hex = HexFormat.of();
        ChainKind kind = ChainKind.valueOf(json.requireString("kind"));
        int keyId = (int) json.requireLong("key_id");
        ChainHeader header = new ChainHeader(kind, json.requireLong("seq"), json.requireLong("epoch"),
                unsignedLong(json, "nonce"), ChainHash.parse(json.requireString("prev_hex")), keyId);
        JsonObject fields = json.requireObject("fields");
        ChainBody body = kind == ChainKind.RECORDS
                ? new ChainBody.Records(fields.requireLong("lsn_first"), fields.requireLong("lsn_last"),
                        hex.parseHex(fields.requireString("records_hex")))
                : new ChainBody.SnapshotRef(fields.requireString("path"),
                        ChainHash.parse(fields.requireString("sha256_hex")), fields.requireLong("lsn"));
        PrivateKey privateKey = KeyFiles.privateKey(hex.parseHex(json.requireString("seed_hex")));
        return new Entry(json.requireString("name"), header, body, new SigningKey(keyId, privateKey),
                KeyFiles.publicKey(hex.parseHex(json.requireString("public_hex"))),
                hex.parseHex(json.requireString("object_hex")), ChainHash.parse(json.requireString("digest_hex")),
                hex.parseHex(json.requireString("signature_hex")));
    }

    private static long unsignedLong(JsonObject json, String name) {
        return Long.parseUnsignedLong(((JsonNumber) json.get(name)).text());
    }
}
