package io.nodusdb.projection;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.SignedRecord;
import io.nodusdb.chain.SigningKey;
import io.nodusdb.iceberg.DataFile;
import io.nodusdb.json.JsonWriter;

import java.util.List;

public final class CommitRecord {

    public static final String DOMAIN = "nodus.commit.v1";
    public static final String PROPERTY = "nodus.commit.v1";

    public record Sealed(String text, ChainHash digest) {
    }

    private CommitRecord() {
    }

    public static Sealed seal(SigningKey key, ChainHash previous, long lsnFirst, long lsnLast, long chainSeq,
                              List<DataFile> files) {
        JsonWriter json = new JsonWriter();
        json.beginObject();
        json.name("v").value(1);
        json.name("prev").value(previous == null ? "" : previous.hex());
        json.name("lsn_first").value(lsnFirst);
        json.name("lsn_last").value(lsnLast);
        json.name("chain_seq").value(chainSeq);
        json.name("files").beginArray();
        for (DataFile file : files) {
            json.beginObject().name("path").value(file.path()).name("sha256").value(file.sha256().hex()).endObject();
        }
        json.endArray();
        json.endObject();
        byte[] payload = json.toBytes();
        return new Sealed(SignedRecord.seal(key, DOMAIN, payload), ChainHash.sha256(payload));
    }

    public static ChainHash digestOf(String sealed) {
        return ChainHash.sha256(SignedRecord.payloadWithoutVerification(sealed));
    }
}
