package io.nodusdb.projection;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.log.record.RecordReader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

final class EdgeLogDecoder {

    static final String EVENT_ADD = "add";
    static final String EVENT_REMOVE = "remove";
    static final String EVENT_SCHEMA = "schema";
    static final String EVENT_EPOCH = "epoch";
    static final String EVENT_ERASE = "erase";

    private static final char TYPE_SEPARATOR = ':';
    private static final String NONE = "";

    private record Pending(long lsn, String event, String object, String relation, String subject,
                           String subjectRelation, long epoch, int schemaVersion, String detail) {
    }

    private final StreamNames names;
    private final DayBuffers buffers;
    private final List<Pending> transaction = new ArrayList<>();
    private long tenureEpoch;
    private int schemaVersion;

    EdgeLogDecoder(StreamNames names, DayBuffers buffers) {
        this.names = names;
        this.buffers = buffers;
    }

    void context(long epoch, int version) {
        this.tenureEpoch = epoch;
        this.schemaVersion = version;
    }

    long epoch() {
        return tenureEpoch;
    }

    int version() {
        return schemaVersion;
    }

    void decode(ChainBody.Records body) {
        byte[] bytes = body.records();
        RecordReader reader = new RecordReader().wrap(ByteBuffer.wrap(bytes), 0, bytes.length);
        while (reader.hasRecord()) {
            record(reader);
            if (reader.isCommitPoint()) {
                commit(reader.lsn(), reader.commitMicros());
            }
            reader.advance();
        }
        if (!transaction.isEmpty()) {
            transaction.clear();
            throw new IllegalStateException("a chain object ends inside a transaction");
        }
    }

    void commitBoundary() {
        names.forgetDefinitions();
    }

    private void record(RecordReader reader) {
        switch (reader.type()) {
            case TUPLE_ADD -> tuple(reader, EVENT_ADD);
            case TUPLE_REMOVE -> tuple(reader, EVENT_REMOVE);
            case SYMBOL -> names.defineSymbol(reader.symbolId(), new String(reader.symbolBytes(),
                    StandardCharsets.UTF_8));
            case SCHEMA -> schema(reader);
            case EPOCH -> epoch(reader);
            case ERASE -> erase(reader);
            case GRAPH_CONFIG, TXN_COMMIT -> {
            }
        }
    }

    private void tuple(RecordReader reader, String event) {
        int subjectRelation = reader.subjectRelation();
        transaction.add(new Pending(reader.lsn(), event, names.symbol(reader.object()),
                names.relation(reader.relation()), names.symbol(reader.subject()),
                subjectRelation == 0 ? NONE : names.relation(subjectRelation), tenureEpoch, schemaVersion, NONE));
    }

    private void schema(RecordReader reader) {
        schemaVersion = reader.schemaVersion();
        for (int i = 0; i < reader.schemaRelationCount(); i++) {
            names.defineRelation(reader.schemaRelationId(i), names.symbol(reader.schemaRelationNameSymbol(i)));
        }
        transaction.add(new Pending(reader.lsn(), EVENT_SCHEMA, NONE, NONE, NONE, NONE, tenureEpoch, schemaVersion,
                new String(reader.schemaDocument(), StandardCharsets.UTF_8)));
    }

    private void epoch(RecordReader reader) {
        tenureEpoch = reader.epochNumber();
        transaction.add(new Pending(reader.lsn(), EVENT_EPOCH, NONE, NONE, NONE, NONE, tenureEpoch, schemaVersion,
                NONE));
    }

    private void erase(RecordReader reader) {
        String pseudonym = HexFormat.of().formatHex(reader.erasePseudonym());
        names.erase(reader.eraseSymbolId(), pseudonym);
        transaction.add(new Pending(reader.lsn(), EVENT_ERASE, NONE, NONE, NONE, NONE, tenureEpoch, schemaVersion,
                pseudonym));
    }

    private void commit(long txnLsn, long commitMicros) {
        for (Pending row : transaction) {
            String[] object = split(row.object());
            String[] subject = split(row.subject());
            buffers.add(commitMicros, row.lsn(), txnLsn, row.epoch(), row.event(), object[0], object[1],
                    row.relation(), subject[0], subject[1], row.subjectRelation(), row.schemaVersion(), row.detail());
        }
        transaction.clear();
    }

    static String[] split(String name) {
        int separator = name.indexOf(TYPE_SEPARATOR);
        if (separator < 0) {
            return new String[]{NONE, name};
        }
        return new String[]{name.substring(0, separator), name.substring(separator + 1)};
    }
}
