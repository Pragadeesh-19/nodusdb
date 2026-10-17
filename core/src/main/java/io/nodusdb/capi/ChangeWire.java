package io.nodusdb.capi;

import io.nodusdb.watch.Change;
import io.nodusdb.watch.ChangeBatch;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

final class ChangeWire {

    static final int NAMES_PER_CHANGE = 5;
    static final int NUMBERS_PER_CHANGE = 5;

    record Encoded(byte[] utf8, int[] lengths, long[] numbers, int[] kinds) {

        int changes() {
            return kinds.length;
        }
    }

    private ChangeWire() {
    }

    static Encoded encode(ChangeBatch batch) {
        List<Change> changes = batch.changes();
        int count = changes.size();
        ByteArrayOutputStream utf8 = new ByteArrayOutputStream();
        int[] lengths = new int[NAMES_PER_CHANGE * count];
        long[] numbers = new long[NUMBERS_PER_CHANGE * count];
        int[] kinds = new int[count];
        for (int i = 0; i < count; i++) {
            Change change = changes.get(i);
            String[] names = {change.object(), change.relation(), change.subject(), change.subjectRelation(),
                change.detail()};
            for (int field = 0; field < NAMES_PER_CHANGE; field++) {
                byte[] bytes = names[field].getBytes(StandardCharsets.UTF_8);
                utf8.writeBytes(bytes);
                lengths[NAMES_PER_CHANGE * i + field] = bytes.length;
            }
            int at = NUMBERS_PER_CHANGE * i;
            numbers[at] = change.lsn();
            numbers[at + 1] = change.token().epoch();
            numbers[at + 2] = change.token().lsn();
            numbers[at + 3] = change.commitMicros();
            numbers[at + 4] = change.schemaVersion();
            kinds[i] = change.kind().code();
        }
        return new Encoded(utf8.toByteArray(), lengths, numbers, kinds);
    }
}
