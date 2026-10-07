package io.nodusdb.log.record;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class RecordFixtures {

    public static final long FIRST_LSN = 1_000;
    public static final long COMMIT_MICROS = 1_700_000_000_000_000L;

    private RecordFixtures() {
    }

    public static RecordBatch autocommit(RecordType type, int object, int relation, int subjectRelation, int subject) {
        RecordBatch batch = new RecordBatch();
        batch.autocommitTuple(type, object, relation, subjectRelation, subject);
        batch.seal(FIRST_LSN, COMMIT_MICROS);
        return batch;
    }

    public static RecordBatch transaction() {
        RecordBatch batch = new RecordBatch();
        batch.graphConfig(2);
        symbol(batch, 7, "document:readme");
        batch.tuple(RecordType.TUPLE_ADD, 7, 3, 0, 9);
        batch.tuple(RecordType.TUPLE_REMOVE, 9, 4, 5, 7);
        batch.commit();
        batch.seal(FIRST_LSN, COMMIT_MICROS);
        return batch;
    }

    public static RecordBatch everyRecordType() {
        RecordBatch batch = new RecordBatch();
        batch.epoch(3, 11, 995);
        batch.graphConfig(1);
        symbol(batch, 0, "");
        symbol(batch, 1, "user:alice");
        symbol(batch, 2, "a longer name with spaces and unicode é");
        batch.schema(4, digest(), new int[] {1, 2, 3}, new int[] {10, 10, 11}, new int[] {20, 21, 22},
                new int[] {0, 1, 6}, utf8("type user\ntype group { relation member: user }\n"));
        batch.tuple(RecordType.TUPLE_ADD, 1, 2, 0, 0);
        batch.tuple(RecordType.TUPLE_REMOVE, 2, 3, 65_535, 2_147_483_637);
        batch.erase(1, pseudonym());
        batch.commit();
        batch.seal(FIRST_LSN, COMMIT_MICROS);
        return batch;
    }

    public static void symbol(RecordBatch batch, int id, String text) {
        byte[] bytes = utf8(text);
        batch.symbol(id, bytes, 0, bytes.length);
    }

    public static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] digest() {
        byte[] digest = new byte[32];
        for (int i = 0; i < digest.length; i++) {
            digest[i] = (byte) (i * 7 + 1);
        }
        return digest;
    }

    public static byte[] pseudonym() {
        byte[] pseudonym = new byte[24];
        Arrays.fill(pseudonym, (byte) 0);
        byte[] text = utf8("erased:0123456789abcdef");
        System.arraycopy(text, 0, pseudonym, 0, text.length);
        return pseudonym;
    }

    public static byte[] copyOf(RecordBatch batch) {
        byte[] bytes = new byte[batch.size()];
        batch.bytes().get(0, bytes);
        return bytes;
    }
}
