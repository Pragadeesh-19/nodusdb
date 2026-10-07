package io.nodusdb.capi;

import io.nodusdb.authz.TupleTransaction;

import java.nio.charset.StandardCharsets;

final class TupleWire {

    static final int FIELDS_PER_OPERATION = 4;
    static final int NO_FIELD = -1;

    private TupleWire() {
    }

    static TupleTransaction decode(byte[] utf8, int[] lengths, int[] kinds, int operations) {
        if (operations < 0 || lengths.length != FIELDS_PER_OPERATION * operations || kinds.length != operations) {
            throw new IllegalArgumentException("the operation columns do not match the operation count");
        }
        TupleTransaction transaction = new TupleTransaction();
        int cursor = 0;
        for (int i = 0; i < operations; i++) {
            String[] fields = new String[FIELDS_PER_OPERATION];
            for (int field = 0; field < FIELDS_PER_OPERATION; field++) {
                int length = lengths[FIELDS_PER_OPERATION * i + field];
                if (length == NO_FIELD && field == FIELDS_PER_OPERATION - 1) {
                    continue;
                }
                if (length < 0 || length > utf8.length - cursor) {
                    throw new IllegalArgumentException("string lengths exceed the supplied bytes");
                }
                fields[field] = new String(utf8, cursor, length, StandardCharsets.UTF_8);
                cursor += length;
            }
            if (kinds[i] == 1) {
                transaction.add(fields[0], fields[1], fields[2], fields[3]);
            } else if (kinds[i] == 0) {
                transaction.remove(fields[0], fields[1], fields[2], fields[3]);
            } else {
                throw new IllegalArgumentException("operation kind must be 0 (remove) or 1 (add): " + kinds[i]);
            }
        }
        if (cursor != utf8.length) {
            throw new IllegalArgumentException("the supplied bytes are longer than the string lengths");
        }
        return transaction;
    }
}
