package io.nodusdb.capi;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.symbols.SymbolTable;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFormat;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

final class StringKeys {

    static final int MAX_NAME_BYTES = RecordFormat.WRITE_LIMIT_BYTES - RecordFormat.COMMIT_BYTES
            - RecordFormat.SYMBOL_FIXED_BYTES - Long.BYTES;

    private final GraphKernel kernel;
    private final RecordBatch batch = new RecordBatch();
    private final Map<String, Integer> pending = new HashMap<>();

    StringKeys(GraphKernel kernel) {
        this.kernel = kernel;
    }

    long lookup(byte[] utf8, int offset, int length) {
        return kernel.symbols().lookup(utf8, offset, length);
    }

    byte[] resolve(long id) {
        SymbolTable symbols = kernel.symbols();
        if (id < 0 || id >= symbols.size()) {
            throw new IllegalArgumentException("unknown symbol id: " + id);
        }
        return symbols.resolve((int) id);
    }

    long intern(byte[] utf8, int offset, int length) {
        requireNameLength(length);
        int existing = kernel.symbols().lookup(utf8, offset, length);
        if (existing >= 0) {
            return existing;
        }
        int id = kernel.symbols().size();
        batch.clear();
        batch.symbol(id, utf8, offset, length);
        batch.commit();
        kernel.commit(batch);
        return id;
    }

    void internAll(byte[] utf8, int[] lengths, int count, long[] ids) {
        SymbolTable symbols = kernel.symbols();
        int cursor = 0;
        int next = symbols.size();
        startBatch();
        for (int i = 0; i < count; i++) {
            int length = lengths[i];
            if (length < 0 || length > utf8.length - cursor) {
                throw new IllegalArgumentException("string lengths exceed the supplied bytes");
            }
            requireNameLength(length);
            int id = symbols.lookup(utf8, cursor, length);
            if (id < 0) {
                String name = new String(utf8, cursor, length, StandardCharsets.ISO_8859_1);
                Integer defined = pending.get(name);
                if (defined == null) {
                    if (!fits(length)) {
                        flush();
                        next = symbols.size();
                    }
                    defined = next++;
                    batch.symbol(defined, utf8, cursor, length);
                    pending.put(name, defined);
                }
                id = defined;
            }
            ids[i] = id;
            cursor += length;
        }
        flush();
    }

    private void startBatch() {
        batch.clear();
        pending.clear();
    }

    private boolean fits(int length) {
        long used = batch.size() + (long) RecordFormat.symbolBytes(length) + RecordFormat.COMMIT_BYTES;
        return used <= RecordFormat.WRITE_LIMIT_BYTES;
    }

    private void flush() {
        if (!batch.isEmpty()) {
            batch.commit();
            kernel.commit(batch);
        }
        startBatch();
    }

    private static void requireNameLength(int length) {
        if (length > MAX_NAME_BYTES) {
            throw new IllegalArgumentException("a string key is limited to " + MAX_NAME_BYTES + " bytes: " + length);
        }
    }
}
