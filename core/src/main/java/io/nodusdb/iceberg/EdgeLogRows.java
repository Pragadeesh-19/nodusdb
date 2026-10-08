package io.nodusdb.iceberg;

import io.nodusdb.iceberg.NodusLogTable.Column;
import io.nodusdb.lake.parquet.ColumnSource;
import io.nodusdb.lake.parquet.ColumnSpec;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public final class EdgeLogRows implements ColumnSource {

    private static final int INITIAL_ROWS = 256;
    private static final int INITIAL_BYTES = 4_096;

    private static final class Text {

        private byte[] data = new byte[INITIAL_BYTES];
        private int[] ends = new int[INITIAL_ROWS];
        private int used;

        void add(int row, String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            if (row == ends.length) {
                ends = Arrays.copyOf(ends, ends.length * 2);
            }
            if (used + bytes.length > data.length) {
                data = Arrays.copyOf(data, Math.max(data.length * 2, used + bytes.length));
            }
            System.arraycopy(bytes, 0, data, used, bytes.length);
            used += bytes.length;
            ends[row] = used;
        }

        Strings slice(int from, int to) {
            int count = to - from;
            int base = from == 0 ? 0 : ends[from - 1];
            int[] offsets = new int[count + 1];
            for (int i = 0; i < count; i++) {
                offsets[i + 1] = ends[from + i] - base;
            }
            int total = offsets[count];
            return new Strings(MemorySegment.ofArray(data).asSlice(base, total), MemorySegment.ofArray(offsets));
        }

        void clear() {
            used = 0;
        }
    }

    private static final List<ColumnSpec> COLUMNS = NodusLogTable.columns();

    private long[] lsn = new long[INITIAL_ROWS];
    private long[] commitTs = new long[INITIAL_ROWS];
    private long[] txnLsn = new long[INITIAL_ROWS];
    private long[] epoch = new long[INITIAL_ROWS];
    private long[] schemaVersion = new long[INITIAL_ROWS];
    private final Text event = new Text();
    private final Text objectType = new Text();
    private final Text objectId = new Text();
    private final Text relation = new Text();
    private final Text subjectType = new Text();
    private final Text subjectId = new Text();
    private final Text subjectRelation = new Text();
    private final Text detail = new Text();
    private int size;

    public void add(long lsnValue, long commitMicros, long txnLsnValue, long epochValue, String eventName,
                    String objectTypeName, String objectIdName, String relationName, String subjectTypeName,
                    String subjectIdName, String subjectRelationName, int schemaVersionValue, String detailText) {
        if (size == lsn.length) {
            grow();
        }
        lsn[size] = lsnValue;
        commitTs[size] = commitMicros;
        txnLsn[size] = txnLsnValue;
        epoch[size] = epochValue;
        schemaVersion[size] = schemaVersionValue;
        event.add(size, eventName);
        objectType.add(size, objectTypeName);
        objectId.add(size, objectIdName);
        relation.add(size, relationName);
        subjectType.add(size, subjectTypeName);
        subjectId.add(size, subjectIdName);
        subjectRelation.add(size, subjectRelationName);
        detail.add(size, detailText);
        size++;
    }

    public int size() {
        return size;
    }

    public long firstLsn() {
        return lsn[0];
    }

    public long lastLsn() {
        return lsn[size - 1];
    }

    public void clear() {
        size = 0;
        event.clear();
        objectType.clear();
        objectId.clear();
        relation.clear();
        subjectType.clear();
        subjectId.clear();
        subjectRelation.clear();
        detail.clear();
    }

    @Override
    public int rowCount() {
        return size;
    }

    @Override
    public List<ColumnSpec> columns() {
        return COLUMNS;
    }

    @Override
    public MemorySegment fixed(int column, int from, int to) {
        long[] values = switch (Column.values()[column]) {
            case LSN -> lsn;
            case COMMIT_TS -> commitTs;
            case TXN_LSN -> txnLsn;
            case EPOCH -> epoch;
            case SCHEMA_VERSION -> schemaVersion;
            default -> throw new IllegalArgumentException("column " + column + " is not a fixed-width column");
        };
        return MemorySegment.ofArray(values).asSlice((long) from * Long.BYTES, (long) (to - from) * Long.BYTES);
    }

    @Override
    public Strings strings(int column, int from, int to) {
        Text text = switch (Column.values()[column]) {
            case EVENT -> event;
            case OBJECT_TYPE -> objectType;
            case OBJECT_ID -> objectId;
            case RELATION -> relation;
            case SUBJECT_TYPE -> subjectType;
            case SUBJECT_ID -> subjectId;
            case SUBJECT_RELATION -> subjectRelation;
            case DETAIL -> detail;
            default -> throw new IllegalArgumentException("column " + column + " is not a string column");
        };
        return text.slice(from, to);
    }

    private void grow() {
        int capacity = lsn.length * 2;
        lsn = Arrays.copyOf(lsn, capacity);
        commitTs = Arrays.copyOf(commitTs, capacity);
        txnLsn = Arrays.copyOf(txnLsn, capacity);
        epoch = Arrays.copyOf(epoch, capacity);
        schemaVersion = Arrays.copyOf(schemaVersion, capacity);
    }
}
