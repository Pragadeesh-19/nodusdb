package io.nodusdb.iceberg;

import io.nodusdb.iceberg.NodusLogTable.Column;
import io.nodusdb.lake.parquet.ParquetReader;
import io.nodusdb.objectstore.ObjectStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class DataFileReader {

    private final ObjectStore store;
    private final IcebergTable table;

    public DataFileReader(ObjectStore store, IcebergTable table) {
        this.store = store;
        this.table = table;
    }

    public List<EdgeLogRow> read(String uri) throws IOException {
        String key = table.keyOf(uri);
        byte[] bytes = store.get(key).orElseThrow(() -> new IllegalStateException(
                "the data file " + key + " cannot be read"));
        ParquetReader.Columns columns = ParquetReader.read(bytes, key, NodusLogTable.columns());
        int count = columns.fixed()[Column.LSN.ordinal()].length;
        List<EdgeLogRow> rows = new ArrayList<>(count);
        for (int row = 0; row < count; row++) {
            rows.add(new EdgeLogRow(number(columns, Column.LSN, row), number(columns, Column.COMMIT_TS, row),
                    number(columns, Column.TXN_LSN, row), number(columns, Column.EPOCH, row),
                    text(columns, Column.EVENT, row), text(columns, Column.OBJECT_TYPE, row),
                    text(columns, Column.OBJECT_ID, row), text(columns, Column.RELATION, row),
                    text(columns, Column.SUBJECT_TYPE, row), text(columns, Column.SUBJECT_ID, row),
                    text(columns, Column.SUBJECT_RELATION, row),
                    Math.toIntExact(number(columns, Column.SCHEMA_VERSION, row)), text(columns, Column.DETAIL, row)));
        }
        return rows;
    }

    public static EdgeLogRows rowsOf(List<EdgeLogRow> rows) {
        EdgeLogRows written = new EdgeLogRows();
        for (EdgeLogRow row : rows) {
            row.addTo(written);
        }
        return written;
    }

    private static long number(ParquetReader.Columns columns, Column column, int row) {
        return columns.fixed()[column.ordinal()][row];
    }

    private static String text(ParquetReader.Columns columns, Column column, int row) {
        return new String(columns.strings()[column.ordinal()][row], StandardCharsets.UTF_8);
    }
}
