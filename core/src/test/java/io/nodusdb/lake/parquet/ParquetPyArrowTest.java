package io.nodusdb.lake.parquet;

import io.nodusdb.json.JsonArray;
import io.nodusdb.json.JsonNull;
import io.nodusdb.json.JsonNumber;
import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonString;
import io.nodusdb.lake.codec.ParquetCodec;
import io.nodusdb.verify.PythonVerifier;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetPyArrowTest {

    @TempDir
    Path directory;

    private static final List<ColumnSpec> SPECS = List.of(
            ColumnSpec.withFieldId("lsn", ColumnType.INT64, 1),
            ColumnSpec.withFieldId("commit_ts", ColumnType.TIMESTAMP_MICROS, 2),
            ColumnSpec.withFieldId("schema_version", ColumnType.INT32, 3),
            ColumnSpec.withFieldId("event", ColumnType.STRING, 4));

    private static ArrayColumnSource source(int rows, List<ColumnSpec> specs) {
        long[] lsn = new long[rows];
        long[] ts = new long[rows];
        long[] version = new long[rows];
        String[] event = new String[rows];
        for (int i = 0; i < rows; i++) {
            lsn[i] = i + 1L;
            ts[i] = 1_700_000_000_000_000L + (i / 100) * 1_000_000L;
            version[i] = i % 3 - 1;
            event[i] = i % 7 == 0 ? "" : (i % 2 == 0 ? "add" : "removeé中");
        }
        return ArrayColumnSource.of(rows).fixed(specs.get(0), lsn).fixed(specs.get(1), ts)
                .fixed(specs.get(2), version).strings(specs.get(3), event);
    }

    private Optional<JsonObject> inspect(Path file) {
        return PythonVerifier.run(directory, "parquet_info.py", file.toString());
    }

    @Test
    void aRealReaderSeesTheSchemaFieldIdsAndTheTimestampType() throws IOException {
        Path file = directory.resolve("log.parquet");
        WrittenFile written = ParquetWriter.write(file, source(5_000, SPECS), ParquetCodec.SNAPPY);
        Optional<JsonObject> info = inspect(file);
        Assumptions.assumeTrue(info.isPresent(), "pyarrow is not available");

        JsonObject report = info.get();

        assertEquals(5_000, report.requireLong("rows"));
        assertEquals(1, report.requireLong("row_groups"));
        JsonArray columns = report.requireArray("columns");
        String[] names = {"lsn", "commit_ts", "schema_version", "event"};
        String[] types = {"int64", "timestamp[us, tz=UTC]", "int32", "string"};
        for (int i = 0; i < 4; i++) {
            JsonObject column = (JsonObject) columns.get(i);
            assertEquals(names[i], column.requireString("name"));
            assertEquals(types[i], column.requireString("arrow_type"));
            assertEquals(i + 1, column.requireLong("field_id"));
            assertFalse(column.boolOr("nullable", true), names[i] + " must be required");
        }
        assertEquals(written.rowCount(), report.requireLong("rows"));
    }

    @Test
    void aRealReaderReadsEveryValueBack() throws IOException {
        Path file = directory.resolve("values.parquet");
        ParquetWriter.write(file, source(5_000, SPECS), ParquetCodec.SNAPPY);
        Optional<JsonObject> info = inspect(file);
        Assumptions.assumeTrue(info.isPresent(), "pyarrow is not available");

        JsonArray values = info.get().requireArray("values");

        JsonArray lsn = (JsonArray) values.get(0);
        JsonArray ts = (JsonArray) values.get(1);
        JsonArray version = (JsonArray) values.get(2);
        JsonArray event = (JsonArray) values.get(3);
        for (int i = 0; i < 5_000; i++) {
            assertEquals(i + 1L, ((JsonNumber) lsn.get(i)).asLong(), "lsn " + i);
            assertEquals(1_700_000_000_000_000L + (i / 100) * 1_000_000L,
                    ((JsonNumber) ts.get(i)).asLong(), "ts " + i);
            assertEquals(i % 3 - 1, ((JsonNumber) version.get(i)).asLong(), "version " + i);
            String expected = i % 7 == 0 ? "" : (i % 2 == 0 ? "add" : "removeé中");
            assertEquals(expected, ((JsonString) event.get(i)).value(), "event " + i);
        }
    }

    @Test
    void theStatisticsARealReaderSeesMatchTheMetricsWeReport() throws IOException {
        Path file = directory.resolve("stats.parquet");
        WrittenFile written = ParquetWriter.write(file, source(5_000, SPECS), ParquetCodec.UNCOMPRESSED);
        Optional<JsonObject> info = inspect(file);
        Assumptions.assumeTrue(info.isPresent(), "pyarrow is not available");

        JsonArray group = (JsonArray) info.get().requireArray("statistics").get(0);

        JsonObject lsn = (JsonObject) group.get(0);
        assertEquals(1, lsn.requireLong("min"));
        assertEquals(5_000, lsn.requireLong("max"));
        assertEquals(0, lsn.requireLong("nulls"));
        JsonObject version = (JsonObject) group.get(2);
        assertEquals(-1, version.requireLong("min"));
        assertEquals(1, version.requireLong("max"));
        assertEquals(5_000, written.columns().get(0).valueCount());
        assertTrue(written.columns().get(3).hasBounds());
    }

    @Test
    void severalRowGroupsAreReadAsOneTable() throws IOException {
        int rows = 2 * ParquetWriter.ROW_GROUP_ROWS + 77;
        Path file = directory.resolve("groups.parquet");
        ParquetWriter.write(file, source(rows, SPECS), ParquetCodec.SNAPPY);
        Optional<JsonObject> info = inspect(file);
        Assumptions.assumeTrue(info.isPresent(), "pyarrow is not available");

        JsonObject report = info.get();

        assertEquals(rows, report.requireLong("rows"));
        assertEquals(3, report.requireLong("row_groups"));
        JsonArray lsn = (JsonArray) report.requireArray("values").get(0);
        assertEquals(rows, ((JsonNumber) lsn.get(rows - 1)).asLong());
        assertEquals("nodusdb lake", report.requireString("created_by"));
    }

    @Test
    void columnsWithoutFieldIdsHaveNoneInTheFile() throws IOException {
        List<ColumnSpec> plain = List.of(ColumnSpec.of("lsn", ColumnType.INT64),
                ColumnSpec.of("commit_ts", ColumnType.TIMESTAMP_MICROS),
                ColumnSpec.of("schema_version", ColumnType.INT32), ColumnSpec.of("event", ColumnType.STRING));
        Path file = directory.resolve("plain.parquet");
        ParquetWriter.write(file, source(50, plain), ParquetCodec.SNAPPY);
        Optional<JsonObject> info = inspect(file);
        Assumptions.assumeTrue(info.isPresent(), "pyarrow is not available");

        JsonArray columns = info.get().requireArray("columns");

        for (int i = 0; i < 4; i++) {
            assertTrue(((JsonObject) columns.get(i)).get("field_id") instanceof JsonNull, "column " + i);
        }
    }
}
