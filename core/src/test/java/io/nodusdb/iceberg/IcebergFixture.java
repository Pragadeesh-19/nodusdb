package io.nodusdb.iceberg;

import io.nodusdb.lake.codec.ParquetCodec;
import io.nodusdb.objectstore.DirectoryObjectStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

final class IcebergFixture {

    static final String KEY_PREFIX = "iceberg/";
    static final long DAY_MICROS = 86_400_000_000L;
    static final long START_MICROS = 1_700_000_000_000_000L - 1_700_000_000_000_000L % DAY_MICROS;

    final Path bucket;
    final DirectoryObjectStore store;
    final IcebergTable table;
    final AtomicLong clock = new AtomicLong(1_700_000_000_000L);
    final DataFileWriter writer;

    IcebergFixture(Path root) {
        this.bucket = root.resolve("bucket");
        this.store = new DirectoryObjectStore(bucket);
        String location = bucket.toAbsolutePath().toString().replace('\\', '/') + "/iceberg";
        this.table = new IcebergTable(store, location, KEY_PREFIX, clock::get);
        this.writer = new DataFileWriter(store, table, root.resolve("scratch"), ParquetCodec.SNAPPY);
    }

    String metadataUri(long version) {
        return table.uriOf(KEY_PREFIX + IcebergTable.METADATA_FOLDER + "v" + version + ".metadata.json");
    }

    static EdgeLogRows rows(long firstLsn, int count, long firstMicros, int schemaVersion) {
        EdgeLogRows rows = new EdgeLogRows();
        for (int i = 0; i < count; i++) {
            long lsn = firstLsn + i;
            rows.add(lsn, firstMicros + i * 1_000_000L, firstLsn + (i / 3) * 3L + 2, 1, i % 2 == 0 ? "add" : "remove",
                    "doc", "d" + i, "viewer", "user", "u" + i % 7, i % 5 == 0 ? "member" : "", schemaVersion, "");
        }
        return rows;
    }

    DataFile dataFile(long firstLsn, int count, long firstMicros, int schemaVersion) throws IOException {
        return writer.write(rows(firstLsn, count, firstMicros, schemaVersion), NodusLogTable.dayOf(firstMicros));
    }

    IcebergTable.Head commit(Optional<IcebergTable.Head> base, Map<String, String> extra, DataFile... files) {
        clock.addAndGet(1_000);
        return table.publish(table.prepareAppend(base, List.of(files), extra));
    }
}
