package io.nodusdb.projection;

import io.nodusdb.chain.ChainObject;
import io.nodusdb.iceberg.DataFileWriter;
import io.nodusdb.iceberg.IcebergTable;
import io.nodusdb.iceberg.NodusLogTable;
import io.nodusdb.lake.parquet.ParquetReader;
import io.nodusdb.objectstore.DirectoryObjectStore;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.projection.EdgeLogProjector.Cadence;
import io.nodusdb.projection.EdgeLogProjector.Clocks;
import io.nodusdb.projection.EdgeLogProjector.Next;
import io.nodusdb.projection.StreamScript.Chunk;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ShipState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

final class ProjectionRig {

    record Row(long lsn, long commitTs, long txnLsn, long epoch, String event, String objectType, String objectId,
               String relation, String subjectType, String subjectId, String subjectRelation, long schemaVersion,
               String detail) {
    }

    static final long SNAPSHOT_LSN = 10;
    static final long DAY = 86_400_000_000L;
    static final long DAY_ZERO = 1_700_000_000_000_000L - 1_700_000_000_000_000L % DAY;

    final Path root;
    final Path bucket;
    final DirectoryObjectStore store;
    final FaultyObjectStore faulty;
    final ChainBuilder chain;
    ShipState state;
    final IcebergTable table;
    final DataFileWriter writer;
    final FakeNames names = new FakeNames();
    final AtomicLong nanos = new AtomicLong();
    final AtomicLong millis = new AtomicLong(1_700_000_000_000L);
    final StreamScript script = new StreamScript(SNAPSHOT_LSN + 1);
    long shippedLsn = SNAPSHOT_LSN;
    final ProjectionSettings settings;

    ProjectionRig(Path root, ProjectionSettings settings) {
        this.root = root;
        this.settings = settings;
        this.bucket = root.resolve("bucket");
        this.store = new DirectoryObjectStore(bucket);
        this.faulty = new FaultyObjectStore(store);
        this.chain = new ChainBuilder(store, 1).epoch(1);
        this.state = new ShipState(1, 0, 0, 1L << 30);
        String location = bucket.toAbsolutePath().toString().replace('\\', '/') + "/iceberg";
        this.table = new IcebergTable(faulty, location, "iceberg/", millis::get);
        this.writer = new DataFileWriter(faulty, table, root.resolve("scratch"), settings.codec());
        ChainObject first = chain.snapshotRef(SNAPSHOT_LSN, 1);
        state.ring().put(first);
        state.shipped(SNAPSHOT_LSN, first.seq(), first.encoded().length, 0);
    }

    ChainObject ship(Chunk chunk) {
        ChainObject object = chain.records(chunk.records(), chunk.lsnFirst(), chunk.lsnLast());
        state.ring().put(object);
        state.shipped(chunk.lsnLast(), object.seq(), object.encoded().length, 0);
        shippedLsn = chunk.lsnLast();
        return object;
    }

    EdgeLogProjector projector() {
        return projector(new StoreChainSource(state.ring(), faulty, ChainBuilder.keyring()));
    }

    EdgeLogProjector projector(ChainSource source) {
        return new EdgeLogProjector(source, state, table, writer, names, ChainBuilder.signingKey(), settings,
                new Clocks(nanos::get, millis::get));
    }

    EdgeLogProjector restart() {
        state = new ShipState(1, shippedLsn, chain.seq(), 1L << 30);
        return projector();
    }

    Next run(EdgeLogProjector projector) {
        Next next;
        do {
            next = projector.step();
        } while (next.cadence() == Cadence.CONTINUE);
        return next;
    }

    void advance(long seconds) {
        nanos.addAndGet(seconds * 1_000_000_000L);
        millis.addAndGet(seconds * 1_000L);
    }

    List<Row> rows() {
        List<Row> rows = new ArrayList<>();
        Path data = bucket.resolve("iceberg/data");
        if (!Files.isDirectory(data)) {
            return rows;
        }
        try (Stream<Path> files = Files.list(data)) {
            for (Path file : (Iterable<Path>) files.sorted()::iterator) {
                if (file.getFileName().toString().endsWith(".parquet")) {
                    rows.addAll(read(file));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        rows.sort(Comparator.comparingLong(Row::lsn));
        return rows;
    }

    int dataFiles() {
        Path data = bucket.resolve("iceberg/data");
        if (!Files.isDirectory(data)) {
            return 0;
        }
        try (Stream<Path> files = Files.list(data)) {
            return (int) files.filter(file -> file.getFileName().toString().endsWith(".parquet")).count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<Row> read(Path file) throws IOException {
        ParquetReader.Columns columns = ParquetReader.read(file, NodusLogTable.columns());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < columns.fixed()[0].length; i++) {
            rows.add(new Row(columns.fixed()[0][i], columns.fixed()[1][i], columns.fixed()[2][i],
                    columns.fixed()[3][i], text(columns, 4, i), text(columns, 5, i), text(columns, 6, i),
                    text(columns, 7, i), text(columns, 8, i), text(columns, 9, i), text(columns, 10, i),
                    columns.fixed()[11][i], text(columns, 12, i)));
        }
        return rows;
    }

    private static String text(ParquetReader.Columns columns, int column, int row) {
        return new String(columns.strings()[column][row], StandardCharsets.UTF_8);
    }
}
