package io.nodusdb.storage.snapshot;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Partition;
import io.nodusdb.kernel.catalog.RelationCatalog;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class SnapshotReader {

    private final FileChannel channel;
    private final SectionTable table;
    private final GraphKernel kernel;
    private final Path source;

    private SnapshotReader(FileChannel channel, SectionTable table, GraphKernel kernel, Path source) {
        this.channel = channel;
        this.table = table;
        this.kernel = kernel;
        this.source = source;
    }

    public static SnapshotMeta load(Path source, GraphKernel kernel) throws IOException {
        try (FileChannel channel = FileChannel.open(source, StandardOpenOption.READ)) {
            long size = channel.size();
            SnapshotHeader header = SnapshotHeader.read(channel, size, source);
            SectionTable table = SectionTable.index(channel, size, source);
            SnapshotReader reader = new SnapshotReader(channel, table, kernel, source);
            reader.readConfig();
            RelationCatalog catalog = reader.readSchema();
            reader.readSymbols();
            byte[] salt = reader.readSalt();
            reader.readEpochHistory();
            reader.readAdjacency(header.nodeCapacity(), catalog);
            return new SnapshotMeta(header.lsn(), header.epoch(), header.lastCommitMicros(), salt);
        }
    }

    private void readAdjacency(int nodes, RelationCatalog catalog) throws IOException {
        if (nodes == 0) {
            requireNoAdjacency();
            return;
        }
        AdjacencyLoader loader = new AdjacencyLoader(channel, kernel, catalog, nodes, source);
        loader.load(Partition.DIRECT, extent(SnapshotFormat.KIND_DIRECT_OUT), extent(SnapshotFormat.KIND_DIRECT_IN));
        boolean hasIndirect = table.length(SnapshotFormat.KIND_INDIRECT_OUT) != 0
                || table.length(SnapshotFormat.KIND_INDIRECT_IN) != 0;
        if (hasIndirect) {
            loader.load(Partition.INDIRECT, extent(SnapshotFormat.KIND_INDIRECT_OUT),
                    extent(SnapshotFormat.KIND_INDIRECT_IN));
        }
    }

    private void requireNoAdjacency() throws IOException {
        for (int kind = SnapshotFormat.KIND_DIRECT_OUT; kind <= SnapshotFormat.KIND_INDIRECT_IN; kind++) {
            if (table.length(kind) != 0) {
                throw new IOException("snapshot has adjacency data but no nodes: " + source);
            }
        }
    }

    private AdjacencyLoader.Extent extent(int kind) {
        return new AdjacencyLoader.Extent(table.start(kind), table.end(kind));
    }

    private SectionCursor cursor(int kind) {
        return new SectionCursor(channel, table.start(kind), table.end(kind), source);
    }

    private void readConfig() throws IOException {
        SectionCursor in = cursor(SnapshotFormat.KIND_CONFIG);
        int kind = in.getInt();
        int flags = in.getInt();
        in.requireFullyConsumed();
        if (flags != 0) {
            throw new UnsupportedFeatureException("snapshot graph flags " + flags + " need a newer nodusdb");
        }
        try {
            kernel.restoreKeyKind(KeyKind.fromCode(kind));
        } catch (IllegalArgumentException e) {
            throw new IOException("snapshot names an unknown key kind: " + source, e);
        }
    }

    private RelationCatalog readSchema() throws IOException {
        SectionCursor in = cursor(SnapshotFormat.KIND_SCHEMA);
        int version = in.getInt();
        byte[] digest = in.getBytes(SnapshotFormat.DIGEST_BYTES);
        int documentLength = in.getInt();
        int count = in.getInt();
        if (documentLength < 0 || count < 0 || count > 0xFFFF) {
            throw new IOException("snapshot schema section is invalid: " + source);
        }
        int[] ids = new int[count];
        int[] types = new int[count];
        int[] names = new int[count];
        int[] flags = new int[count];
        for (int i = 0; i < count; i++) {
            ids[i] = in.getUnsignedShort();
            types[i] = in.getInt();
            names[i] = in.getInt();
            flags[i] = in.getUnsignedShort();
        }
        byte[] document = in.getBytes(documentLength);
        in.requireFullyConsumed();
        if (version == 0) {
            return RelationCatalog.EMPTY;
        }
        try {
            RelationCatalog catalog = RelationCatalog.of(version, digest, document, ids, types, names, flags);
            kernel.restoreCatalog(catalog);
            return catalog;
        } catch (IllegalArgumentException e) {
            throw new IOException("snapshot schema is invalid: " + source, e);
        }
    }

    private void readSymbols() throws IOException {
        SectionCursor in = cursor(SnapshotFormat.KIND_SYMBOLS);
        int count = in.getInt();
        if (count < 0) {
            throw new IOException("snapshot symbol count is negative: " + source);
        }
        for (int id = 0; id < count; id++) {
            if (in.getByte() != 0) {
                throw new UnsupportedFeatureException("snapshot holds erased symbols, which need a newer nodusdb");
            }
            byte[] name = in.getBytes(in.getInt());
            kernel.restoreSymbol(id, name);
        }
        in.requireFullyConsumed();
    }

    private byte[] readSalt() throws IOException {
        SectionCursor in = cursor(SnapshotFormat.KIND_SALT);
        byte[] salt = in.getBytes(SnapshotFormat.SALT_BYTES);
        in.requireFullyConsumed();
        return salt;
    }

    private void readEpochHistory() throws IOException {
        SectionCursor in = cursor(SnapshotFormat.KIND_EPOCH_HISTORY);
        int count = in.getInt();
        if (count < 0) {
            throw new IOException("snapshot epoch count is negative: " + source);
        }
        for (int i = 0; i < count; i++) {
            kernel.recordEpoch(in.getLong(), in.getLong(), in.getLong());
        }
        in.requireFullyConsumed();
    }
}
