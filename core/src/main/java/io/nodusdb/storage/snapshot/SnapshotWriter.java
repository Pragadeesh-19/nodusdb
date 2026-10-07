package io.nodusdb.storage.snapshot;

import io.nodusdb.kernel.EpochHistory;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Partition;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.kernel.symbols.SymbolTable;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class SnapshotWriter {

    private SnapshotWriter() {
    }

    public static void write(GraphKernel kernel, SnapshotMeta meta, Path target) throws IOException {
        int nodeCapacity = kernel.nodeCapacity();
        SnapshotHeader header = new SnapshotHeader(meta.lsn(), meta.epoch(), meta.lastCommitMicros(), nodeCapacity);
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(header.encode(), 0);
            SectionOutput out = new SectionOutput(channel, SnapshotFormat.HEADER_BYTES);
            writeConfig(out, kernel);
            writeSchema(out, kernel.catalog());
            writeSymbols(out, kernel.symbols());
            writeSalt(out, meta.salt());
            writeEpochHistory(out, kernel.epochHistory());
            writeAdjacency(out, kernel, Partition.DIRECT, true, SnapshotFormat.KIND_DIRECT_OUT, nodeCapacity);
            writeAdjacency(out, kernel, Partition.DIRECT, false, SnapshotFormat.KIND_DIRECT_IN, nodeCapacity);
            writeAdjacency(out, kernel, Partition.INDIRECT, true, SnapshotFormat.KIND_INDIRECT_OUT, nodeCapacity);
            writeAdjacency(out, kernel, Partition.INDIRECT, false, SnapshotFormat.KIND_INDIRECT_IN, nodeCapacity);
            channel.force(true);
        }
    }

    private static void writeConfig(SectionOutput out, GraphKernel kernel) throws IOException {
        out.begin(SnapshotFormat.KIND_CONFIG);
        out.putInt(kernel.keyKind().code());
        out.putInt(0);
        out.end();
    }

    private static void writeSchema(SectionOutput out, RelationCatalog catalog) throws IOException {
        byte[] document = catalog.document();
        out.begin(SnapshotFormat.KIND_SCHEMA);
        out.putInt(catalog.version());
        out.putBytes(catalog.digest());
        out.putInt(document.length);
        out.putInt(catalog.relationCount());
        for (int i = 0; i < catalog.relationCount(); i++) {
            out.putShort((short) catalog.idAt(i));
            out.putInt(catalog.typeSymbolAt(i));
            out.putInt(catalog.nameSymbolAt(i));
            out.putShort((short) catalog.flagsAt(i));
        }
        out.putBytes(document);
        out.end();
    }

    private static void writeSymbols(SectionOutput out, SymbolTable symbols) throws IOException {
        int count = symbols.size();
        out.begin(SnapshotFormat.KIND_SYMBOLS);
        out.putInt(count);
        for (int id = 0; id < count; id++) {
            byte[] name = symbols.resolve(id);
            out.putByte((byte) 0);
            out.putInt(name.length);
            out.putBytes(name);
        }
        out.end();
    }

    private static void writeSalt(SectionOutput out, byte[] salt) throws IOException {
        out.begin(SnapshotFormat.KIND_SALT);
        out.putBytes(salt);
        out.end();
    }

    private static void writeEpochHistory(SectionOutput out, EpochHistory history) throws IOException {
        int count = history.size();
        out.begin(SnapshotFormat.KIND_EPOCH_HISTORY);
        out.putInt(count);
        for (int i = 0; i < count; i++) {
            EpochHistory.Tenure tenure = history.tenureAt(i);
            out.putLong(tenure.epoch());
            out.putLong(tenure.firstLsn());
            out.putLong(tenure.handoffLsn());
        }
        out.end();
    }

    private static void writeAdjacency(SectionOutput out, GraphKernel kernel, Partition partition, boolean outgoing,
                                       int kind, int nodeCapacity) throws IOException {
        out.begin(kind);
        if (kernel.hasPartition(partition)) {
            for (int node = 0; node < nodeCapacity; node++) {
                int degree = kernel.degree(partition, outgoing, node);
                out.putInt(degree);
                for (int i = 0; i < degree; i++) {
                    out.putLong(kernel.keyAt(partition, outgoing, node, i));
                }
            }
        }
        out.end();
    }
}
