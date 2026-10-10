package io.nodusdb.storage;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.catalog.RelationCatalog;
import io.nodusdb.log.LogRecovery;
import io.nodusdb.log.RecoveryResult;
import io.nodusdb.log.ReplaySink;
import io.nodusdb.log.io.DirectoryLogFileSystem;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.storage.snapshot.SnapshotReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public final class GraphSurvey {

    public record Change(String kind, String object, String relation, String subject, String subjectRelation) {
    }

    public record Transaction(long firstLsn, long lastLsn, long commitMicros, List<Change> changes) {
    }

    public record Result(long snapshotLsn, long lastLsn, long latestEpoch, List<Transaction> transactions) {
    }

    private static final String SCRATCH_PREFIX = "nodusdb-survey-";
    private static final String NONE = "";
    private static final long UNSET = -1;

    private GraphSurvey() {
    }

    public static Result of(Path directory) throws IOException {
        DirectoryProbe.requireNotOpen(directory);
        Path scratch = Files.createTempDirectory(SCRATCH_PREFIX);
        try {
            copyLog(directory.resolve(GraphFiles.LOG_DIRECTORY), scratch.resolve(GraphFiles.LOG_DIRECTORY));
            return survey(directory, scratch);
        } finally {
            FileTrees.deleteRecursively(scratch);
        }
    }

    private static Result survey(Path directory, Path scratch) throws IOException {
        GraphKernel kernel = new GraphKernel();
        Path snapshot = directory.resolve(GraphFiles.SNAPSHOT);
        long snapshotLsn = Files.isRegularFile(snapshot) ? SnapshotReader.load(snapshot, kernel).lsn() : 0;
        Collector collector = new Collector(kernel);
        RecoveryResult recovered = LogRecovery.recover(
                new DirectoryLogFileSystem(scratch.resolve(GraphFiles.LOG_DIRECTORY)), snapshotLsn, collector);
        return new Result(snapshotLsn, recovered.lastLsn(), kernel.epochHistory().latestEpoch(),
                collector.transactions());
    }

    private static void copyLog(Path source, Path target) throws IOException {
        Files.createDirectories(target);
        if (!Files.isDirectory(source)) {
            return;
        }
        try (Stream<Path> files = Files.list(source)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                Files.copy(file, target.resolve(file.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private static final class Collector implements ReplaySink {

        private final GraphKernel kernel;
        private final List<Transaction> transactions = new ArrayList<>();
        private List<Change> pending = new ArrayList<>();
        private long pendingFirstLsn = UNSET;

        Collector(GraphKernel kernel) {
            this.kernel = kernel;
        }

        List<Transaction> transactions() {
            return transactions;
        }

        @Override
        public void apply(RecordReader record) {
            if (pendingFirstLsn == UNSET) {
                pendingFirstLsn = record.lsn();
            }
            RecordType type = record.type();
            switch (type) {
                case TUPLE_ADD -> pending.add(tuple("add", record));
                case TUPLE_REMOVE -> pending.add(tuple("remove", record));
                case SCHEMA -> pending.add(new Change("schema", String.valueOf(record.schemaVersion()), NONE, NONE,
                        NONE));
                case EPOCH -> pending.add(new Change("epoch", String.valueOf(record.epochNumber()), NONE, NONE,
                        NONE));
                default -> {
                }
            }
            kernel.replay(record);
        }

        @Override
        public void committed(long commitLsn, long commitMicros) {
            transactions.add(new Transaction(pendingFirstLsn, commitLsn, commitMicros, List.copyOf(pending)));
            pending = new ArrayList<>();
            pendingFirstLsn = UNSET;
        }

        private Change tuple(String kind, RecordReader record) {
            int subjectRelation = record.subjectRelation();
            return new Change(kind, symbol(record.object()), relation(record.relation()), symbol(record.subject()),
                    subjectRelation == 0 ? NONE : relation(subjectRelation));
        }

        private String symbol(int id) {
            if (id < 0 || id >= kernel.symbols().size()) {
                return "#" + id;
            }
            return new String(kernel.symbols().resolve(id), StandardCharsets.UTF_8);
        }

        private String relation(int id) {
            RelationCatalog catalog = kernel.catalog();
            for (int i = 0; i < catalog.relationCount(); i++) {
                if (catalog.idAt(i) == id) {
                    return symbol(catalog.nameSymbolAt(i));
                }
            }
            return id == 0 ? NONE : "#" + id;
        }
    }
}
