package io.nodusdb.lake;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;

public final class LakeTable implements AutoCloseable {

    public record Config(int maxRows, int maxSlabBytes, int initialCapacity, int initialSlabBytes,
                         long flushIntervalMillis) {

        public static final Config DEFAULT = new Config(1 << 20, 1 << 26, 1 << 16, 1 << 20, 0L);

        public Config {
            if (maxRows < 1 || maxRows > DeltaMemTable.MAX_ROWS) {
                throw new IllegalArgumentException("maxRows must be in [1, 2^29]: " + maxRows);
            }
            if (maxSlabBytes < 1) {
                throw new IllegalArgumentException("maxSlabBytes must be positive: " + maxSlabBytes);
            }
            if (flushIntervalMillis < 0) {
                throw new IllegalArgumentException("flushIntervalMillis must be non-negative: " + flushIntervalMillis);
            }
        }
    }

    private static final String DATA_PREFIX = "data-";
    private static final String DELETE_PREFIX = "delete-";
    private static final String EXTENSION = ".parquet";

    private final Path directory;
    private final LakeSchema schema;
    private final DeltaMemTable.Schema shape;
    private final int[] slots;
    private final Config config;
    private final Executor flusher;
    private final Runnable release;
    private final Object lock = new Object();
    private DeltaMemTable active;
    private DeltaMemTable frozen;
    private DeltaMemTable spare;
    private CompletableFuture<Void> inFlight;
    private List<Manifest.Entry> committed;
    private long nextSequence;
    private Throwable lastFlushFailure;

    public static LakeTable open(Path directory, LakeSchema schema, Config config) throws IOException {
        ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor(daemon("nodus-lake-flusher"));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(daemon("nodus-lake-scheduler"));
        Runnable release = () -> {
            flusher.shutdown();
            scheduler.shutdown();
        };
        try {
            return new LakeTable(directory, schema, config, flusher, scheduler, release);
        } catch (IOException | RuntimeException e) {
            release.run();
            throw e;
        }
    }

    LakeTable(Path directory, LakeSchema schema, Config config, Executor flusher,
              ScheduledExecutorService scheduler, Runnable release) throws IOException {
        this.directory = directory;
        this.schema = schema;
        this.config = config;
        this.flusher = flusher;
        this.release = release;
        this.shape = schema.memtableSchema();
        this.slots = schema.slots();
        Files.createDirectories(directory);
        Path manifest = directory.resolve(Manifest.FILE_NAME);
        if (Files.exists(manifest)) {
            committed = Manifest.read(directory, schema.signature());
        } else {
            committed = List.of();
            Manifest.write(directory, schema.signature(), committed);
        }
        nextSequence = committed.stream().mapToLong(Manifest.Entry::sequence).max().orElse(-1L) + 1;
        active = newMemTable();
        spare = newMemTable();
        if (config.flushIntervalMillis() > 0 && scheduler != null) {
            scheduler.scheduleAtFixedRate(this::flushIfDirty, config.flushIntervalMillis(),
                    config.flushIntervalMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public LakeSchema schema() {
        return schema;
    }

    public void upsert(long keyHash, long[] longValues, int[] intValues, byte[] varCharValues, int[] varCharLengths) {
        synchronized (lock) {
            awaitCapacityLocked();
            active.upsert(keyHash, longValues, intValues, varCharValues, varCharLengths);
            freezeIfFullLocked();
        }
    }

    public void delete(long keyHash) {
        synchronized (lock) {
            awaitCapacityLocked();
            active.tombstone(keyHash);
            freezeIfFullLocked();
        }
    }

    public Optional<LakeRow> get(long keyHash) {
        List<Manifest.Entry> entries;
        synchronized (lock) {
            int row = active.getRow(keyHash);
            if (row != DeltaMemTable.ABSENT) {
                return liveRow(active, row);
            }
            if (frozen != null) {
                row = frozen.getRow(keyHash);
                if (row != DeltaMemTable.ABSENT) {
                    return liveRow(frozen, row);
                }
            }
            entries = committed;
        }
        return committedRow(entries, keyHash);
    }

    public void flush() {
        while (true) {
            CompletableFuture<Void> job;
            synchronized (lock) {
                if (frozen != null) {
                    job = inFlight != null && !inFlight.isDone() ? inFlight : resubmitFrozenLocked();
                } else if (spare == null) {
                    waitLocked();
                    continue;
                } else if (active.size() > 0) {
                    job = freezeLocked();
                } else {
                    return;
                }
            }
            await(job);
        }
    }

    public Optional<Throwable> lastFlushFailure() {
        synchronized (lock) {
            return Optional.ofNullable(lastFlushFailure);
        }
    }

    public List<Path> dataFiles() {
        List<Manifest.Entry> entries;
        synchronized (lock) {
            entries = committed;
        }
        List<Path> files = new ArrayList<>();
        for (Manifest.Entry entry : entries) {
            if (entry.dataFile() != null) {
                files.add(directory.resolve(entry.dataFile()));
            }
        }
        return files;
    }

    @Override
    public void close() {
        flush();
        release.run();
    }

    private void flushIfDirty() {
        synchronized (lock) {
            if (frozen == null && active.size() == 0) {
                return;
            }
        }
        try {
            flush();
        } catch (RuntimeException e) {
            synchronized (lock) {
                lastFlushFailure = e;
            }
        }
    }

    private Optional<LakeRow> liveRow(DeltaMemTable table, int row) {
        if (table.kindAt(row) == DeltaMemTable.TOMBSTONE) {
            return Optional.empty();
        }
        return Optional.of(assemble(table.keyHashAt(row),
                slot -> table.longAt(slot, row),
                slot -> table.intAt(slot, row),
                slot -> copyVarChar(table, slot, row)));
    }

    private static byte[] copyVarChar(DeltaMemTable table, int slot, int row) {
        byte[] value = new byte[table.varCharLength(slot, row)];
        table.copyVarChar(slot, row, value, 0);
        return value;
    }

    private Optional<LakeRow> committedRow(List<Manifest.Entry> entries, long keyHash) {
        try {
            for (int i = entries.size() - 1; i >= 0; i--) {
                Manifest.Entry entry = entries.get(i);
                if (entry.dataFile() != null) {
                    ParquetReader.Contents contents = ParquetReader.read(directory.resolve(entry.dataFile()), schema);
                    int index = indexOf(contents.keyHashes(), keyHash);
                    if (index >= 0) {
                        return Optional.of(assemble(keyHash,
                                slot -> contents.longValues()[slot][index],
                                slot -> contents.intValues()[slot][index],
                                slot -> contents.varCharValues()[slot][index]));
                    }
                }
                if (entry.deleteFile() != null) {
                    ParquetReader.Contents deletes =
                            ParquetReader.read(directory.resolve(entry.deleteFile()), LakeSchema.KEYS_ONLY);
                    if (indexOf(deletes.keyHashes(), keyHash) >= 0) {
                        return Optional.empty();
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Optional.empty();
    }

    private LakeRow assemble(long keyHash, IntToLongFunction longAt, IntUnaryOperator intAt,
                             IntFunction<byte[]> varCharAt) {
        long[] longs = new long[shape.longColumns()];
        int[] ints = new int[shape.intColumns()];
        byte[][] vars = new byte[shape.varCharColumns()][];
        for (int i = 0; i < slots.length; i++) {
            int slot = slots[i];
            switch (schema.fields().get(i).type()) {
                case INT64, DOUBLE -> longs[slot] = longAt.applyAsLong(slot);
                case INT32 -> ints[slot] = intAt.applyAsInt(slot);
                case UTF8 -> vars[slot] = varCharAt.apply(slot);
            }
        }
        return new LakeRow(keyHash, longs, ints, vars);
    }

    private static int indexOf(long[] keyHashes, long keyHash) {
        for (int i = 0; i < keyHashes.length; i++) {
            if (keyHashes[i] == keyHash) {
                return i;
            }
        }
        return -1;
    }

    private void awaitCapacityLocked() {
        while ((frozen != null || spare == null) && isOverCapacity()) {
            if (lastFlushFailure != null) {
                throw new IllegalStateException("writes are blocked behind a failed flush", lastFlushFailure);
            }
            waitLocked();
        }
    }

    private boolean isOverCapacity() {
        return active.size() >= 2L * config.maxRows() || active.slabBytesUsed() >= 2L * config.maxSlabBytes();
    }

    private void waitLocked() {
        try {
            lock.wait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for a flush", e);
        }
    }

    private void freezeIfFullLocked() {
        if (frozen == null && spare != null
                && (active.size() >= config.maxRows() || active.slabBytesUsed() >= config.maxSlabBytes())) {
            freezeLocked();
        }
    }

    private CompletableFuture<Void> freezeLocked() {
        CompletableFuture<Void> job = CompletableFuture.runAsync(this::flushFrozen, flusher);
        frozen = active;
        active = spare;
        spare = null;
        inFlight = job;
        return job;
    }

    private CompletableFuture<Void> resubmitFrozenLocked() {
        inFlight = CompletableFuture.runAsync(this::flushFrozen, flusher);
        return inFlight;
    }

    private void flushFrozen() {
        DeltaMemTable table;
        long sequence;
        List<Manifest.Entry> next;
        synchronized (lock) {
            table = frozen;
            sequence = nextSequence++;
            next = new ArrayList<>(committed);
        }
        try {
            next.add(writeFiles(table, sequence));
            Manifest.write(directory, schema.signature(), next);
        } catch (IOException | RuntimeException e) {
            synchronized (lock) {
                lastFlushFailure = e;
                lock.notifyAll();
            }
            throw e instanceof IOException io ? new UncheckedIOException(io) : (RuntimeException) e;
        }
        synchronized (lock) {
            committed = List.copyOf(next);
            frozen = null;
            lastFlushFailure = null;
        }
        table.clear();
        synchronized (lock) {
            spare = table;
            lock.notifyAll();
        }
    }

    private Manifest.Entry writeFiles(DeltaMemTable table, long sequence) throws IOException {
        String dataFile = null;
        String deleteFile = null;
        int[] inserts = rowsOfKind(table, DeltaMemTable.INSERT);
        if (inserts.length > 0) {
            dataFile = fileName(DATA_PREFIX, sequence);
            ParquetWriter.write(directory.resolve(dataFile), schema, table, inserts);
        }
        int[] tombstones = rowsOfKind(table, DeltaMemTable.TOMBSTONE);
        if (tombstones.length > 0) {
            deleteFile = fileName(DELETE_PREFIX, sequence);
            ParquetWriter.write(directory.resolve(deleteFile), LakeSchema.KEYS_ONLY, table, tombstones);
        }
        return new Manifest.Entry(sequence, dataFile, deleteFile);
    }

    private static String fileName(String prefix, long sequence) {
        return String.format("%s%08d%s", prefix, sequence, EXTENSION);
    }

    private static int[] rowsOfKind(DeltaMemTable table, byte kind) {
        int count = 0;
        for (int row = 0; row < table.size(); row++) {
            if (table.kindAt(row) == kind) {
                count++;
            }
        }
        int[] rows = new int[count];
        int next = 0;
        for (int row = 0; row < table.size(); row++) {
            if (table.kindAt(row) == kind) {
                rows[next++] = row;
            }
        }
        return rows;
    }

    private static void await(CompletableFuture<Void> job) {
        try {
            job.join();
        } catch (CompletionException e) {
            throw new IllegalStateException("lake flush failed", e.getCause());
        }
    }

    private static ThreadFactory daemon(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        };
    }

    private DeltaMemTable newMemTable() {
        return new DeltaMemTable(shape, config.initialCapacity(), config.initialSlabBytes());
    }
}
