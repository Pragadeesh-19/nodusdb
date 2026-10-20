package io.nodusdb.lake;

import io.nodusdb.lake.buffer.ColumnarRows;
import io.nodusdb.lake.codec.ParquetCodec;
import io.nodusdb.lake.model.LakeSchema;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ParquetGoldenOutputTest {

    private static final String RECORD_PROPERTY = "nodus.golden.record";
    private static final String GOLDEN = "/golden/lake/parquet-sha256.txt";
    private static final int ROWS = 130_000;
    private static final String[] STATUSES = {"paid", "open", "void"};
    private static final LakeSchema SCHEMA = new LakeSchema(List.of(
            new LakeSchema.Field("amount", LakeSchema.Type.INT64),
            new LakeSchema.Field("bucket", LakeSchema.Type.INT64),
            new LakeSchema.Field("score", LakeSchema.Type.DOUBLE),
            new LakeSchema.Field("ratio", LakeSchema.Type.DOUBLE),
            new LakeSchema.Field("status", LakeSchema.Type.INT32),
            new LakeSchema.Field("wide", LakeSchema.Type.INT32),
            new LakeSchema.Field("label", LakeSchema.Type.UTF8),
            new LakeSchema.Field("name", LakeSchema.Type.UTF8)));

    @TempDir
    Path directory;

    @ParameterizedTest
    @EnumSource(value = ParquetCodec.class)
    void flushedFilesAreByteIdenticalToThePreviousWriter(ParquetCodec codec) throws IOException {
        LakeTable.Config config = new LakeTable.Config(1 << 20, 1 << 26, 1 << 16, 1 << 20, 0L, codec);
        try (LakeTable table = LakeTable.open(directory, SCHEMA, config)) {
            table.upsertColumns(batch(0, ROWS, new Random(7)));
            table.flush();
            Random random = new Random(11);
            for (int i = 0; i < 5_000; i++) {
                long key = random.nextInt(ROWS);
                UpsertArrays.upsert(table, key,
                        new long[]{key * 3, key % 7, Double.doubleToRawLongBits(key * 0.5),
                            Double.doubleToRawLongBits(2.0)},
                        new int[]{(int) (key % 5), random.nextInt()},
                        ("over" + key % 4 + "renamed-" + key).getBytes(StandardCharsets.UTF_8),
                        new int[]{5, ("renamed-" + key).length()});
            }
            for (int i = 0; i < 3_000; i++) {
                table.delete(random.nextInt(ROWS));
            }
            table.upsertColumns(batch(ROWS, 2_000, new Random(13)));
            table.flush();
        }
        TreeMap<String, String> hashes = new TreeMap<>();
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.filter(path -> path.toString().endsWith(".parquet")).toList()) {
                hashes.put(codec + "/" + file.getFileName(), sha256(Files.readAllBytes(file)));
            }
        }
        String record = System.getProperty(RECORD_PROPERTY);
        if (record != null) {
            List<String> lines = new ArrayList<>();
            hashes.forEach((name, hash) -> lines.add(name + " " + hash));
            Files.write(Path.of(record).resolve(codec + ".txt"), lines);
            return;
        }
        TreeMap<String, String> expected = golden(codec);
        assertEquals(expected, hashes);
    }

    private static ColumnarRows batch(int first, int count, Random random) {
        long[] keys = new long[count];
        long[] amounts = new long[count];
        long[] buckets = new long[count];
        long[] scores = new long[count];
        long[] ratios = new long[count];
        int[] statuses = new int[count];
        int[] wides = new int[count];
        int[] labelOffsets = new int[count + 1];
        int[] nameOffsets = new int[count + 1];
        StringBuilder labels = new StringBuilder();
        StringBuilder names = new StringBuilder();
        for (int i = 0; i < count; i++) {
            int key = first + i;
            keys[i] = key;
            amounts[i] = key * 7_919L;
            buckets[i] = key % 10;
            double score = switch (key % 997) {
                case 0 -> Double.NaN;
                case 1 -> -0.0;
                default -> key / 7.0;
            };
            scores[i] = Double.doubleToRawLongBits(score);
            ratios[i] = Double.doubleToRawLongBits(1.0 + random.nextDouble());
            statuses[i] = key % 3;
            wides[i] = random.nextInt();
            labels.append(STATUSES[key % STATUSES.length]);
            labelOffsets[i + 1] = labels.length();
            names.append("order-").append(key);
            nameOffsets[i + 1] = names.length();
        }
        return new ColumnarRows(count, MemorySegment.ofArray(keys),
                List.of(MemorySegment.ofArray(amounts), MemorySegment.ofArray(buckets),
                        MemorySegment.ofArray(scores), MemorySegment.ofArray(ratios)),
                List.of(MemorySegment.ofArray(statuses), MemorySegment.ofArray(wides)),
                List.of(MemorySegment.ofArray(labelOffsets), MemorySegment.ofArray(nameOffsets)),
                List.of(MemorySegment.ofArray(labels.toString().getBytes(StandardCharsets.UTF_8)),
                        MemorySegment.ofArray(names.toString().getBytes(StandardCharsets.UTF_8))));
    }

    private static TreeMap<String, String> golden(ParquetCodec codec) throws IOException {
        var stream = ParquetGoldenOutputTest.class.getResourceAsStream(GOLDEN);
        assertNotNull(stream, "missing " + GOLDEN);
        TreeMap<String, String> hashes = new TreeMap<>();
        try (stream) {
            for (String line : new String(stream.readAllBytes(), StandardCharsets.UTF_8).split("\\R")) {
                if (line.startsWith(codec + "/")) {
                    String[] parts = line.split(" ");
                    hashes.put(parts[0], parts[1]);
                }
            }
        }
        return hashes;
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
