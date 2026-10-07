package io.nodusdb.lake.parquet;

import io.nodusdb.lake.UpsertArrays;
import io.nodusdb.lake.buffer.DeltaMemTable;
import io.nodusdb.lake.buffer.RowSelection;
import io.nodusdb.lake.codec.ParquetCodec;
import io.nodusdb.lake.model.LakeSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetGoldenTest {

    private static final LakeSchema ALL_TYPES = new LakeSchema(List.of(
            new LakeSchema.Field("amount", LakeSchema.Type.INT64),
            new LakeSchema.Field("score", LakeSchema.Type.DOUBLE),
            new LakeSchema.Field("status", LakeSchema.Type.INT32),
            new LakeSchema.Field("label", LakeSchema.Type.UTF8)));
    private static final String[] LABELS = {"gamma", "alpha", "", "beta"};

    private enum Case {
        SMALL_SNAPPY("9d33da3bfe4e39e11ad5b3ab75ebe9013fdf683bbf39dd895fceaa6324d40d7a"),
        SMALL_UNCOMPRESSED("594e9b8ea13eddef2b4e598b98307e161f22634743f1b450bba15bb7f770be79"),
        ONE_ROW("03360dc4ba8a5c2da9ee6bb7a36c3595855a724e3c28fe21cf26220389c5bf70"),
        KEYS_ONLY("ed23dc8aa82bb9480829eac4754f6ffc4116eaf510e7b39f3ffcf40bde57dc23"),
        EXACTLY_ONE_ROW_GROUP("f70a0a6f22ae7d9f908ec5c35da3437b2f8835a189b16925fa312b1795f59f64"),
        ONE_ROW_OVER_A_GROUP("d3665f534689b1a7c31a71208d8a6d055492d16531d39b4d37ee24c6034b50e0"),
        TWO_AND_A_BIT_GROUPS("8a64b88cffeca94f259ef39e0567856754c024c789571abf6881ddcd2b485fa3"),
        REORDERED_SELECTION("61f4df20801be228c848bb1a46598900451505f9d941d333a6eb4e10d0765846"),
        HIGH_CARDINALITY_STRINGS("d61cd4fa8b66662d47daf27d57cd7e93eb2e2c34f9da773710cfa9c5249688bc"),
        EMPTY_AND_LONG_STRINGS("8b383d7ca1e0d888192e2b048c64bd07be3184ac4b4b9bc2d33654928a217de7");

        final String sha256;

        Case(String sha256) {
            this.sha256 = sha256;
        }
    }

    private static DeltaMemTable fill(LakeSchema schema, int rows, int capacity) {
        DeltaMemTable table = new DeltaMemTable(schema.memtableSchema(), capacity, 1 << 24);
        for (int i = 0; i < rows; i++) {
            long[] longs = {i * 7_919L - 5_000L, Double.doubleToRawLongBits(i * 0.5 - 3.25)};
            int[] ints = {i % 5 - 2};
            byte[] label = LABELS[i % LABELS.length].getBytes(StandardCharsets.UTF_8);
            if (schema.fields().isEmpty()) {
                UpsertArrays.upsert(table, i * 31L + 1, new long[0], new int[0], new byte[0], new int[0]);
            } else {
                UpsertArrays.upsert(table, i * 31L + 1, longs, ints, label, new int[] {label.length});
            }
        }
        return table;
    }

    private static byte[] write(Case which, Path directory) throws IOException {
        Path file = directory.resolve(which.name() + ".parquet");
        switch (which) {
            case SMALL_SNAPPY -> writeAll(file, ALL_TYPES, fill(ALL_TYPES, 10, 64), ParquetCodec.SNAPPY);
            case SMALL_UNCOMPRESSED -> writeAll(file, ALL_TYPES, fill(ALL_TYPES, 10, 64), ParquetCodec.UNCOMPRESSED);
            case ONE_ROW -> writeAll(file, ALL_TYPES, fill(ALL_TYPES, 1, 64), ParquetCodec.SNAPPY);
            case KEYS_ONLY -> writeAll(file, LakeSchema.KEYS_ONLY, fill(LakeSchema.KEYS_ONLY, 100, 256),
                    ParquetCodec.SNAPPY);
            case EXACTLY_ONE_ROW_GROUP -> writeAll(file, ALL_TYPES,
                    fill(ALL_TYPES, ParquetWriter.ROW_GROUP_ROWS, 1 << 18), ParquetCodec.SNAPPY);
            case ONE_ROW_OVER_A_GROUP -> writeAll(file, ALL_TYPES,
                    fill(ALL_TYPES, ParquetWriter.ROW_GROUP_ROWS + 1, 1 << 18), ParquetCodec.SNAPPY);
            case TWO_AND_A_BIT_GROUPS -> writeAll(file, ALL_TYPES,
                    fill(ALL_TYPES, 2 * ParquetWriter.ROW_GROUP_ROWS + 1_000, 1 << 18), ParquetCodec.UNCOMPRESSED);
            case REORDERED_SELECTION -> writeReordered(file);
            case HIGH_CARDINALITY_STRINGS -> writeStrings(file, 5_000, false);
            case EMPTY_AND_LONG_STRINGS -> writeStrings(file, 300, true);
        }
        return Files.readAllBytes(file);
    }

    private static void writeAll(Path file, LakeSchema schema, DeltaMemTable table, ParquetCodec codec)
            throws IOException {
        int[] rows = new int[table.size()];
        for (int row = 0; row < rows.length; row++) {
            rows[row] = row;
        }
        try (RowSelection selection = RowSelection.of(rows)) {
            ParquetWriter.write(file, schema, table, selection, codec);
        }
    }

    private static void writeReordered(Path file) throws IOException {
        DeltaMemTable table = fill(ALL_TYPES, 40, 128);
        int[] rows = new int[25];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = (i * 7 + 3) % 40;
        }
        try (RowSelection selection = RowSelection.of(rows)) {
            ParquetWriter.write(file, ALL_TYPES, table, selection, ParquetCodec.SNAPPY);
        }
    }

    private static void writeStrings(Path file, int rows, boolean extremes) throws IOException {
        LakeSchema schema = new LakeSchema(List.of(new LakeSchema.Field("text", LakeSchema.Type.UTF8)));
        DeltaMemTable table = new DeltaMemTable(schema.memtableSchema(), 8_192, 1 << 24);
        for (int i = 0; i < rows; i++) {
            String text;
            if (extremes) {
                text = i % 3 == 0 ? "" : "x".repeat(1 + (i * 37) % 700) + i;
            } else {
                text = "value-" + (i * 2_654_435_761L);
            }
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            UpsertArrays.upsert(table, i + 1L, new long[0], new int[0], bytes, new int[] {bytes.length});
        }
        writeAll(file, schema, table, ParquetCodec.SNAPPY);
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @ParameterizedTest
    @EnumSource(Case.class)
    void theWriterOutputIsByteForByteWhatItWasBeforeTheColumnSourceRefactor(Case which) throws IOException {
        Path directory = Files.createTempDirectory("parquet-golden");
        try {
            assertEquals(which.sha256, sha256(write(which, directory)), which.name());
        } finally {
            try (var files = Files.list(directory)) {
                files.forEach(path -> path.toFile().delete());
            }
            directory.toFile().delete();
        }
    }
}
