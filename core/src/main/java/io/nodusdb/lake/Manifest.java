package io.nodusdb.lake;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

final class Manifest {

    static final String FILE_NAME = "manifest.txt";

    private static final String FORMAT_LINE = "nodus-lake-manifest 1";
    private static final String SCHEMA_PREFIX = "schema ";
    private static final String ENTRY_PREFIX = "entry ";
    private static final String NONE = "-";

    record Entry(long sequence, String dataFile, String deleteFile) {
    }

    private Manifest() {
    }

    static List<Entry> read(Path directory, String signature) throws IOException {
        Path file = directory.resolve(FILE_NAME);
        List<Entry> entries = new ArrayList<>();
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.size() < 2 || !FORMAT_LINE.equals(lines.get(0))) {
            throw new IOException("unrecognised manifest format in " + file);
        }
        if (!lines.get(1).equals(SCHEMA_PREFIX + signature)) {
            throw new IllegalStateException("table schema does not match the manifest in " + directory);
        }
        for (int i = 2; i < lines.size(); i++) {
            entries.add(parseEntry(lines.get(i), file));
        }
        return entries;
    }

    static void write(Path directory, String signature, List<Entry> entries) throws IOException {
        StringBuilder content = new StringBuilder();
        content.append(FORMAT_LINE).append('\n');
        content.append(SCHEMA_PREFIX).append(signature).append('\n');
        for (Entry entry : entries) {
            content.append(ENTRY_PREFIX).append(entry.sequence()).append(' ')
                    .append(entry.dataFile() == null ? NONE : entry.dataFile()).append(' ')
                    .append(entry.deleteFile() == null ? NONE : entry.deleteFile()).append('\n');
        }
        Path target = directory.resolve(FILE_NAME);
        Path temporary = directory.resolve(FILE_NAME + ".tmp");
        Files.writeString(temporary, content, StandardCharsets.UTF_8);
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private static Entry parseEntry(String line, Path file) throws IOException {
        if (!line.startsWith(ENTRY_PREFIX)) {
            throw new IOException("malformed manifest line in " + file + ": " + line);
        }
        String[] parts = line.substring(ENTRY_PREFIX.length()).split(" ");
        if (parts.length != 3) {
            throw new IOException("malformed manifest line in " + file + ": " + line);
        }
        return new Entry(Long.parseLong(parts[0]),
                NONE.equals(parts[1]) ? null : parts[1],
                NONE.equals(parts[2]) ? null : parts[2]);
    }
}
