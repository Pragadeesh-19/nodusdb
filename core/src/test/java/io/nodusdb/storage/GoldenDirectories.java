package io.nodusdb.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

final class GoldenDirectories {

    private static final List<String> FILES = List.of("snapshot.bin", "nodus.wal", "symbols.nodus");
    private static final List<String> VERSION_TWO_FILES = List.of("FORMAT", "snapshot.bin", "nodus.wal",
            "log/00000000000000000031.nlog", "log/FORCED");

    private GoldenDirectories() {
    }

    static void copyVersionTwo(Path target) throws IOException {
        for (String file : VERSION_TWO_FILES) {
            String resource = "/golden/v2/directory/" + file;
            try (InputStream in = GoldenDirectories.class.getResourceAsStream(resource)) {
                if (in == null) {
                    throw new IOException("missing golden resource " + resource);
                }
                Path destination = target.resolve(file);
                Files.createDirectories(destination.getParent());
                Files.copy(in, destination);
            }
        }
    }

    static void copyVersionOne(String name, Path target) throws IOException {
        for (String file : FILES) {
            String resource = "/golden/v1/" + name + "/" + file;
            try (InputStream in = GoldenDirectories.class.getResourceAsStream(resource)) {
                if (in != null) {
                    Files.copy(in, target.resolve(file));
                }
            }
        }
    }
}
