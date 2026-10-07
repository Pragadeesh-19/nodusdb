package io.nodusdb.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

final class GoldenDirectories {

    private static final List<String> FILES = List.of("snapshot.bin", "nodus.wal", "symbols.nodus");

    private GoldenDirectories() {
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
