package io.nodusdb.kernel.memory;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessorGuardTest {

    private static final Path SOURCES = Path.of("src", "main", "java");
    private static final Set<String> ALLOWED = Set.of(
            "io/nodusdb/kernel/memory/NativeMemory.java",
            "io/nodusdb/kernel/memory/NativeMemorySelfTest.java");
    private static final Pattern INDEXED = Pattern.compile("\\.(getAtIndex|setAtIndex)\\s*\\(");
    private static final Pattern LAYOUT_FIELD =
            Pattern.compile("ValueLayout(?:\\.Of\\w+)?\\s+(\\w+)\\s*=");
    private static final Pattern ACCESS = Pattern.compile("\\.(get|set)\\s*\\(\\s*([\\w.]+)\\s*,");

    @Test
    void segmentAccessorsAreCalledOnlyThroughNativeMemory() throws IOException {
        List<String> violations = new ArrayList<>();
        try (Stream<Path> files = Files.walk(SOURCES)) {
            for (Path file : files.filter(path -> path.toString().endsWith(".java")).toList()) {
                String relative = SOURCES.relativize(file).toString().replace('\\', '/');
                if (!ALLOWED.contains(relative)) {
                    violations.addAll(violationsIn(relative, Files.readString(file, StandardCharsets.UTF_8)));
                }
            }
        }
        assertEquals(List.of(), violations,
                "MemorySegment get/set accessors cost about 300 ns per call in the native library; "
                        + "call io.nodusdb.kernel.memory.NativeMemory instead");
    }

    @Test
    void theGuardCatchesEachSpelling() {
        String source = """
                private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
                long a = segment.getAtIndex(LONG, 1);
                segment.setAtIndex(ValueLayout.JAVA_INT, 2, 3);
                byte b = segment.get(ValueLayout.JAVA_BYTE, 4);
                segment.set(LONG, 8, 9L);
                Object c = map.get(key, other);
                long d = NativeMemory.getLong(segment, 0);
                """;

        List<String> found = violationsIn("Example.java", source);

        assertEquals(4, found.size(), found.toString());
        assertTrue(found.get(0).startsWith("Example.java:2"), found.toString());
    }

    private static List<String> violationsIn(String name, String source) {
        Set<String> layouts = new HashSet<>();
        Matcher declared = LAYOUT_FIELD.matcher(source);
        while (declared.find()) {
            layouts.add(declared.group(1));
        }
        List<String> violations = new ArrayList<>();
        String[] lines = source.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            boolean indexed = INDEXED.matcher(line).find();
            boolean layoutAccess = false;
            Matcher access = ACCESS.matcher(line);
            while (access.find()) {
                String argument = access.group(2);
                if (argument.startsWith("ValueLayout.") || layouts.contains(argument)) {
                    layoutAccess = true;
                }
            }
            if (indexed || layoutAccess) {
                violations.add(name + ":" + (i + 1) + ": " + line.strip());
            }
        }
        return violations;
    }
}
