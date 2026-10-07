package io.nodusdb.objectstore;

import java.util.Map;
import java.util.Set;

public final class ObjectKeys {

    public static final int MAX_KEY_LENGTH = 1024;
    public static final int MAX_SEGMENT_LENGTH = 255;
    public static final int MAX_METADATA_ENTRIES = 16;
    public static final int MAX_METADATA_NAME_LENGTH = 64;
    public static final int MAX_METADATA_VALUE_LENGTH = 256;

    private static final char SEPARATOR = '/';
    private static final Set<String> RESERVED_DEVICE_NAMES = Set.of("con", "prn", "aux", "nul",
            "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
            "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9");

    private ObjectKeys() {
    }

    public static String requireKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("an object key must not be empty");
        }
        requirePath(key, "key", true);
        if (key.charAt(key.length() - 1) == SEPARATOR) {
            throw new IllegalArgumentException("an object key must not end with '/'");
        }
        return key;
    }

    public static String requirePrefix(String prefix) {
        if (prefix == null) {
            throw new IllegalArgumentException("a prefix must not be null");
        }
        if (!prefix.isEmpty()) {
            requirePath(prefix, "prefix", false);
        }
        return prefix;
    }

    public static Map<String, String> requireMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must not be null");
        }
        if (metadata.size() > MAX_METADATA_ENTRIES) {
            throw new IllegalArgumentException("metadata holds more than " + MAX_METADATA_ENTRIES + " entries");
        }
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            requireMetadataName(entry.getKey());
            requireMetadataValue(entry.getKey(), entry.getValue());
        }
        return metadata;
    }

    private static void requirePath(String path, String what, boolean lastSegmentComplete) {
        if (path.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("a " + what + " is longer than " + MAX_KEY_LENGTH + " characters");
        }
        if (path.charAt(0) == SEPARATOR) {
            throw new IllegalArgumentException("a " + what + " must not start with '/'");
        }
        int segmentStart = 0;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == SEPARATOR) {
                requireSegment(path, segmentStart, i, what, true);
                segmentStart = i + 1;
            } else if (!isAllowed(c)) {
                throw new IllegalArgumentException("a " + what + " holds a character outside [a-z0-9._-/] at "
                        + i);
            }
        }
        if (segmentStart < path.length()) {
            requireSegment(path, segmentStart, path.length(), what, lastSegmentComplete);
        }
    }

    private static void requireSegment(String path, int from, int to, String what, boolean complete) {
        if (from == to) {
            throw new IllegalArgumentException("a " + what + " must not hold an empty segment");
        }
        if (to - from > MAX_SEGMENT_LENGTH) {
            throw new IllegalArgumentException("a " + what + " segment is longer than " + MAX_SEGMENT_LENGTH
                    + " characters");
        }
        if (path.charAt(from) == '.') {
            throw new IllegalArgumentException("a " + what + " segment must not start with '.'");
        }
        if (complete && path.charAt(to - 1) == '.') {
            throw new IllegalArgumentException("a " + what + " segment must not end with '.'");
        }
        if (complete && isReservedDeviceName(path.substring(from, to))) {
            throw new IllegalArgumentException("a " + what + " segment must not be a reserved device name");
        }
    }

    private static boolean isReservedDeviceName(String segment) {
        int dot = segment.indexOf('.');
        return RESERVED_DEVICE_NAMES.contains(dot < 0 ? segment : segment.substring(0, dot));
    }

    private static boolean isAllowed(char c) {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
    }

    private static void requireMetadataName(String name) {
        if (name == null || name.isEmpty() || name.length() > MAX_METADATA_NAME_LENGTH) {
            throw new IllegalArgumentException("a metadata name must hold 1 to " + MAX_METADATA_NAME_LENGTH
                    + " characters");
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) {
                throw new IllegalArgumentException("a metadata name must match [a-z0-9-]");
            }
        }
    }

    private static void requireMetadataValue(String name, String value) {
        if (value == null || value.length() > MAX_METADATA_VALUE_LENGTH) {
            throw new IllegalArgumentException("the value of metadata '" + name + "' must hold at most "
                    + MAX_METADATA_VALUE_LENGTH + " characters");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x20 || c > 0x7e) {
                throw new IllegalArgumentException("the value of metadata '" + name + "' must be printable ASCII");
            }
        }
        if (!value.isEmpty() && (value.charAt(0) == ' ' || value.charAt(value.length() - 1) == ' ')) {
            throw new IllegalArgumentException("the value of metadata '" + name
                    + "' must not start or end with a space");
        }
    }
}
