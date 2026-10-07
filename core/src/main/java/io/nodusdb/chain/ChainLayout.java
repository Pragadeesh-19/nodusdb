package io.nodusdb.chain;

import java.util.OptionalLong;

public final class ChainLayout {

    public static final String CHAIN_PREFIX = "_nodus/chain/";
    public static final String SNAPSHOT_PREFIX = "_nodus/snapshots/";
    public static final String EPOCH_PREFIX = "_nodus/epoch/";

    private static final String CHAIN_SUFFIX = ".obj";
    private static final String SNAPSHOT_SUFFIX = ".nsnap";
    private static final String EPOCH_SUFFIX = ".json";
    private static final int DIGITS = 20;

    private ChainLayout() {
    }

    public static String chainKey(long seq) {
        return CHAIN_PREFIX + padded(seq) + CHAIN_SUFFIX;
    }

    public static String snapshotKey(long lsn) {
        return SNAPSHOT_PREFIX + padded(lsn) + SNAPSHOT_SUFFIX;
    }

    public static String epochKey(long epoch) {
        return EPOCH_PREFIX + padded(epoch) + EPOCH_SUFFIX;
    }

    public static OptionalLong chainSeq(String key) {
        return parse(key, CHAIN_PREFIX, CHAIN_SUFFIX);
    }

    public static OptionalLong snapshotLsn(String key) {
        return parse(key, SNAPSHOT_PREFIX, SNAPSHOT_SUFFIX);
    }

    public static OptionalLong epochNumber(String key) {
        return parse(key, EPOCH_PREFIX, EPOCH_SUFFIX);
    }

    private static String padded(long number) {
        if (number < 0) {
            throw new IllegalArgumentException("a layout number must not be negative: " + number);
        }
        String digits = Long.toString(number);
        return "0".repeat(DIGITS - digits.length()) + digits;
    }

    private static OptionalLong parse(String key, String prefix, String suffix) {
        if (!key.startsWith(prefix) || !key.endsWith(suffix)
                || key.length() != prefix.length() + DIGITS + suffix.length()) {
            return OptionalLong.empty();
        }
        String digits = key.substring(prefix.length(), prefix.length() + DIGITS);
        for (int i = 0; i < digits.length(); i++) {
            if (digits.charAt(i) < '0' || digits.charAt(i) > '9') {
                return OptionalLong.empty();
            }
        }
        try {
            return OptionalLong.of(Long.parseLong(digits));
        } catch (NumberFormatException overflow) {
            return OptionalLong.empty();
        }
    }
}
