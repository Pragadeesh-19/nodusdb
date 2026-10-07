package io.nodusdb.log;

final class SegmentNames {

    static final String SUFFIX = ".nlog";
    static final String FORCED_MARK = "FORCED";

    private static final int DIGITS = 20;

    private SegmentNames() {
    }

    static String of(long baseLsn) {
        return String.format("%0" + DIGITS + "d%s", baseLsn, SUFFIX);
    }

    static boolean isSegment(String name) {
        if (!name.endsWith(SUFFIX) || name.length() != DIGITS + SUFFIX.length()) {
            return false;
        }
        for (int i = 0; i < DIGITS; i++) {
            if (!Character.isDigit(name.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    static long baseLsnOf(String name) {
        return Long.parseLong(name.substring(0, DIGITS));
    }
}
