package io.nodusdb.kernel.changes;

public final class ChangeEntry {

    public static final int LONGS = 3;

    public static final int ADD = 1;
    public static final int REMOVE = 2;
    public static final int SCHEMA = 3;
    public static final int EPOCH = 4;
    public static final int ERASE = 5;
    public static final int COMMIT = 6;

    private static final int KIND_SHIFT = 56;
    private static final int RELATION_SHIFT = 40;
    private static final int SUBJECT_RELATION_SHIFT = 24;
    private static final long KIND_MASK = 0xFF;
    private static final long RELATION_MASK = 0xFFFF;
    private static final long ID_MASK = 0xFFFF_FFFFL;
    private static final int OBJECT_SHIFT = 32;

    private ChangeEntry() {
    }

    public static long meta(int kind, int relation, int subjectRelation) {
        return (kind & KIND_MASK) << KIND_SHIFT
                | (relation & RELATION_MASK) << RELATION_SHIFT
                | (subjectRelation & RELATION_MASK) << SUBJECT_RELATION_SHIFT;
    }

    public static long ids(int object, int subject) {
        return (object & ID_MASK) << OBJECT_SHIFT | subject & ID_MASK;
    }

    public static int kind(long meta) {
        return (int) (meta >>> KIND_SHIFT & KIND_MASK);
    }

    public static int relation(long meta) {
        return (int) (meta >>> RELATION_SHIFT & RELATION_MASK);
    }

    public static int subjectRelation(long meta) {
        return (int) (meta >>> SUBJECT_RELATION_SHIFT & RELATION_MASK);
    }

    public static int object(long payload) {
        return (int) (payload >>> OBJECT_SHIFT);
    }

    public static int subject(long payload) {
        return (int) (payload & ID_MASK);
    }

    public static boolean isTuple(int kind) {
        return kind == ADD || kind == REMOVE;
    }
}
