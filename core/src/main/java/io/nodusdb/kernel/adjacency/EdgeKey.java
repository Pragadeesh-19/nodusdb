package io.nodusdb.kernel.adjacency;

public final class EdgeKey {

    public static final int MAX_RELATION = 0xFFFF;

    private static final int RELATION_SHIFT = 47;
    private static final int SUBJECT_RELATION_SHIFT = 31;
    private static final long ID_MASK = 0x7FFF_FFFFL;
    private static final long RELATION_MASK = 0xFFFFL;

    private EdgeKey() {
    }

    public static long pack(int relation, int subjectRelation, int id) {
        return ((long) relation << RELATION_SHIFT) | ((long) subjectRelation << SUBJECT_RELATION_SHIFT)
                | (id & ID_MASK);
    }

    public static int relation(long key) {
        return (int) (key >>> RELATION_SHIFT & RELATION_MASK);
    }

    public static int subjectRelation(long key) {
        return (int) (key >>> SUBJECT_RELATION_SHIFT & RELATION_MASK);
    }

    public static int id(long key) {
        return (int) (key & ID_MASK);
    }
}
