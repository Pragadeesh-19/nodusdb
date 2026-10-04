package io.nodusdb.kernel;

public final class NodeIds {

    public static final long MAX_NODE_ID = Integer.MAX_VALUE - 9L;
    static final long NONE = -1L;

    private NodeIds() {
    }

    public static long checkValid(long id) {
        if (id < 0 || id > MAX_NODE_ID) {
            throw new IllegalArgumentException("node id out of range [0, " + MAX_NODE_ID + "]: " + id);
        }
        return id;
    }
}
