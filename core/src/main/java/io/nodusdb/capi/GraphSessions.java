package io.nodusdb.capi;

public final class GraphSessions {

    private final HandleTable<GraphSession> table = new HandleTable<>();

    public long open() {
        return table.open(new GraphSession());
    }

    public GraphSession get(long handle) {
        return table.get(handle);
    }

    public void close(long handle) {
        table.close(handle);
    }
}
