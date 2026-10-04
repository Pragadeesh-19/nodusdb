package io.nodusdb.kernel;

public interface Persistence extends AutoCloseable {

    Persistence NONE = new Persistence() {
        @Override
        public void recordAdd(long u, long v) {
        }

        @Override
        public void recordRemove(long u, long v) {
        }

        @Override
        public void checkpoint() {
            throw new IllegalStateException("graph is in memory only and has nothing to checkpoint");
        }

        @Override
        public void sync() {
            throw new IllegalStateException("graph is in memory only and has nothing to sync");
        }

        @Override
        public void beginBatch() {
        }

        @Override
        public void endBatch() {
        }

        @Override
        public void close() {
        }
    };

    void recordAdd(long u, long v);

    void recordRemove(long u, long v);

    void checkpoint();

    void sync();

    void beginBatch();

    void endBatch();

    @Override
    void close();
}
