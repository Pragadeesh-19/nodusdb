package io.nodusdb.kernel;

import java.time.Duration;
import java.util.List;

public interface ErasureProgress extends AutoCloseable {

    ErasureProgress NONE = new ErasureProgress() {
        @Override
        public void wake() {
        }

        @Override
        public void awaitErased(Token token, Duration timeout) {
        }

        @Override
        public List<Layer> layers() {
            return List.of();
        }

        @Override
        public String lastError() {
            return "";
        }

        @Override
        public void close() {
        }
    };

    record Layer(String name, long coveredLsn, boolean pending) {
    }

    void wake();

    void awaitErased(Token token, Duration timeout);

    List<Layer> layers();

    String lastError();

    @Override
    void close();
}
