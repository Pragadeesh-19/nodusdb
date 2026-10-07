package io.nodusdb.log.io;

import java.io.IOException;
import java.util.List;

public interface LogFileSystem {

    List<String> list() throws IOException;

    boolean exists(String name) throws IOException;

    LogChannel create(String name) throws IOException;

    LogChannel open(String name) throws IOException;

    void delete(String name) throws IOException;

    void trySyncDirectory() throws IOException;
}
