package io.nodusdb.log.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface LogChannel extends Closeable {

    int read(ByteBuffer destination, long position) throws IOException;

    int write(ByteBuffer source, long position) throws IOException;

    long size() throws IOException;

    void truncate(long size) throws IOException;

    void force() throws IOException;
}
