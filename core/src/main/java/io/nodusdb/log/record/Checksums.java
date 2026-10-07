package io.nodusdb.log.record;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

final class Checksums {

    private Checksums() {
    }

    static int crc32c(CRC32C crc, ByteBuffer buffer, int from, int to) {
        crc.reset();
        if (buffer.hasArray()) {
            crc.update(buffer.array(), buffer.arrayOffset() + from, to - from);
        } else {
            int savedPosition = buffer.position();
            int savedLimit = buffer.limit();
            buffer.limit(to).position(from);
            crc.update(buffer);
            buffer.limit(savedLimit).position(savedPosition);
        }
        return (int) crc.getValue();
    }
}
