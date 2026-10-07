package io.nodusdb.log.record;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public final class SegmentHeader {

    public static final int BYTES = 32;
    public static final int MAGIC = 0x4E4F4455;
    public static final short VERSION = 2;

    private static final int VERSION_OFFSET = 4;
    private static final int FLAGS_OFFSET = 6;
    private static final int CREATED_OFFSET = 8;
    private static final int BASE_LSN_OFFSET = 16;
    private static final int CHECKSUM_OFFSET = 24;
    private static final int RESERVED_OFFSET = 28;

    private SegmentHeader() {
    }

    public static void write(ByteBuffer destination, int position, long createdMicros, long baseLsn) {
        destination.putInt(position, MAGIC);
        destination.putShort(position + VERSION_OFFSET, VERSION);
        destination.putShort(position + FLAGS_OFFSET, (short) 0);
        destination.putLong(position + CREATED_OFFSET, createdMicros);
        destination.putLong(position + BASE_LSN_OFFSET, baseLsn);
        destination.putInt(position + CHECKSUM_OFFSET, Checksums.crc32c(new CRC32C(), destination, position,
                position + CHECKSUM_OFFSET));
        destination.putInt(position + RESERVED_OFFSET, 0);
    }

    public static Verdict inspect(ByteBuffer source, int position, int end) {
        if (end - position < BYTES) {
            return Verdict.INCOMPLETE;
        }
        boolean intact = source.getInt(position) == MAGIC
                && source.getShort(position + VERSION_OFFSET) == VERSION
                && source.getShort(position + FLAGS_OFFSET) == 0
                && source.getInt(position + RESERVED_OFFSET) == 0
                && source.getLong(position + BASE_LSN_OFFSET) >= 1
                && source.getInt(position + CHECKSUM_OFFSET)
                == Checksums.crc32c(new CRC32C(), source, position, position + CHECKSUM_OFFSET);
        return intact ? Verdict.VALID : Verdict.INVALID;
    }

    public static long baseLsn(ByteBuffer source, int position) {
        return source.getLong(position + BASE_LSN_OFFSET);
    }

    public static long createdMicros(ByteBuffer source, int position) {
        return source.getLong(position + CREATED_OFFSET);
    }
}
