package io.nodusdb.log;

import io.nodusdb.log.io.LogChannel;
import io.nodusdb.log.io.LogFileSystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

final class ForcedMark implements AutoCloseable {

    record Position(long segmentBase, long offset) {

        boolean isAfter(long otherSegmentBase, long otherOffset) {
            return segmentBase > otherSegmentBase || (segmentBase == otherSegmentBase && offset > otherOffset);
        }
    }

    private static final int MAGIC = 0x4E465243;
    private static final int SLOT_BYTES = 32;
    private static final int SLOTS = 2;
    private static final int CHECKSUM_OFFSET = 28;

    private final LogChannel channel;
    private final ByteBuffer slot = ByteBuffer.allocate(SLOT_BYTES);
    private long sequence;

    private ForcedMark(LogChannel channel, long sequence) {
        this.channel = channel;
        this.sequence = sequence;
    }

    static ForcedMark open(LogFileSystem files) throws IOException {
        boolean existed = files.exists(SegmentNames.FORCED_MARK);
        LogChannel channel = existed ? files.open(SegmentNames.FORCED_MARK) : files.create(SegmentNames.FORCED_MARK);
        long latest = existed ? latestSequence(channel) : 0;
        return new ForcedMark(channel, latest);
    }

    static Position read(LogFileSystem files) throws IOException {
        if (!files.exists(SegmentNames.FORCED_MARK)) {
            return null;
        }
        try (LogChannel channel = files.open(SegmentNames.FORCED_MARK)) {
            Position best = null;
            long bestSequence = -1;
            for (int index = 0; index < SLOTS; index++) {
                ByteBuffer bytes = readSlot(channel, index);
                if (bytes != null && bytes.getLong(4) > bestSequence) {
                    bestSequence = bytes.getLong(4);
                    best = new Position(bytes.getLong(12), bytes.getLong(20));
                }
            }
            return best;
        }
    }

    void record(long segmentBase, long offset) throws IOException {
        sequence++;
        slot.clear();
        slot.putInt(0, MAGIC).putLong(4, sequence).putLong(12, segmentBase).putLong(20, offset);
        slot.putInt(CHECKSUM_OFFSET, checksum(slot));
        channel.write(slot, (sequence % SLOTS) * SLOT_BYTES);
        channel.force();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private static long latestSequence(LogChannel channel) throws IOException {
        long latest = 0;
        for (int index = 0; index < SLOTS; index++) {
            ByteBuffer bytes = readSlot(channel, index);
            if (bytes != null) {
                latest = Math.max(latest, bytes.getLong(4));
            }
        }
        return latest;
    }

    private static ByteBuffer readSlot(LogChannel channel, int index) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(SLOT_BYTES);
        int read = channel.read(bytes, (long) index * SLOT_BYTES);
        boolean intact = read == SLOT_BYTES && bytes.getInt(0) == MAGIC
                && bytes.getInt(CHECKSUM_OFFSET) == checksum(bytes);
        return intact ? bytes : null;
    }

    private static int checksum(ByteBuffer bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes.array(), 0, CHECKSUM_OFFSET);
        return (int) crc.getValue();
    }
}
