package io.nodusdb.log;

import io.nodusdb.error.CorruptLogException;
import io.nodusdb.log.io.ChannelWindow;
import io.nodusdb.log.io.LogChannel;
import io.nodusdb.log.io.LogFileSystem;
import io.nodusdb.log.record.MalformedTransactionException;
import io.nodusdb.log.record.RecordFormat;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.SegmentHeader;
import io.nodusdb.log.record.TransactionTracker;
import io.nodusdb.log.record.Verdict;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class LogTailReader implements AutoCloseable {

    public record Batch(long lsnFirst, long lsnLast, byte[] records, int recordCount) {
    }

    private static final long NOT_LOCATED = -1;
    private static final int WINDOW_BYTES = 1 << 20;
    private static final int INITIAL_STAGING_BYTES = 1 << 16;

    private final LogFileSystem files;
    private final RecordReader reader = new RecordReader();
    private final TransactionTracker transaction = new TransactionTracker();
    private final Map<Long, LogChannel> channels = new HashMap<>();
    private ByteBuffer staging = ByteBuffer.allocate(INITIAL_STAGING_BYTES);
    private long nextLsn;
    private long cursorBase = NOT_LOCATED;
    private long cursorOffset;

    public LogTailReader(LogFileSystem files, long afterLsn) {
        if (afterLsn < 0) {
            throw new IllegalArgumentException("the LSN to read after must not be negative: " + afterLsn);
        }
        this.files = files;
        this.nextLsn = afterLsn + 1;
    }

    public long nextLsn() {
        return nextLsn;
    }

    public Batch read(long durableLsn, int maxBytes) throws IOException {
        if (nextLsn > durableLsn) {
            return null;
        }
        if (cursorBase == NOT_LOCATED) {
            locate();
        }
        return new Walk(durableLsn, maxBytes).run();
    }

    public long unreadBytes() throws IOException {
        long total = 0;
        long first = cursorBase == NOT_LOCATED ? firstSegmentToRead() : cursorBase;
        for (long base : segmentBases()) {
            if (base < first) {
                continue;
            }
            long size = sizeOf(base);
            long skip = base == cursorBase ? cursorOffset : SegmentHeader.BYTES;
            total += Math.max(0, size - skip);
        }
        return total;
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (LogChannel channel : channels.values()) {
            try {
                channel.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        channels.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private void locate() throws IOException {
        long base = firstSegmentToRead();
        LogChannel channel = channelFor(base);
        requireHeader(channel, base);
        ChannelWindow window = windowAt(channel, SegmentHeader.BYTES);
        long lsn = base;
        long offset = SegmentHeader.BYTES;
        while (lsn < nextLsn) {
            int length = nextRecordLength(window, base, offset);
            if (length < 0) {
                throw corrupt(base, offset, "the log ends before LSN " + nextLsn + " although it is durable");
            }
            window.advance(length);
            offset += length;
            lsn++;
        }
        cursorBase = base;
        cursorOffset = offset;
    }

    private long firstSegmentToRead() throws IOException {
        long chosen = NOT_LOCATED;
        for (long base : segmentBases()) {
            if (base <= nextLsn) {
                chosen = base;
            }
        }
        if (chosen == NOT_LOCATED) {
            throw new CorruptLogException("the log no longer holds LSN " + nextLsn);
        }
        return chosen;
    }

    private List<Long> segmentBases() throws IOException {
        List<Long> bases = new ArrayList<>();
        for (String name : files.list()) {
            if (SegmentNames.isSegment(name)) {
                bases.add(SegmentNames.baseLsnOf(name));
            }
        }
        bases.sort(Long::compare);
        return bases;
    }

    private long sizeOf(long base) throws IOException {
        LogChannel cached = channels.get(base);
        if (cached != null) {
            return cached.size();
        }
        try (LogChannel channel = files.openForRead(SegmentNames.of(base))) {
            return channel.size();
        } catch (NoSuchFileException gone) {
            return 0;
        }
    }

    private LogChannel channelFor(long base) throws IOException {
        LogChannel channel = channels.get(base);
        if (channel == null) {
            try {
                channel = files.openForRead(SegmentNames.of(base));
            } catch (NoSuchFileException gone) {
                throw new CorruptLogException("the log segment " + SegmentNames.of(base) + " is gone");
            }
            channels.put(base, channel);
        }
        return channel;
    }

    private void requireHeader(LogChannel channel, long base) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(SegmentHeader.BYTES);
        int read = channel.size() >= SegmentHeader.BYTES ? channel.read(header, 0) : 0;
        if (SegmentHeader.inspect(header, 0, read) != Verdict.VALID || SegmentHeader.baseLsn(header, 0) != base) {
            throw corrupt(base, 0, "the segment header is damaged");
        }
    }

    private ChannelWindow windowAt(LogChannel channel, long offset) throws IOException {
        return new ChannelWindow(channel, offset, channel.size(), WINDOW_BYTES);
    }

    private int nextRecordLength(ChannelWindow window, long base, long offset) throws IOException {
        window.has(Integer.BYTES);
        if (window.available() == 0) {
            return -1;
        }
        if (window.available() < Integer.BYTES) {
            throw corrupt(base, offset, "a record is cut short inside the durable part of the log");
        }
        int length = window.buffer().getInt(window.position());
        if (length >= RecordFormat.MIN_RECORD_BYTES && length <= RecordFormat.READ_LIMIT_BYTES) {
            window.has(length);
        }
        reader.wrap(window.buffer(), window.position(), window.position() + window.available());
        Verdict verdict = reader.inspect();
        if (verdict != Verdict.VALID) {
            throw corrupt(base, offset, (verdict == Verdict.INCOMPLETE ? "a record is cut short"
                    : reader.invalidReason()) + " inside the durable part of the log");
        }
        return reader.length();
    }

    private static CorruptLogException corrupt(long base, long offset, String detail) {
        return new CorruptLogException("segment " + SegmentNames.of(base) + " at offset " + offset + ": " + detail);
    }

    private final class Walk {

        private final long durableLsn;
        private final int maxBytes;
        private final long firstLsn = nextLsn;
        private List<Long> bases;
        private long base = cursorBase;
        private long offset = cursorOffset;
        private long lsn = nextLsn;
        private ChannelWindow window;
        private int stagedBytes;
        private int stagedRecords;
        private long boundaryBase = cursorBase;
        private long boundaryOffset = cursorOffset;
        private long boundaryLsn = nextLsn - 1;
        private int boundaryBytes;
        private int boundaryRecords;

        Walk(long durableLsn, int maxBytes) {
            this.durableLsn = durableLsn;
            this.maxBytes = maxBytes;
        }

        Batch run() throws IOException {
            window = windowAt(channelFor(base), offset);
            transaction.reset();
            while (lsn <= durableLsn) {
                int length = nextRecordLength(window, base, offset);
                if (length < 0) {
                    enterNextSegment();
                    continue;
                }
                accept(length);
                if (boundaryRecords == stagedRecords && stagedBytes >= maxBytes) {
                    break;
                }
            }
            transaction.reset();
            return finish();
        }

        private void accept(int length) {
            if (reader.lsn() != lsn) {
                throw corrupt(base, offset, "expected LSN " + lsn + " but found " + reader.lsn());
            }
            if (stagedBytes + length > staging.capacity()) {
                ByteBuffer grown = ByteBuffer.allocate(Math.max(stagedBytes + length, staging.capacity() * 2));
                System.arraycopy(staging.array(), 0, grown.array(), 0, stagedBytes);
                staging = grown;
            }
            reader.copyRecordTo(staging, stagedBytes);
            stagedBytes += length;
            stagedRecords++;
            boolean commit;
            try {
                commit = transaction.accept(reader);
            } catch (MalformedTransactionException malformed) {
                throw corrupt(base, offset, malformed.getMessage());
            }
            window.advance(length);
            offset += length;
            lsn++;
            if (commit) {
                boundaryBase = base;
                boundaryOffset = offset;
                boundaryLsn = lsn - 1;
                boundaryBytes = stagedBytes;
                boundaryRecords = stagedRecords;
            }
        }

        private void enterNextSegment() throws IOException {
            if (bases == null || !bases.contains(lsn)) {
                bases = segmentBases();
            }
            if (!bases.contains(lsn)) {
                throw corrupt(base, offset, "LSN " + lsn + " is durable but no segment starts there");
            }
            LogChannel channel = channelFor(lsn);
            requireHeader(channel, lsn);
            base = lsn;
            offset = SegmentHeader.BYTES;
            window = windowAt(channel, offset);
        }

        private Batch finish() throws IOException {
            retainOnly(boundaryBase);
            if (boundaryRecords == 0) {
                return null;
            }
            cursorBase = boundaryBase;
            cursorOffset = boundaryOffset;
            nextLsn = boundaryLsn + 1;
            return new Batch(firstLsn, boundaryLsn, Arrays.copyOf(staging.array(), boundaryBytes), boundaryRecords);
        }

        private void retainOnly(long keep) throws IOException {
            Iterator<Map.Entry<Long, LogChannel>> iterator = channels.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, LogChannel> entry = iterator.next();
                if (entry.getKey() != keep) {
                    entry.getValue().close();
                    iterator.remove();
                }
            }
        }
    }
}
