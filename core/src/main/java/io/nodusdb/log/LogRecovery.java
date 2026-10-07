package io.nodusdb.log;

import io.nodusdb.error.CorruptLogException;
import io.nodusdb.log.io.ChannelWindow;
import io.nodusdb.log.io.LogChannel;
import io.nodusdb.log.io.LogFileSystem;
import io.nodusdb.log.record.RecordFormat;
import io.nodusdb.log.record.RecordReader;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.log.record.SegmentHeader;
import io.nodusdb.log.record.Verdict;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class LogRecovery {

    private record Segment(String name, long base) {
    }

    private static final int WINDOW_BYTES = 1 << 20;
    private static final int INITIAL_PENDING_BYTES = 1 << 12;

    private final LogFileSystem files;
    private final long afterLsn;
    private final ReplaySink sink;
    private final ForcedMark.Position mark;
    private final RecordReader reader = new RecordReader();
    private final RecordReader pendingReader = new RecordReader();
    private ByteBuffer pending = ByteBuffer.allocate(INITIAL_PENDING_BYTES);
    private int pendingRecords;
    private int pendingBytes;
    private long pendingFirstLsn;
    private long expectedLsn;
    private boolean commitSeen;
    private int commitSegment = -1;
    private long commitEnd;
    private long lastCommitLsn;
    private long lastCommitMicros;
    private boolean stopped;
    private long lastScannedSize;

    private LogRecovery(LogFileSystem files, long afterLsn, ReplaySink sink, ForcedMark.Position mark) {
        this.files = files;
        this.afterLsn = afterLsn;
        this.sink = sink;
        this.mark = mark;
    }

    public static RecoveryResult recover(LogFileSystem files, long afterLsn, ReplaySink sink) throws IOException {
        return new LogRecovery(files, afterLsn, sink, ForcedMark.read(files)).run();
    }

    private RecoveryResult run() throws IOException {
        List<Segment> segments = listSegments();
        for (int index = 0; index < segments.size() && !stopped; index++) {
            scan(segments, index);
        }
        requireCoverageOfMark(segments);
        return commitSeen ? keepCommitted(segments) : discardAll(segments);
    }

    private List<Segment> listSegments() throws IOException {
        List<Segment> segments = new ArrayList<>();
        for (String name : files.list()) {
            if (SegmentNames.isSegment(name)) {
                segments.add(new Segment(name, SegmentNames.baseLsnOf(name)));
            }
        }
        segments.sort((a, b) -> Long.compare(a.base(), b.base()));
        return segments;
    }

    private void scan(List<Segment> segments, int index) throws IOException {
        Segment segment = segments.get(index);
        try (LogChannel channel = files.open(segment.name())) {
            long size = channel.size();
            lastScannedSize = size;
            if (!acceptHeader(segments, index, channel, size)) {
                return;
            }
            ChannelWindow window = new ChannelWindow(channel, SegmentHeader.BYTES, size, WINDOW_BYTES);
            boolean more = true;
            while (more) {
                more = scanRecord(segment, index, window);
            }
        }
    }

    private boolean acceptHeader(List<Segment> segments, int index, LogChannel channel, long size)
            throws IOException {
        Segment segment = segments.get(index);
        ByteBuffer header = ByteBuffer.allocate(SegmentHeader.BYTES);
        int read = size >= SegmentHeader.BYTES ? channel.read(header, 0) : 0;
        if (SegmentHeader.inspect(header, 0, read) != Verdict.VALID) {
            if (index == segments.size() - 1 && !isBelowMark(segment.base(), 0)) {
                stopped = true;
                return false;
            }
            throw corrupt(segment, 0, "the segment header is damaged");
        }
        if (SegmentHeader.baseLsn(header, 0) != segment.base()) {
            throw corrupt(segment, 0, "the header names a different base LSN than the file");
        }
        if (index == 0 && segment.base() > afterLsn + 1) {
            throw corrupt(segment, 0, "the log starts at LSN " + segment.base()
                    + " but the snapshot ends at LSN " + afterLsn);
        }
        if (index > 0 && segment.base() != expectedLsn) {
            throw corrupt(segment, 0, "LSN " + expectedLsn + " is missing before this segment");
        }
        expectedLsn = segment.base();
        return true;
    }

    private boolean scanRecord(Segment segment, int index, ChannelWindow window) throws IOException {
        long offset = window.offset();
        window.has(Integer.BYTES);
        if (window.available() == 0) {
            return false;
        }
        if (window.available() >= Integer.BYTES) {
            int length = window.buffer().getInt(window.position());
            if (length >= RecordFormat.MIN_RECORD_BYTES && length <= RecordFormat.READ_LIMIT_BYTES) {
                window.has(length);
            }
        }
        reader.wrap(window.buffer(), window.position(), window.position() + window.available());
        Verdict verdict = reader.inspect();
        if (verdict != Verdict.VALID) {
            classifyDamage(segment, offset, verdict);
            return false;
        }
        if (reader.lsn() != expectedLsn) {
            throw corrupt(segment, offset, "expected LSN " + expectedLsn + " but found " + reader.lsn());
        }
        int length = reader.length();
        accept(segment, index, offset + length);
        window.advance(length);
        return true;
    }

    private void classifyDamage(Segment segment, long offset, Verdict verdict) {
        if (isBelowMark(segment.base(), offset)) {
            String detail = verdict == Verdict.INCOMPLETE ? "the record is cut short" : reader.invalidReason();
            throw corrupt(segment, offset, detail + " inside the part of the log known to be durable");
        }
        stopped = true;
    }

    private void accept(Segment segment, int index, long endOffset) {
        long lsn = reader.lsn();
        expectedLsn = lsn + 1;
        if (reader.autocommit()) {
            requireNoPending(segment, endOffset);
            boolean applies = lsn > afterLsn;
            if (applies) {
                sink.apply(reader);
            }
            commitPoint(index, endOffset, applies);
        } else if (reader.type() == RecordType.TXN_COMMIT) {
            requireMatchingCommit(segment, endOffset);
            boolean applies = reader.commitFirstLsn() > afterLsn;
            if (applies) {
                deliverPending();
            }
            clearPending();
            commitPoint(index, endOffset, applies);
        } else {
            holdPending(lsn);
        }
    }

    private void commitPoint(int segmentIndex, long endOffset, boolean applies) {
        commitSeen = true;
        commitSegment = segmentIndex;
        commitEnd = endOffset;
        lastCommitLsn = reader.lsn();
        lastCommitMicros = reader.commitMicros();
        if (applies) {
            sink.committed(lastCommitLsn, lastCommitMicros);
        }
    }

    private void holdPending(long lsn) {
        if (pendingRecords == 0) {
            pendingFirstLsn = lsn;
        }
        pendingRecords++;
        if (lsn <= afterLsn) {
            return;
        }
        int length = reader.length();
        if (pendingBytes + length > pending.capacity()) {
            ByteBuffer grown = ByteBuffer.allocate(Math.max(pendingBytes + length, pending.capacity() * 2));
            System.arraycopy(pending.array(), 0, grown.array(), 0, pendingBytes);
            pending = grown;
        }
        reader.copyRecordTo(pending, pendingBytes);
        pendingBytes += length;
    }

    private void deliverPending() {
        pendingReader.wrap(pending, 0, pendingBytes);
        while (pendingReader.hasRecord()) {
            sink.apply(pendingReader);
            pendingReader.advance();
        }
    }

    private void clearPending() {
        pendingRecords = 0;
        pendingBytes = 0;
    }

    private void requireNoPending(Segment segment, long endOffset) {
        if (pendingRecords != 0) {
            throw corrupt(segment, endOffset, "a single-record commit interrupts an open transaction");
        }
    }

    private void requireMatchingCommit(Segment segment, long endOffset) {
        boolean consistent = pendingRecords > 0 && reader.commitRecordCount() == pendingRecords
                && reader.commitFirstLsn() == pendingFirstLsn
                && reader.lsn() == pendingFirstLsn + pendingRecords;
        if (!consistent) {
            throw corrupt(segment, endOffset, "the commit record does not match the records before it");
        }
    }

    private boolean isBelowMark(long segmentBase, long offset) {
        return mark != null && mark.isAfter(segmentBase, offset);
    }

    private void requireCoverageOfMark(List<Segment> segments) {
        if (stopped || segments.isEmpty() || mark == null) {
            return;
        }
        Segment last = segments.get(segments.size() - 1);
        if (mark.isAfter(last.base(), lastScannedSize)) {
            throw new CorruptLogException("the log ends at segment " + last.name() + " offset " + lastScannedSize
                    + " but data up to segment " + mark.segmentBase() + " offset " + mark.offset()
                    + " was known to be durable");
        }
    }

    private RecoveryResult keepCommitted(List<Segment> segments) throws IOException {
        if (lastCommitLsn < afterLsn) {
            throw new CorruptLogException("the log ends at LSN " + lastCommitLsn
                    + " before the snapshot at LSN " + afterLsn);
        }
        long truncated = 0;
        List<Long> kept = new ArrayList<>();
        lowerMark(segments.get(commitSegment).base(), commitEnd);
        for (int index = 0; index < segments.size(); index++) {
            Segment segment = segments.get(index);
            if (index <= commitSegment) {
                kept.add(segment.base());
            }
            if (index == commitSegment) {
                truncated += truncateTo(segment, commitEnd);
            } else if (index > commitSegment) {
                truncated += sizeOf(segment);
                files.delete(segment.name());
            }
        }
        files.trySyncDirectory();
        return new RecoveryResult(lastCommitLsn, lastCommitMicros, kept, commitEnd, pendingRecords, truncated);
    }

    private RecoveryResult discardAll(List<Segment> segments) throws IOException {
        long truncated = 0;
        lowerMark(afterLsn + 1, 0);
        for (Segment segment : segments) {
            truncated += sizeOf(segment);
            files.delete(segment.name());
        }
        files.trySyncDirectory();
        return new RecoveryResult(afterLsn, 0, List.of(), 0, pendingRecords, truncated);
    }

    private void lowerMark(long segmentBase, long offset) throws IOException {
        if (mark != null && mark.isAfter(segmentBase, offset)) {
            try (ForcedMark writable = ForcedMark.open(files)) {
                writable.record(segmentBase, offset);
            }
        }
    }

    private long truncateTo(Segment segment, long length) throws IOException {
        try (LogChannel channel = files.open(segment.name())) {
            long size = channel.size();
            if (size <= length) {
                return 0;
            }
            channel.truncate(length);
            channel.force();
            return size - length;
        }
    }

    private long sizeOf(Segment segment) throws IOException {
        try (LogChannel channel = files.open(segment.name())) {
            return channel.size();
        }
    }

    private static CorruptLogException corrupt(Segment segment, long offset, String detail) {
        return new CorruptLogException("log segment " + segment.name() + " is corrupt at offset " + offset + ": "
                + detail);
    }
}
