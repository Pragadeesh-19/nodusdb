package io.nodusdb.lake;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

final class SnappyCompressor implements AutoCloseable {

    private static final int MIN_MATCH = 4;
    private static final int MAX_COPY_LENGTH = 64;
    private static final int TWO_BYTE_OFFSET_LIMIT = 1 << 16;
    private static final int HASH_BITS = 14;
    private static final int HASH_MULTIPLIER = 0x1E35A7BD;
    private static final int TAG_COPY_TWO_BYTE_OFFSET = 2;
    private static final int TAG_COPY_FOUR_BYTE_OFFSET = 3;
    private static final int SKIP_SHIFT = 5;
    private static final int MAX_INPUT_BYTES = Integer.MAX_VALUE - 8;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final ValueLayout.OfInt INT_SLOT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfInt INT_LITTLE_ENDIAN =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private final NativeColumn table;

    SnappyCompressor() {
        this.table = new NativeColumn((1L << HASH_BITS) * Integer.BYTES);
    }

    void compress(MemorySegment input, NativeSink out) {
        if (input.byteSize() > MAX_INPUT_BYTES) {
            throw new IllegalArgumentException("snappy input exceeds " + MAX_INPUT_BYTES + " bytes");
        }
        int size = (int) input.byteSize();
        MemorySegment slots = table.segment();
        slots.fill((byte) 0);
        out.putVarint(size);
        int literalStart = 0;
        int position = 0;
        int misses = 0;
        int lastCandidate = size - MIN_MATCH;
        while (position <= lastCandidate) {
            int hash = hash(input.get(INT_LITTLE_ENDIAN, position));
            int candidate = slots.getAtIndex(INT_SLOT, hash) - 1;
            slots.setAtIndex(INT_SLOT, hash, position + 1);
            if (candidate >= 0 && input.get(INT_LITTLE_ENDIAN, candidate) == input.get(INT_LITTLE_ENDIAN, position)) {
                int matched = MIN_MATCH;
                while (position + matched < size
                        && input.get(BYTE, candidate + matched) == input.get(BYTE, position + matched)) {
                    matched++;
                }
                literal(out, input, literalStart, position);
                copy(out, position - candidate, matched);
                position += matched;
                literalStart = position;
                misses = 0;
            } else {
                position += 1 + (misses++ >>> SKIP_SHIFT);
            }
        }
        literal(out, input, literalStart, size);
    }

    @Override
    public void close() {
        table.close();
    }

    private static void literal(NativeSink out, MemorySegment input, int from, int to) {
        int length = to - from;
        if (length == 0) {
            return;
        }
        int encoded = length - 1;
        if (encoded < 60) {
            out.put(encoded << 2);
        } else {
            int extra = encoded < (1 << 8) ? 1 : encoded < (1 << 16) ? 2 : encoded < (1 << 24) ? 3 : 4;
            out.put((59 + extra) << 2);
            out.putLittleEndian(encoded, extra);
        }
        out.write(input, from, length);
    }

    private static void copy(NativeSink out, int offset, int length) {
        int remaining = length;
        while (remaining > 0) {
            int piece = Math.min(remaining, MAX_COPY_LENGTH);
            if (offset < TWO_BYTE_OFFSET_LIMIT) {
                out.put(((piece - 1) << 2) | TAG_COPY_TWO_BYTE_OFFSET);
                out.putLittleEndian(offset, 2);
            } else {
                out.put(((piece - 1) << 2) | TAG_COPY_FOUR_BYTE_OFFSET);
                out.putLittleEndian(offset, 4);
            }
            remaining -= piece;
        }
    }

    private static int hash(int value) {
        return (value * HASH_MULTIPLIER) >>> (Integer.SIZE - HASH_BITS);
    }
}
