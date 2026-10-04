package io.nodusdb.lake;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

final class SnappyCodec {

    private static final int MIN_MATCH = 4;
    private static final int MAX_COPY_LENGTH = 64;
    private static final int TWO_BYTE_OFFSET_LIMIT = 1 << 16;
    private static final int HASH_BITS = 14;
    private static final int HASH_MULTIPLIER = 0x1E35A7BD;
    private static final int TAG_LITERAL = 0;
    private static final int TAG_COPY_ONE_BYTE_OFFSET = 1;
    private static final int TAG_COPY_TWO_BYTE_OFFSET = 2;
    private static final int TAG_COPY_FOUR_BYTE_OFFSET = 3;
    private static final int MAX_DECOMPRESSED_LENGTH = Integer.MAX_VALUE - 8;
    private static final int SKIP_SHIFT = 5;
    private static final VarHandle LITTLE_ENDIAN_INT =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private SnappyCodec() {
    }

    static byte[] compress(byte[] input) {
        Output out = new Output(input.length / 2 + 16);
        out.putVarint(input.length);
        int[] table = new int[1 << HASH_BITS];
        Arrays.fill(table, -1);
        int literalStart = 0;
        int position = 0;
        int misses = 0;
        int lastCandidate = input.length - MIN_MATCH;
        while (position <= lastCandidate) {
            int hash = hash(read32(input, position));
            int candidate = table[hash];
            table[hash] = position;
            if (candidate >= 0 && read32(input, candidate) == read32(input, position)) {
                int length = MIN_MATCH;
                while (position + length < input.length && input[candidate + length] == input[position + length]) {
                    length++;
                }
                literal(out, input, literalStart, position);
                copy(out, position - candidate, length);
                position += length;
                literalStart = position;
                misses = 0;
            } else {
                position += 1 + (misses++ >>> SKIP_SHIFT);
            }
        }
        literal(out, input, literalStart, input.length);
        return out.toByteArray();
    }

    static byte[] decompress(byte[] input) throws IOException {
        int position = 0;
        long declared = 0;
        int shift = 0;
        while (true) {
            require(input, position, 1);
            int b = input[position++] & 0xFF;
            declared |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 28) {
                throw corrupt("length preamble too long");
            }
        }
        if (declared > MAX_DECOMPRESSED_LENGTH) {
            throw corrupt("declared length " + declared + " is too large");
        }
        byte[] out = new byte[(int) declared];
        int written = 0;
        while (position < input.length) {
            int tag = input[position++] & 0xFF;
            int type = tag & 0x03;
            if (type == TAG_LITERAL) {
                int length = tag >>> 2;
                if (length >= 60) {
                    int extra = length - 59;
                    require(input, position, extra);
                    length = (int) readLittleEndian(input, position, extra);
                    position += extra;
                }
                length += 1;
                require(input, position, length);
                if (length > out.length - written) {
                    throw corrupt("literal overruns declared length");
                }
                System.arraycopy(input, position, out, written, length);
                position += length;
                written += length;
            } else {
                int length;
                long offset;
                if (type == TAG_COPY_ONE_BYTE_OFFSET) {
                    require(input, position, 1);
                    length = ((tag >>> 2) & 0x07) + 4;
                    offset = ((long) (tag >>> 5) << 8) | (input[position] & 0xFF);
                    position += 1;
                } else if (type == TAG_COPY_TWO_BYTE_OFFSET) {
                    require(input, position, 2);
                    length = (tag >>> 2) + 1;
                    offset = readLittleEndian(input, position, 2);
                    position += 2;
                } else {
                    require(input, position, 4);
                    length = (tag >>> 2) + 1;
                    offset = readLittleEndian(input, position, 4);
                    position += 4;
                }
                if (offset == 0 || offset > written || length > out.length - written) {
                    throw corrupt("copy reaches outside the output");
                }
                int source = written - (int) offset;
                for (int i = 0; i < length; i++) {
                    out[written++] = out[source++];
                }
            }
        }
        if (written != out.length) {
            throw corrupt("produced " + written + " bytes, declared " + out.length);
        }
        return out;
    }

    private static void literal(Output out, byte[] input, int from, int to) {
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

    private static void copy(Output out, int offset, int length) {
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

    private static int read32(byte[] data, int position) {
        return (int) LITTLE_ENDIAN_INT.get(data, position);
    }

    private static long readLittleEndian(byte[] data, int position, int count) {
        long value = 0;
        for (int i = 0; i < count; i++) {
            value |= (long) (data[position + i] & 0xFF) << (8 * i);
        }
        return value;
    }

    private static void require(byte[] data, int position, int count) throws IOException {
        if (count < 0 || position + count > data.length) {
            throw corrupt("truncated at offset " + position);
        }
    }

    private static IOException corrupt(String reason) {
        return new IOException("corrupt snappy block: " + reason);
    }

    private static final class Output {

        private byte[] bytes;
        private int length;

        Output(int capacity) {
            bytes = new byte[Math.max(16, capacity)];
        }

        void put(int value) {
            ensure(1);
            bytes[length++] = (byte) value;
        }

        void write(byte[] source, int from, int count) {
            ensure(count);
            System.arraycopy(source, from, bytes, length, count);
            length += count;
        }

        void putLittleEndian(long value, int count) {
            ensure(count);
            for (int i = 0; i < count; i++) {
                bytes[length++] = (byte) (value >>> (8 * i));
            }
        }

        void putVarint(long value) {
            long remaining = value;
            while (remaining >= 0x80) {
                put((int) (remaining & 0x7F) | 0x80);
                remaining >>>= 7;
            }
            put((int) remaining);
        }

        byte[] toByteArray() {
            return Arrays.copyOf(bytes, length);
        }

        private void ensure(int extra) {
            if (length + extra > bytes.length) {
                bytes = Arrays.copyOf(bytes, Math.max(bytes.length * 2, length + extra));
            }
        }
    }
}
