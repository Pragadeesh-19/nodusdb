package io.nodusdb.lake.codec;

import java.io.IOException;

public final class SnappyCodec {

    private static final int TAG_LITERAL = 0;
    private static final int TAG_COPY_ONE_BYTE_OFFSET = 1;
    private static final int TAG_COPY_TWO_BYTE_OFFSET = 2;
    private static final int TAG_COPY_FOUR_BYTE_OFFSET = 3;
    private static final int MAX_DECOMPRESSED_LENGTH = Integer.MAX_VALUE - 8;

    private SnappyCodec() {
    }

    public static byte[] decompress(byte[] input) throws IOException {
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
}
