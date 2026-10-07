package io.nodusdb.lake.parquet;

import io.nodusdb.lake.codec.NativeSink;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class DictionaryIndexCodec {

    private static final int MAX_BIT_WIDTH = 32;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;

    private DictionaryIndexCodec() {
    }

    static int bitWidthFor(int dictionarySize) {
        return Math.max(1, Integer.SIZE - Integer.numberOfLeadingZeros(Math.max(0, dictionarySize - 1)));
    }

    static void encode(MemorySegment indices, int count, int dictionarySize, NativeSink out) {
        int width = bitWidthFor(dictionarySize);
        int groups = (count + 7) / 8;
        out.put(width);
        out.putVarint(((long) groups << 1) | 1L);
        long buffer = 0;
        int bits = 0;
        int slots = groups * 8;
        for (int i = 0; i < slots; i++) {
            int value = i < count ? indices.getAtIndex(INT, i) : 0;
            buffer |= (long) value << bits;
            bits += width;
            while (bits >= Byte.SIZE) {
                out.put((int) buffer);
                buffer >>>= Byte.SIZE;
                bits -= Byte.SIZE;
            }
        }
    }

    static int[] decode(byte[] data, int offset, int length, int count) throws IOException {
        if (length < 1) {
            throw new IOException("dictionary index page is empty");
        }
        int end = offset + length;
        int width = data[offset] & 0xFF;
        if (width > MAX_BIT_WIDTH) {
            throw new IOException("dictionary index bit width " + width + " is too large");
        }
        long mask = width == MAX_BIT_WIDTH ? 0xFFFF_FFFFL : (1L << width) - 1;
        int[] values = new int[count];
        Cursor cursor = new Cursor(offset + 1);
        int produced = 0;
        while (produced < count) {
            long header = readVarint(data, cursor, end);
            if ((header & 1L) == 1L) {
                long groups = header >>> 1;
                if (groups * width > end - cursor.position) {
                    throw new IOException("bit-packed run overruns the page");
                }
                long buffer = 0;
                int bits = 0;
                long slots = groups * 8;
                for (long slot = 0; slot < slots; slot++) {
                    while (bits < width) {
                        buffer |= (long) (data[cursor.position++] & 0xFF) << bits;
                        bits += Byte.SIZE;
                    }
                    int value = (int) (buffer & mask);
                    buffer >>>= width;
                    bits -= width;
                    if (produced < count) {
                        values[produced++] = value;
                    }
                }
            } else {
                long run = header >>> 1;
                int bytesPerValue = (width + 7) / 8;
                if (bytesPerValue > end - cursor.position) {
                    throw new IOException("run-length value overruns the page");
                }
                int value = 0;
                for (int i = 0; i < bytesPerValue; i++) {
                    value |= (data[cursor.position + i] & 0xFF) << (Byte.SIZE * i);
                }
                cursor.position += bytesPerValue;
                for (long r = 0; r < run && produced < count; r++) {
                    values[produced++] = value;
                }
            }
        }
        return values;
    }

    private static long readVarint(byte[] data, Cursor cursor, int end) throws IOException {
        long value = 0;
        int shift = 0;
        while (true) {
            if (cursor.position >= end || shift > 63) {
                throw new IOException("truncated run header");
            }
            int b = data[cursor.position++] & 0xFF;
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
    }

    private static final class Cursor {

        int position;

        Cursor(int position) {
            this.position = position;
        }
    }
}
