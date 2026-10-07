package io.nodusdb.kernel.memory;

public final class ChunkLayout {

    private final int first;
    private final int firstShift;

    public ChunkLayout(int first) {
        if (first < 1 || Integer.bitCount(first) != 1) {
            throw new IllegalArgumentException("first chunk size must be a positive power of two: " + first);
        }
        this.first = first;
        this.firstShift = Integer.numberOfTrailingZeros(first);
    }

    public int chunkOf(int index) {
        int quotient = index >>> firstShift;
        return quotient == 0 ? 0 : 32 - Integer.numberOfLeadingZeros(quotient);
    }

    int chunkBase(int chunk) {
        return chunk == 0 ? 0 : first << (chunk - 1);
    }

    public int chunkSize(int chunk) {
        return chunk == 0 ? first : first << (chunk - 1);
    }

    public int offsetOf(int index, int chunk) {
        return index - chunkBase(chunk);
    }
}
