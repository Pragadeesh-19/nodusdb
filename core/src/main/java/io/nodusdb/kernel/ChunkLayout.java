package io.nodusdb.kernel;

/*
 * Index arithmetic for grow-only storage split into chunks of doubling size.
 * With first = the size of chunk 0 (a power of two):
 *
 *   chunk 0   indices [0, first)              size first
 *   chunk c   indices [first*2^(c-1), first*2^c)   size first*2^(c-1), c >= 1
 */
final class ChunkLayout {

    private final int first;
    private final int firstShift;

    ChunkLayout(int first) {
        if (first < 1 || Integer.bitCount(first) != 1) {
            throw new IllegalArgumentException("first chunk size must be a positive power of two: " + first);
        }
        this.first = first;
        this.firstShift = Integer.numberOfTrailingZeros(first);
    }

    int chunkOf(int index) {
        int quotient = index >>> firstShift;
        return quotient == 0 ? 0 : 32 - Integer.numberOfLeadingZeros(quotient);
    }

    int chunkBase(int chunk) {
        return chunk == 0 ? 0 : first << (chunk - 1);
    }

    int chunkSize(int chunk) {
        return chunk == 0 ? first : first << (chunk - 1);
    }

    int offsetOf(int index, int chunk) {
        return index - chunkBase(chunk);
    }
}
