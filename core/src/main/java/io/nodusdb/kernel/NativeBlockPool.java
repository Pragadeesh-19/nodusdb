package io.nodusdb.kernel;

import java.lang.foreign.Arena;
import java.util.Objects;

/*
 * Power-of-two blocks of longs in native memory, with one free list per size class.
 *
 *   word:     ... [ header ] [ payload 0 .. 2^log-1 ] [ header ] [ payload ... ]
 *                  ^ word LINE_WORDS-1 of a line; payload starts on a line boundary
 *   handle:   word index of payload 0, always a multiple of LINE_WORDS
 *
 * The arena aligns every chunk to 64 bytes and chunk starts are multiples of LINE_WORDS,
 * so every payload begins on a cache line. A header holds log while the block is
 * allocated and ~log while it is free. Bump allocation skips the padding words before
 * the next aligned header; those words are never written and stay zero. A freed
 * block's first payload word holds the next free handle, and zero ends the list.
 * Fresh blocks are zero because the arena zeroes memory; a reused block is cleared
 * when it is handed out.
 * Blocks never move, and a released block returns only to its own size class, so
 * classes cannot fragment one another. Mutation is single-writer; readers use
 * logWordsOf, which answers -1 for any handle that is not a live block.
 */
final class NativeBlockPool {

    static final int LINE_WORDS = 8;
    static final int MIN_LOG_WORDS = Integer.numberOfTrailingZeros(LINE_WORDS);
    static final int MAX_LOG_WORDS = 30;

    private static final int CLASS_COUNT = MAX_LOG_WORDS + 1;
    private static final int INITIAL_WORDS = 1 << 10;

    private final NativeLongArray words;
    private final int[] freeHeads = new int[CLASS_COUNT];
    private int top;

    NativeBlockPool(Arena arena) {
        this.words = new NativeLongArray(Objects.requireNonNull(arena, "arena"), INITIAL_WORDS, 0L);
    }

    int allocate(int logWords) {
        checkLogWords(logWords);
        int head = freeHeads[logWords];
        if (head != 0) {
            freeHeads[logWords] = (int) words.get(head);
            words.set(head - 1, logWords);
            clear(head, logWords);
            return head;
        }
        int handle = (top + LINE_WORDS) & -LINE_WORDS;
        long end = (long) handle + (1L << logWords);
        if (end > NativeLongArray.MAX_CAPACITY) {
            throw new IllegalStateException("native block pool is full");
        }
        words.ensureCapacity((int) end);
        words.set(handle - 1, logWords);
        top = (int) end;
        return handle;
    }

    void release(int handle) {
        int log = handle < top ? logWordsOf(handle) : -1;
        if (log < 0) {
            throw new IllegalStateException("block is not allocated: " + handle);
        }
        words.set(handle - 1, ~log);
        words.set(handle, freeHeads[log]);
        freeHeads[log] = handle;
    }

    int logWordsOf(int handle) {
        if (handle < LINE_WORDS || (handle & (LINE_WORDS - 1)) != 0) {
            return -1;
        }
        long header = words.get(handle - 1);
        return header >= MIN_LOG_WORDS && header <= MAX_LOG_WORDS ? (int) header : -1;
    }

    long get(int handle, int offset) {
        return words.get(handle + offset);
    }

    void set(int handle, int offset, long value) {
        words.set(handle + offset, value);
    }

    private void clear(int handle, int logWords) {
        for (int offset = 0; offset < 1 << logWords; offset++) {
            words.set(handle + offset, 0L);
        }
    }

    private static void checkLogWords(int logWords) {
        if (logWords < MIN_LOG_WORDS || logWords > MAX_LOG_WORDS) {
            throw new IllegalArgumentException("block size must be 2^" + MIN_LOG_WORDS
                    + " to 2^" + MAX_LOG_WORDS + " words: " + logWords);
        }
    }
}
