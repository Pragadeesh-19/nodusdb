package io.nodusdb.kernel.memory;

import java.lang.foreign.Arena;
import java.util.Objects;

public final class NativeBlockPool {

    public static final int LINE_WORDS = 8;
    static final int MIN_LOG_WORDS = Integer.numberOfTrailingZeros(LINE_WORDS);
    static final int MAX_LOG_WORDS = 30;

    public static final int CLASS_COUNT = MAX_LOG_WORDS + 1;

    private static final int INITIAL_WORDS = 1 << 10;

    private final NativeLongArray words;
    private final int[] freeHeads = new int[CLASS_COUNT];
    private final int[] freeCounts = new int[CLASS_COUNT];
    private int top;

    public NativeBlockPool(Arena arena, MemoryBudget budget) {
        this.words = new NativeLongArray(Objects.requireNonNull(arena, "arena"), budget, INITIAL_WORDS, 0L);
    }

    public int allocate(int logWords) {
        checkLogWords(logWords);
        int head = freeHeads[logWords];
        if (head != 0) {
            freeHeads[logWords] = (int) words.get(head);
            freeCounts[logWords]--;
            words.set(head - 1, logWords);
            clear(head, logWords);
            return head;
        }
        int handle = nextHandle();
        int end = endOfBlock(handle, logWords);
        words.ensureCapacity(end);
        words.set(handle - 1, logWords);
        top = end;
        return handle;
    }

    public long bytesToReserveBlocks(int[] blocksByLogWords) {
        long extraWords = freshWordsFor(blocksByLogWords);
        return extraWords == 0 ? 0 : words.bytesToReach(wordsNeeded(extraWords));
    }

    public void reserveBlocks(int[] blocksByLogWords) {
        long extraWords = freshWordsFor(blocksByLogWords);
        if (extraWords > 0) {
            words.ensureCapacity(wordsNeeded(extraWords));
        }
    }

    public void release(int handle) {
        int log = handle < top ? logWordsOf(handle) : -1;
        if (log < 0) {
            throw new IllegalStateException("block is not allocated: " + handle);
        }
        words.set(handle - 1, ~log);
        words.set(handle, freeHeads[log]);
        freeHeads[log] = handle;
        freeCounts[log]++;
    }

    public int logWordsOf(int handle) {
        if (handle < LINE_WORDS || (handle & (LINE_WORDS - 1)) != 0) {
            return -1;
        }
        long header = words.get(handle - 1);
        return header >= MIN_LOG_WORDS && header <= MAX_LOG_WORDS ? (int) header : -1;
    }

    public long get(int handle, int offset) {
        return words.get(handle + offset);
    }

    public void set(int handle, int offset, long value) {
        words.set(handle + offset, value);
    }

    private long freshWordsFor(int[] blocksByLogWords) {
        long extraWords = 0;
        for (int log = MIN_LOG_WORDS; log < blocksByLogWords.length && log <= MAX_LOG_WORDS; log++) {
            int fresh = blocksByLogWords[log] - freeCounts[log];
            if (fresh > 0) {
                extraWords += (long) fresh * ((1L << log) + LINE_WORDS);
            }
        }
        return extraWords;
    }

    private int wordsNeeded(long extraWords) {
        long needed = (long) top + extraWords + LINE_WORDS;
        if (needed > NativeLongArray.MAX_CAPACITY) {
            throw new IllegalStateException("native block pool is full");
        }
        return (int) needed;
    }

    private int nextHandle() {
        return (top + LINE_WORDS) & -LINE_WORDS;
    }

    private static int endOfBlock(int handle, int logWords) {
        long end = (long) handle + (1L << logWords);
        if (end > NativeLongArray.MAX_CAPACITY) {
            throw new IllegalStateException("native block pool is full");
        }
        return (int) end;
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
