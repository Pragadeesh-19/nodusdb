package io.nodusdb.kernel.closure;

import java.util.Arrays;

final class DirtyNodes {

    static final int NONE = -1;

    private static final int INITIAL_QUEUE = 16;

    private volatile long[] bits = new long[0];
    private volatile boolean sweeping;
    private volatile int sweepLine;
    private int[] queue = new int[INITIAL_QUEUE];
    private int head;
    private int tail;
    private int marked;

    boolean isDirty(int node) {
        if (sweeping && node >= sweepLine) {
            return true;
        }
        return isMarked(node);
    }

    boolean isMarked(int node) {
        long[] words = bits;
        int word = node >>> 6;
        return node >= 0 && word < words.length && (words[word] & (1L << node)) != 0;
    }

    void mark(int node) {
        if (setBit(node)) {
            enqueue(node);
        }
    }

    void markWithoutQueueing(int node) {
        setBit(node);
    }

    void enqueue(int node) {
        if (tail == queue.length) {
            if (head > 0) {
                System.arraycopy(queue, head, queue, 0, tail - head);
                tail -= head;
                head = 0;
            } else {
                queue = Arrays.copyOf(queue, queue.length * 2);
            }
        }
        queue[tail++] = node;
    }

    int pollMarked(WorkBudget budget) {
        while (head < tail) {
            budget.spend(1);
            int node = queue[head++];
            if (isMarked(node)) {
                return node;
            }
        }
        head = 0;
        tail = 0;
        return NONE;
    }

    boolean hasQueued() {
        return head < tail;
    }

    void clear(int node) {
        long[] words = bits;
        int word = node >>> 6;
        long bit = 1L << node;
        if (word < words.length && (words[word] & bit) != 0) {
            words[word] &= ~bit;
            marked--;
        }
    }

    int markedCount() {
        return marked;
    }

    boolean sweeping() {
        return sweeping;
    }

    int sweepLine() {
        return sweepLine;
    }

    void startSweep() {
        sweepLine = 0;
        sweeping = true;
    }

    void advanceSweep() {
        sweepLine = sweepLine + 1;
    }

    void finishSweep() {
        sweeping = false;
    }

    private boolean setBit(int node) {
        int word = node >>> 6;
        long[] words = bits;
        if (word >= words.length) {
            words = Arrays.copyOf(words, Math.max(word + 1, words.length * 2));
            bits = words;
        }
        long bit = 1L << node;
        if ((words[word] & bit) != 0) {
            return false;
        }
        words[word] |= bit;
        marked++;
        return true;
    }
}
