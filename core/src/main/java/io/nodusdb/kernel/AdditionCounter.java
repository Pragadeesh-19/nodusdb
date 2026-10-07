package io.nodusdb.kernel;

import java.util.Arrays;

final class AdditionCounter {

    private static final int INITIAL_CAPACITY = 16;

    private int[] nodes = new int[INITIAL_CAPACITY];
    private int size;

    void clear() {
        size = 0;
    }

    void add(int node) {
        if (size == nodes.length) {
            nodes = Arrays.copyOf(nodes, size * 2);
        }
        nodes[size++] = node;
    }

    int size() {
        return size;
    }

    void sort() {
        if (size > 1) {
            Arrays.sort(nodes, 0, size);
        }
    }

    int nodeAt(int index) {
        return nodes[index];
    }

    int runLengthAt(int index) {
        int end = index + 1;
        while (end < size && nodes[end] == nodes[index]) {
            end++;
        }
        return end - index;
    }
}
