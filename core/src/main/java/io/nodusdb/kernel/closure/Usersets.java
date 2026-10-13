package io.nodusdb.kernel.closure;

import java.util.Arrays;

final class Usersets {

    private static final int INITIAL_CAPACITY = 16;

    private int[] nodes = new int[INITIAL_CAPACITY];
    private int[] relations = new int[INITIAL_CAPACITY];
    private int count;

    void clear() {
        count = 0;
    }

    void add(int node, int relation) {
        if (count == nodes.length) {
            nodes = Arrays.copyOf(nodes, count * 2);
            relations = Arrays.copyOf(relations, count * 2);
        }
        nodes[count] = node;
        relations[count++] = relation;
    }

    int count() {
        return count;
    }

    int node(int index) {
        return nodes[index];
    }

    int relation(int index) {
        return relations[index];
    }
}
