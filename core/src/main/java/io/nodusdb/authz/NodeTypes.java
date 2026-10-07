package io.nodusdb.authz;

import io.nodusdb.authz.schema.CompiledSchema;
import io.nodusdb.kernel.symbols.SymbolTable;

import java.util.Arrays;

final class NodeTypes {

    private static final int NOT_COMPUTED = -2;
    private static final int NO_PREFIX = -1;
    private static final byte SEPARATOR = ':';

    private final SymbolTable symbols;
    private volatile int[] prefixSymbols = new int[0];

    NodeTypes(SymbolTable symbols) {
        this.symbols = symbols;
    }

    int typeOf(int node, CompiledSchema model) {
        int[] cache = prefixSymbols;
        if (node < cache.length && cache[node] != NOT_COMPUTED) {
            return model.typeIndexOfSymbol(cache[node]);
        }
        int symbol = prefixOf(node);
        if (symbol != NO_PREFIX) {
            remember(node, symbol);
        }
        return model.typeIndexOfSymbol(symbol);
    }

    private int prefixOf(int node) {
        if (node < 0 || node >= symbols.size()) {
            return NO_PREFIX;
        }
        byte[] name = symbols.resolve(node);
        for (int i = 0; i < name.length; i++) {
            if (name[i] == SEPARATOR) {
                return symbols.lookup(name, 0, i);
            }
        }
        return NO_PREFIX;
    }

    private synchronized void remember(int node, int symbol) {
        int[] cache = prefixSymbols;
        if (node >= cache.length) {
            int grown = Math.max(node + 1, cache.length * 2);
            int[] larger = Arrays.copyOf(cache, grown);
            Arrays.fill(larger, cache.length, grown, NOT_COMPUTED);
            prefixSymbols = larger;
            cache = larger;
        }
        cache[node] = symbol;
    }
}
