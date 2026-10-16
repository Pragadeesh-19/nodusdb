package io.nodusdb.projection;

import io.nodusdb.kernel.ErasedSymbols;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.symbols.Pseudonym;
import io.nodusdb.kernel.symbols.SymbolTable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public final class KernelErasedNames implements ErasedNames {

    private final GraphKernel kernel;

    public KernelErasedNames(GraphKernel kernel) {
        this.kernel = kernel;
    }

    @Override
    public Lookup current() {
        byte[] salt = kernel.erasureSalt();
        SymbolTable symbols = kernel.symbols();
        ErasedSymbols erased = kernel.erasedSymbolLog();
        Map<String, Long> erasedAt = new HashMap<>();
        for (int i = 0; i < erased.count(); i++) {
            String pseudonym = new String(symbols.resolve(erased.symbolId(i)), StandardCharsets.US_ASCII);
            erasedAt.merge(pseudonym, erased.eraseLsn(i), Math::max);
        }
        return name -> {
            String pseudonym = new String(Pseudonym.of(salt, name.getBytes(StandardCharsets.UTF_8)),
                    StandardCharsets.US_ASCII);
            Long eraseLsn = erasedAt.get(pseudonym);
            return eraseLsn == null ? null : new Erased(pseudonym, eraseLsn);
        };
    }
}
