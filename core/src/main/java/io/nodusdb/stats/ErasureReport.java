package io.nodusdb.stats;

import io.nodusdb.kernel.ErasureProgress;
import io.nodusdb.kernel.GraphKernel;

import java.util.List;

public record ErasureReport(int erasedSymbols, long newestEraseLsn, List<ErasureProgress.Layer> layers,
                            String lastError) {

    public static final ErasureReport NONE = new ErasureReport(0, 0, List.of(), "");

    public ErasureReport {
        layers = List.copyOf(layers);
    }

    public static ErasureReport of(GraphKernel kernel) {
        ErasureProgress progress = kernel.erasureProgress();
        return new ErasureReport(kernel.erasedSymbols(), kernel.newestEraseLsn(), progress.layers(),
                progress.lastError());
    }
}
