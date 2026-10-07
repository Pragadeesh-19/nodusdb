package io.nodusdb.capi;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CLongPointer;

public final class ProbeCApi {

    private ProbeCApi() {
    }

    @CEntryPoint(name = "nodus_probe")
    public static int probe(IsolateThread thread, int operations, CLongPointer nanosOut) {
        try {
            PerformanceProbe.Timing timing = PerformanceProbe.measure(operations);
            nanosOut.write(0, timing.addNanos());
            nanosOut.write(1, timing.hasNanos());
            return NodusCApi.OK;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }
}
