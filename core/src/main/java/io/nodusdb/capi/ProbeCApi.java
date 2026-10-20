package io.nodusdb.capi;

import io.nodusdb.kernel.memory.NativeMemorySelfTest;

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

    @CEntryPoint(name = "nodus_lake_probe")
    public static int lakeProbe(IsolateThread thread, int rows, CLongPointer nanosOut) {
        try {
            LakeProbe.Timing timing = LakeProbe.measure(rows);
            nanosOut.write(0, timing.ingestNanos());
            nanosOut.write(1, timing.sumNanos());
            nanosOut.write(2, timing.flushNanos());
            return NodusCApi.OK;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_selftest_memory")
    public static int selfTestMemory(IsolateThread thread) {
        try {
            NativeMemorySelfTest.Result result = NativeMemorySelfTest.run(true);
            Failures.remember(result.messages());
            return result.failures();
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }
}
