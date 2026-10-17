package io.nodusdb.capi;

import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.past.PastReport;
import io.nodusdb.kernel.past.PastTarget;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.word.WordFactory;

import java.time.Duration;

public final class HistoryCApi {

    private static final int AT_TOKEN = 0;
    private static final int AT_TIME = 1;
    private static final long MAX_WAIT_MILLIS = Duration.ofDays(3_650).toMillis();

    private HistoryCApi() {
    }

    @CEntryPoint(name = "nodus_open_as_of")
    public static VoidPointer openAsOf(IsolateThread thread, VoidPointer handle, int kind, long epoch,
                                       long lsnOrMicros, long maxMemoryBytes, long waitMillis,
                                       CLongPointer positionOut, CIntPointer status) {
        try {
            long view = NodusCApi.SESSIONS.openPast(handle.rawValue(), target(kind, epoch, lsnOrMicros),
                    maxMemoryBytes, waitFor(waitMillis));
            writePosition(positionOut, NodusCApi.SESSIONS.get(view).pastReport());
            NodusCApi.report(status, NodusCApi.OK);
            return WordFactory.pointer(view);
        } catch (RuntimeException e) {
            NodusCApi.report(status, Failures.codeOf(e));
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "nodus_past_advance")
    public static int advance(IsolateThread thread, VoidPointer handle, int kind, long epoch, long lsnOrMicros,
                              long waitMillis, CLongPointer positionOut) {
        try {
            PastReport report = NodusCApi.session(handle).advancePast(target(kind, epoch, lsnOrMicros),
                    waitFor(waitMillis));
            writePosition(positionOut, report);
            return NodusCApi.OK;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_past_position")
    public static int position(IsolateThread thread, VoidPointer handle, CLongPointer positionOut) {
        try {
            writePosition(positionOut, NodusCApi.session(handle).pastReport());
            return NodusCApi.OK;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    private static PastTarget target(int kind, long epoch, long lsnOrMicros) {
        return switch (kind) {
            case AT_TOKEN -> new PastTarget.AtToken(new Token(epoch, lsnOrMicros));
            case AT_TIME -> new PastTarget.AtTime(lsnOrMicros);
            default -> throw new IllegalArgumentException("a past target is a token (0) or a time (1): " + kind);
        };
    }

    private static Duration waitFor(long waitMillis) {
        if (waitMillis < 0) {
            throw new IllegalArgumentException("the wait must not be negative: " + waitMillis);
        }
        return Duration.ofMillis(Math.min(waitMillis, MAX_WAIT_MILLIS));
    }

    private static void writePosition(CLongPointer positionOut, PastReport report) {
        if (positionOut.isNonNull()) {
            positionOut.write(0, report.position().epoch());
            positionOut.write(1, report.position().lsn());
            positionOut.write(2, report.commitMicros());
        }
    }
}
