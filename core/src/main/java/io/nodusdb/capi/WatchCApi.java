package io.nodusdb.capi;

import io.nodusdb.kernel.Token;
import io.nodusdb.watch.ChangeBatch;
import io.nodusdb.watch.ChangeFeed;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;

import java.time.Duration;

public final class WatchCApi {

    private static final long NO_BOUND = -1;
    private static final int SIZE_FIELDS = 4;
    private static final long MAX_WAIT_MILLIS = Duration.ofDays(3_650).toMillis();

    private WatchCApi() {
    }

    @CEntryPoint(name = "nodus_watch_buffer")
    public static int watchBuffer(IsolateThread thread, VoidPointer handle, int changes) {
        try {
            return NodusCApi.session(handle).watchBuffer(changes);
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_changes")
    public static int changes(IsolateThread thread, VoidPointer handle, long afterEpoch, long afterLsn,
                              long throughLsn, int limit, long waitMillis, CCharPointer utf8, long utf8Cap,
                              CIntPointer lengths, int lengthsCap, CLongPointer numbers, int numbersCap,
                              CIntPointer kinds, int kindsCap, CLongPointer sizesOut, CLongPointer nextOut) {
        try {
            if (waitMillis < 0) {
                throw new IllegalArgumentException("the wait must not be negative: " + waitMillis);
            }
            Duration wait = Duration.ofMillis(Math.min(waitMillis, MAX_WAIT_MILLIS));
            long bound = throughLsn == NO_BOUND ? ChangeFeed.NO_BOUND : throughLsn;
            ChangeBatch batch = NodusCApi.session(handle).changes(new Token(afterEpoch, afterLsn), bound, limit, wait);
            ChangeWire.Encoded encoded = ChangeWire.encode(batch);
            writeSizes(sizesOut, encoded);
            if (nextOut.isNonNull()) {
                nextOut.write(0, batch.next().epoch());
                nextOut.write(1, batch.next().lsn());
            }
            if (fits(encoded, utf8Cap, lengthsCap, numbersCap, kindsCap)) {
                copy(encoded, utf8, lengths, numbers, kinds);
            }
            return encoded.changes();
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    private static boolean fits(ChangeWire.Encoded encoded, long utf8Cap, int lengthsCap, int numbersCap,
                                int kindsCap) {
        return encoded.utf8().length <= utf8Cap && encoded.lengths().length <= lengthsCap
                && encoded.numbers().length <= numbersCap && encoded.kinds().length <= kindsCap;
    }

    private static void writeSizes(CLongPointer sizesOut, ChangeWire.Encoded encoded) {
        if (sizesOut.isNull()) {
            return;
        }
        long[] sizes = {encoded.utf8().length, encoded.lengths().length, encoded.numbers().length,
            encoded.kinds().length};
        for (int i = 0; i < SIZE_FIELDS; i++) {
            sizesOut.write(i, sizes[i]);
        }
    }

    private static void copy(ChangeWire.Encoded encoded, CCharPointer utf8, CIntPointer lengths,
                             CLongPointer numbers, CIntPointer kinds) {
        byte[] bytes = encoded.utf8();
        for (int i = 0; i < bytes.length; i++) {
            utf8.write(i, bytes[i]);
        }
        int[] lengthValues = encoded.lengths();
        for (int i = 0; i < lengthValues.length; i++) {
            lengths.write(i, lengthValues[i]);
        }
        long[] numberValues = encoded.numbers();
        for (int i = 0; i < numberValues.length; i++) {
            numbers.write(i, numberValues[i]);
        }
        int[] kindValues = encoded.kinds();
        for (int i = 0; i < kindValues.length; i++) {
            kinds.write(i, kindValues[i]);
        }
    }
}
