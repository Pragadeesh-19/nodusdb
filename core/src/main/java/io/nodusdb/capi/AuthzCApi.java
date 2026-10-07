package io.nodusdb.capi;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.kernel.Token;
import io.nodusdb.storage.GraphUpgrade;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class AuthzCApi {

    private static final int CHECK_FIELDS = 3;
    private static final int NO_TOKEN = -1;
    private static final int LOCAL_DURABILITY = 0;
    private static final int LAKE_DURABILITY = 1;

    private AuthzCApi() {
    }

    @CEntryPoint(name = "nodus_token")
    public static int token(IsolateThread thread, VoidPointer handle, CLongPointer tokenOut) {
        try {
            writeToken(tokenOut, NodusCApi.session(handle).token());
            return NodusCApi.OK;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_schema_version")
    public static int schemaVersion(IsolateThread thread, VoidPointer handle) {
        try {
            return NodusCApi.session(handle).schemaVersion();
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_schema_apply")
    public static int schemaApply(IsolateThread thread, VoidPointer handle, CCharPointer utf8, int length,
                                  CLongPointer tokenOut) {
        try {
            String document = new String(NodusCApi.readBytes(utf8, length), StandardCharsets.UTF_8);
            writeToken(tokenOut, NodusCApi.session(handle).applySchema(document));
            return NodusCApi.OK;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_tuple_write")
    public static int tupleWrite(IsolateThread thread, VoidPointer handle, CCharPointer utf8, CIntPointer lengths,
                                 CIntPointer kinds, int operations, int durability, CLongPointer tokenOut) {
        try {
            int[] lengthValues = NodusCApi.intArray(lengths, TupleWire.FIELDS_PER_OPERATION * operations);
            int[] kindValues = NodusCApi.intArray(kinds, operations);
            long bytes = 0;
            for (int length : lengthValues) {
                bytes += Math.max(length, 0);
            }
            TupleTransaction transaction = TupleWire.decode(NodusCApi.readBytes(utf8, bytes), lengthValues,
                    kindValues, operations);
            Token token = NodusCApi.session(handle).writeTuples(transaction, durabilityOf(durability));
            writeToken(tokenOut, token);
            return NodusCApi.OK;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_check")
    public static int check(IsolateThread thread, VoidPointer handle, CCharPointer utf8, CIntPointer lengths,
                            long atEpoch, long atLsn) {
        try {
            int[] lengthValues = NodusCApi.intArray(lengths, CHECK_FIELDS);
            byte[] blob = NodusCApi.readBytes(utf8, NodusCApi.totalLength(lengthValues));
            String object = field(blob, lengthValues, 0);
            String permission = field(blob, lengthValues, 1);
            String subject = field(blob, lengthValues, 2);
            Token atLeast = atEpoch == NO_TOKEN ? null : new Token(atEpoch, atLsn);
            return NodusCApi.session(handle).check(object, permission, subject, atLeast) ? 1 : 0;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_upgrade")
    public static int upgrade(IsolateThread thread, CCharPointer directory, CLongPointer reportOut) {
        try {
            GraphUpgrade.Report report = GraphUpgrade.upgrade(Path.of(NodusCApi.cString(directory)));
            if (reportOut.isNonNull()) {
                reportOut.write(0, report.performed() ? 1 : 0);
                reportOut.write(1, report.edges());
                reportOut.write(2, report.symbols());
            }
            return NodusCApi.OK;
        } catch (IOException | RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_upgrade_cleanup")
    public static int upgradeCleanup(IsolateThread thread, CCharPointer directory) {
        try {
            GraphUpgrade.cleanup(Path.of(NodusCApi.cString(directory)));
            return NodusCApi.OK;
        } catch (IOException | RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    private static Durability durabilityOf(int code) {
        return switch (code) {
            case LOCAL_DURABILITY -> Durability.LOCAL;
            case LAKE_DURABILITY -> Durability.LAKE;
            default -> throw new IllegalArgumentException("unknown durability code: " + code);
        };
    }

    private static void writeToken(CLongPointer tokenOut, Token token) {
        if (tokenOut.isNonNull()) {
            tokenOut.write(0, token.epoch());
            tokenOut.write(1, token.lsn());
        }
    }

    private static String field(byte[] blob, int[] lengths, int index) {
        int offset = 0;
        for (int i = 0; i < index; i++) {
            offset += lengths[i];
        }
        return new String(blob, offset, lengths[index], StandardCharsets.UTF_8);
    }
}
