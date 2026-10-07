package io.nodusdb.capi;

import io.nodusdb.error.ErrorCode;
import io.nodusdb.error.NodusException;

import java.nio.charset.StandardCharsets;

public final class Failures {

    private static final ThreadLocal<String> LAST_MESSAGE = ThreadLocal.withInitial(() -> "");

    private Failures() {
    }

    public static int codeOf(Throwable failure) {
        String message = failure.getMessage();
        LAST_MESSAGE.set(message == null ? failure.getClass().getSimpleName() : message);
        if (failure instanceof NodusException nodus) {
            return nodus.code().value();
        }
        return ErrorCode.FAILURE.value();
    }

    public static byte[] lastMessage() {
        return LAST_MESSAGE.get().getBytes(StandardCharsets.UTF_8);
    }
}
