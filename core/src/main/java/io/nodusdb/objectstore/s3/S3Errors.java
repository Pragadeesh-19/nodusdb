package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.ExpiredCredentialsException;
import io.nodusdb.objectstore.FatalStoreException;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.objectstore.TransientStoreException;

import java.util.Set;

final class S3Errors {

    private static final Set<String> EXPIRED = Set.of("ExpiredToken", "InvalidToken", "TokenRefreshRequired");
    private static final Set<String> REFRESHABLE = Set.of("ExpiredToken", "InvalidToken", "TokenRefreshRequired",
            "InvalidAccessKeyId", "SignatureDoesNotMatch");
    private static final int MAX_MESSAGE_CHARS = 300;
    private static final int NOT_IMPLEMENTED = 501;
    private static final int REQUEST_TIMEOUT = 408;
    private static final int CONFLICT = 409;
    private static final int TOO_MANY_REQUESTS = 429;
    private static final int SERVER_ERROR = 500;

    private S3Errors() {
    }

    static String code(S3Response response) {
        return response.body().length == 0 ? "" : XmlText.text(response.text(), "Code").orElse("");
    }

    static boolean refreshable(S3Response response) {
        if (response.status() != 400 && response.status() != 403) {
            return false;
        }
        String code = code(response);
        return REFRESHABLE.contains(code) || (code.isEmpty() && response.status() == 403);
    }

    static ObjectStoreException failure(String operation, S3Response response) {
        int status = response.status();
        String code = code(response);
        String message = response.body().length == 0 ? ""
                : XmlText.text(response.text(), "Message").orElse("");
        StringBuilder description = new StringBuilder(operation).append(" failed with HTTP ").append(status);
        if (!code.isEmpty()) {
            description.append(' ').append(code);
        }
        if (!message.isEmpty()) {
            description.append(": ").append(message, 0, Math.min(message.length(), MAX_MESSAGE_CHARS));
        }
        if (EXPIRED.contains(code)) {
            return new ExpiredCredentialsException(description.toString(), status);
        }
        if (isTransient(status)) {
            return new TransientStoreException(description.toString(), status);
        }
        return new FatalStoreException(description.toString(), status);
    }

    static boolean isTransient(int status) {
        return status == REQUEST_TIMEOUT || status == CONFLICT || status == TOO_MANY_REQUESTS
                || (status >= SERVER_ERROR && status != NOT_IMPLEMENTED);
    }
}
