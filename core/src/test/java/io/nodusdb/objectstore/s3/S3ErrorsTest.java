package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.ExpiredCredentialsException;
import io.nodusdb.objectstore.FatalStoreException;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.objectstore.TransientStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3ErrorsTest {

    private static S3Response response(int status, String code, String message) {
        String body = code == null ? "" : "<?xml version=\"1.0\"?><Error><Code>" + code + "</Code><Message>"
                + XmlText.escape(message) + "</Message></Error>";
        return new S3Response(status, Map.of(), body.getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @CsvSource({
            "408, RequestTimeout, transient", "409, OperationAborted, transient", "429, SlowDown, transient",
            "500, InternalError, transient", "502, BadGateway, transient", "503, ServiceUnavailable, transient",
            "504, GatewayTimeout, transient", "507, InsufficientStorage, transient", "599, Unknown, transient",
            "501, NotImplemented, fatal", "400, InvalidArgument, fatal", "401, Unauthorized, fatal",
            "403, AccessDenied, fatal", "403, SignatureDoesNotMatch, fatal", "403, RequestTimeTooSkewed, fatal",
            "404, NoSuchBucket, fatal", "404, NoSuchKey, fatal", "405, MethodNotAllowed, fatal",
            "412, PreconditionFailed, fatal", "416, InvalidRange, fatal", "301, PermanentRedirect, fatal",
            "400, ExpiredToken, expired", "403, ExpiredToken, expired", "400, InvalidToken, expired",
            "400, TokenRefreshRequired, expired"
    })
    void everyStatusAndCodeMapsToOneKindOfFailure(int status, String code, String kind) {
        ObjectStoreException failure = S3Errors.failure("do something", response(status, code, "a message"));

        Class<? extends ObjectStoreException> expected = switch (kind) {
            case "transient" -> TransientStoreException.class;
            case "fatal" -> FatalStoreException.class;
            default -> ExpiredCredentialsException.class;
        };
        assertInstanceOf(expected, failure);
        assertEquals(status, failure.status());
    }

    @ParameterizedTest
    @CsvSource({
            "400, ExpiredToken, true", "403, ExpiredToken, true", "403, InvalidToken, true",
            "403, InvalidAccessKeyId, true", "403, SignatureDoesNotMatch, true", "400, TokenRefreshRequired, true",
            "403, AccessDenied, false", "403, RequestTimeTooSkewed, false", "500, ExpiredToken, false",
            "404, ExpiredToken, false", "409, ExpiredToken, false"
    })
    void onlyCredentialProblemsOnTheRightStatusTriggerARefresh(int status, String code, boolean refreshable) {
        assertEquals(refreshable, S3Errors.refreshable(response(status, code, "m")));
    }

    @Test
    void aBodylessForbiddenResponseIsWorthOneRefreshButABodylessBadRequestIsNot() {
        assertTrue(S3Errors.refreshable(response(403, null, null)));
        assertFalse(S3Errors.refreshable(response(400, null, null)));
        assertFalse(S3Errors.refreshable(response(500, null, null)));
    }

    @Test
    void theMessageNamesTheOperationStatusCodeAndServerText() {
        ObjectStoreException failure = S3Errors.failure("create chain/1.obj",
                response(503, "SlowDown", "Please reduce your request rate."));

        assertEquals("create chain/1.obj failed with HTTP 503 SlowDown: Please reduce your request rate.",
                failure.getMessage());
    }

    @Test
    void aBodylessFailureStillNamesTheStatus() {
        assertEquals("inspect a/b failed with HTTP 403", S3Errors.failure("inspect a/b", response(403, null, null))
                .getMessage());
    }

    @Test
    void aLongServerMessageIsCutAndEntitiesAreDecoded() {
        String message = "x".repeat(1_000);

        ObjectStoreException failure = S3Errors.failure("op", response(500, "InternalError", message));

        assertTrue(failure.getMessage().length() < 400, "message length " + failure.getMessage().length());
        assertEquals("a < b & c", XmlText.text(response(400, "Bad", "a < b & c").text(), "Message").orElseThrow());
    }

    @Test
    void anUnreadableBodyDoesNotBreakTheMapping() {
        S3Response garbage = new S3Response(500, Map.of(), "<<<not xml".getBytes(StandardCharsets.UTF_8));

        assertEquals("", S3Errors.code(garbage));
        assertInstanceOf(TransientStoreException.class, S3Errors.failure("op", garbage));
    }
}
