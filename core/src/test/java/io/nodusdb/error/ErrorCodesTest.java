package io.nodusdb.error;

import io.nodusdb.capi.Failures;
import io.nodusdb.chain.ChainFormatException;
import io.nodusdb.chain.ChainTrustException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ErrorCodesTest {

    private static final int FIRST_CODE = -1;
    private static final int LAST_CODE = -15;

    @Test
    void everyCodeIsUniqueAndInsideTheContractRange() {
        Set<Integer> values = Arrays.stream(ErrorCode.values()).map(ErrorCode::value).collect(Collectors.toSet());

        assertEquals(ErrorCode.values().length, values.size());
        for (int value : values) {
            assertTrue(value <= FIRST_CODE && value >= LAST_CODE, "code outside the contract: " + value);
        }
    }

    @Test
    void theCodesFormAContiguousRangeFromMinusOneToMinusFifteen() {
        Set<Integer> values = Arrays.stream(ErrorCode.values()).map(ErrorCode::value).collect(Collectors.toSet());

        for (int value = FIRST_CODE; value >= LAST_CODE; value--) {
            assertTrue(values.contains(value), "the contract has no code " + value);
        }
    }

    @Test
    void everyExceptionReachesTheNativeCallerWithItsOwnCode() {
        Map<NodusException, ErrorCode> expected = new LinkedHashMap<>();
        expected.put(new ChainTrustException("forged"), ErrorCode.CHAIN_TRUST);
        expected.put(new ChainFormatException("damaged"), ErrorCode.CHAIN_TRUST);
        expected.put(new CheckDepthException("depth"), ErrorCode.CHECK_DEPTH);
        expected.put(new CorruptLogException("corrupt"), ErrorCode.CORRUPT_LOG);
        expected.put(new IndeterminateOutcomeException("indeterminate"), ErrorCode.INDETERMINATE);
        expected.put(new LogBacklogException("backlog"), ErrorCode.LOG_BACKLOG);
        expected.put(new SchemaViolationException("schema"), ErrorCode.SCHEMA_VIOLATION);
        expected.put(new ShipTimeoutException("timeout", 3, 40), ErrorCode.SHIP_TIMEOUT);
        expected.put(new StaleReadException("stale"), ErrorCode.STALE_READ);
        expected.put(new TokenLostException("lost"), ErrorCode.TOKEN_LOST);
        expected.put(new UnsupportedFeatureException("unsupported"), ErrorCode.UNSUPPORTED);
        expected.put(new UpgradeFailedException("upgrade failed"), ErrorCode.FAILURE);
        expected.put(new UpgradeRequiredException("upgrade required"), ErrorCode.UPGRADE_REQUIRED);
        expected.put(new WriterFencedException("fenced"), ErrorCode.WRITER_FENCED);

        expected.forEach((failure, code) -> {
            assertEquals(code, failure.code(), failure.getClass().getSimpleName());
            assertEquals(code.value(), Failures.codeOf(failure), failure.getClass().getSimpleName());
            assertEquals(failure.getMessage(), new String(Failures.lastMessage(), StandardCharsets.UTF_8));
        });
    }

    @Test
    void aShipTimeoutKeepsTheTokenItWasWaitingFor() {
        ShipTimeoutException failure = new ShipTimeoutException("not shipped", 7, 12_345);

        assertEquals(7, failure.epoch());
        assertEquals(12_345, failure.lsn());
    }

    @Test
    void aFencedWriterKeepsItsCause() {
        IllegalStateException cause = new IllegalStateException("higher epoch");

        assertEquals(cause, new WriterFencedException("fenced", cause).getCause());
    }
}
