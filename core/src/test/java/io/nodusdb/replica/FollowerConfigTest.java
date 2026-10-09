package io.nodusdb.replica;

import io.nodusdb.config.ConfigFields;
import io.nodusdb.config.StoreConfig.CredentialKind;
import io.nodusdb.config.StoreConfig.StoreType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FollowerConfigTest {

    private static final String TRUST = "\"trust\":{\"key_id\":4,\"public_key_file\":\"/keys/writer.pub\"}";
    private static final String DIRECTORY = "\"store\":{\"type\":\"directory\",\"directory\":\"/data/bucket\"}";
    private static final String S3 = "\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"eu-west-1\"},"
            + "\"credentials\":{\"source\":\"static\",\"access_key_id\":\"AKIAEXAMPLE\","
            + "\"secret_access_key\":\"s3cr3tValue\"}";

    private static String config(String... blocks) {
        return "{" + String.join(",", blocks) + "}";
    }

    private static String rejection(String document) {
        return assertThrows(IllegalArgumentException.class, () -> FollowerConfig.parse(document)).getMessage();
    }

    @Test
    void aDirectoryFollowerWithOnlyTheRequiredFieldsGetsTheDefaults() {
        FollowerConfig parsed = FollowerConfig.parse(config(DIRECTORY, TRUST));

        assertEquals(StoreType.DIRECTORY, parsed.storage().store().type());
        assertEquals(Path.of("/data/bucket"), parsed.storage().store().directory());
        assertNull(parsed.storage().credentials());
        assertEquals(4, parsed.trust().keyId());
        assertEquals(Path.of("/keys/writer.pub"), parsed.trust().publicKeyFile());
        assertEquals(FollowerConfig.DEFAULT_POLL_INTERVAL, parsed.follow().pollInterval());
        assertEquals(FollowerConfig.DEFAULT_READ_WAIT, parsed.follow().readWait());
        assertFalse(parsed.follow().hasStalenessBound());
        assertEquals(FollowerConfig.DEFAULT_DOWNLOAD_PARALLELISM, parsed.follow().downloadParallelism());
        assertFalse(parsed.follow().persistsRollbackMemory());
        assertEquals(ConfigFields.DEFAULT_REQUEST_TIMEOUT, parsed.storage().requestTimeout());
    }

    @Test
    void everyFollowSettingIsRead() {
        FollowerConfig parsed = FollowerConfig.parse(config(S3, TRUST, "\"follow\":{\"poll_interval_ms\":250,"
                + "\"read_wait_ms\":2000,\"max_staleness_ms\":30000,\"download_parallelism\":8,"
                + "\"request_timeout_ms\":2500,\"state_directory\":\"/var/lib/nodus/follower\"}"));

        assertEquals(StoreType.S3, parsed.storage().store().type());
        assertEquals(CredentialKind.STATIC, parsed.storage().credentials().kind());
        assertEquals(Duration.ofMillis(250), parsed.follow().pollInterval());
        assertEquals(Duration.ofSeconds(2), parsed.follow().readWait());
        assertEquals(Duration.ofSeconds(30), parsed.follow().maxStaleness());
        assertTrue(parsed.follow().hasStalenessBound());
        assertEquals(8, parsed.follow().downloadParallelism());
        assertEquals(Duration.ofMillis(2500), parsed.storage().requestTimeout());
        assertEquals(Path.of("/var/lib/nodus/follower"), parsed.follow().stateDirectory());
        assertTrue(parsed.follow().persistsRollbackMemory());
    }

    @Test
    void thePollIntervalAcceptsItsBoundaries() {
        assertEquals(Duration.ofMillis(10), FollowerConfig.parse(config(DIRECTORY, TRUST,
                "\"follow\":{\"poll_interval_ms\":10}")).follow().pollInterval());
        assertEquals(Duration.ofMillis(3_600_000), FollowerConfig.parse(config(DIRECTORY, TRUST,
                "\"follow\":{\"poll_interval_ms\":3600000}")).follow().pollInterval());
    }

    @Test
    void aWriterSigningBlockIsRefusedAndItsPathIsNotEchoed() {
        String message = rejection(config(DIRECTORY, TRUST, "\"signing\":{\"key_file\":\"/secret/writer.key\","
                + "\"key_id\":4}"));

        assertTrue(message.startsWith("invalid follower configuration: "), message);
        assertTrue(message.contains("signing"), message);
        assertFalse(message.contains("/secret/writer.key"), message);
    }

    @Test
    void aPrivateKeyFileInsideTheTrustBlockIsRefused() {
        String message = rejection(config(DIRECTORY, "\"trust\":{\"key_id\":4,\"public_key_file\":\"/k.pub\","
                + "\"key_file\":\"/secret/writer.key\"}"));

        assertTrue(message.contains("key_file"), message);
        assertFalse(message.contains("/secret/writer.key"), message);
    }

    @Test
    void secretsNeverAppearInAnErrorMessage() {
        String message = rejection(config(S3, TRUST, "\"follow\":{\"bogus\":1}"));

        assertTrue(message.contains("bogus"), message);
        assertFalse(message.contains("s3cr3tValue"), message);
        assertFalse(message.contains("AKIAEXAMPLE"), message);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"}}|trust",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1}}|public_key_file",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"public_key_file\":\"/k\"}}|key_id",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":-1,\"public_key_file\":\"/k\"}}|key_id",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":4294967296,\"public_key_file\":\"/k\"}}|key_id",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"\"}}|public_key_file",
            "{\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"}}|store",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"}}|credentials",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"follow\":{\"poll_interval_ms\":9}}|poll_interval_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"follow\":{\"poll_interval_ms\":3600001}}|poll_interval_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"follow\":{\"read_wait_ms\":0}}|read_wait_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"follow\":{\"max_staleness_ms\":0}}|max_staleness_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"follow\":{\"download_parallelism\":0}}|download_parallelism",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"follow\":{\"download_parallelism\":17}}|download_parallelism",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"follow\":{\"request_timeout_ms\":-5}}|request_timeout_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"follow\":{\"state_directory\":\"\"}}|state_directory",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"follow\":{\"poll_interval_ms\":\"fast\"}}|poll_interval_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"ship\":{\"interval_ms\":50}}|ship",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"trust\":{\"key_id\":1,\"public_key_file\":\"/k\"},\"iceberg\":{\"enabled\":true}}|iceberg",
            "not json at all|",
            "[]|"})
    void aBadDocumentIsRejectedWithAMessageThatNamesTheField(String document, String field) {
        String message = rejection(document);

        assertTrue(message.startsWith("invalid follower configuration: "), message);
        if (field != null && !field.isEmpty()) {
            assertTrue(message.contains(field), "'" + message + "' should name '" + field + "'");
        }
    }
}
