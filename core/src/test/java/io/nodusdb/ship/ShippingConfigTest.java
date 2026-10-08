package io.nodusdb.ship;

import io.nodusdb.ship.ShippingConfig.CredentialKind;
import io.nodusdb.ship.ShippingConfig.StoreType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShippingConfigTest {

    private static final String SIGNING = "\"signing\":{\"key_file\":\"/keys/writer.key\",\"key_id\":4}";
    private static final String DIRECTORY = "\"store\":{\"type\":\"directory\",\"directory\":\"/data/bucket\"}";
    private static final String S3_STATIC = "\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"eu-west-1\"},"
            + "\"credentials\":{\"source\":\"static\",\"access_key_id\":\"AKIAEXAMPLE\","
            + "\"secret_access_key\":\"s3cr3tValue\"}";

    private static String config(String... blocks) {
        return "{" + String.join(",", blocks) + "}";
    }

    private static String rejection(String document) {
        return assertThrows(IllegalArgumentException.class, () -> ShippingConfig.parse(document)).getMessage();
    }

    @Test
    void aMinimalDirectoryConfigGetsTheDefaults() {
        ShippingConfig parsed = ShippingConfig.parse(config(DIRECTORY, SIGNING));

        assertEquals(StoreType.DIRECTORY, parsed.store().type());
        assertEquals(Path.of("/data/bucket"), parsed.store().directory());
        assertNull(parsed.credentials());
        assertEquals(Path.of("/keys/writer.key"), parsed.signing().keyFile());
        assertEquals(4, parsed.signing().keyId());
        assertNull(parsed.signing().publicKeyFile());
        assertEquals(ShipSettings.defaults(), parsed.ship());
        assertEquals(ShippingConfig.DEFAULT_REQUEST_TIMEOUT, parsed.requestTimeout());
        assertFalse(parsed.iceberg().enabled());
        assertEquals(Duration.ofSeconds(60), parsed.iceberg().commitInterval());
        assertEquals(Duration.ofDays(7), parsed.iceberg().tableRetention());
    }

    @Test
    void everyShipAndIcebergSettingCanBeOverridden() {
        ShippingConfig parsed = ShippingConfig.parse(config(DIRECTORY, SIGNING,
                "\"ship\":{\"interval_ms\":250,\"max_object_bytes\":2097152,\"backlog_cap_bytes\":5242880,"
                        + "\"retention_days\":3,\"request_timeout_ms\":2500}",
                "\"iceberg\":{\"enabled\":true,\"commit_interval_s\":15,\"table_retention_days\":30}"));

        assertEquals(Duration.ofMillis(250), parsed.ship().interval());
        assertEquals(2_097_152, parsed.ship().maxObjectBytes());
        assertEquals(5_242_880, parsed.ship().backlogCapBytes());
        assertEquals(Duration.ofDays(3), parsed.ship().retention());
        assertEquals(Duration.ofMillis(2500), parsed.requestTimeout());
        assertTrue(parsed.iceberg().enabled());
        assertEquals(Duration.ofSeconds(15), parsed.iceberg().commitInterval());
        assertEquals(Duration.ofDays(30), parsed.iceberg().tableRetention());
    }

    @Test
    void anOptionalPublicKeyFileIsRead() {
        ShippingConfig parsed = ShippingConfig.parse(config(DIRECTORY,
                "\"signing\":{\"key_file\":\"/k\",\"key_id\":0,\"public_key_file\":\"/k.pub\"}"));

        assertEquals(Path.of("/k.pub"), parsed.signing().publicKeyFile());
        assertEquals(0, parsed.signing().keyId());
    }

    @Test
    void aMinimalS3ConfigTargetsAwsWithVirtualHostStyle() {
        ShippingConfig parsed = ShippingConfig.parse(config(S3_STATIC, SIGNING));

        assertEquals(StoreType.S3, parsed.store().type());
        assertEquals("graphs", parsed.store().bucket());
        assertEquals("eu-west-1", parsed.store().region());
        assertEquals("", parsed.store().prefix());
        assertNull(parsed.store().endpoint());
        assertNull(parsed.store().pathStyle());
        assertNull(parsed.store().caBundle());
        assertEquals(CredentialKind.STATIC, parsed.credentials().kind());
        assertEquals("AKIAEXAMPLE", parsed.credentials().accessKeyId());
        assertNull(parsed.credentials().sessionToken());
    }

    @Test
    void aCustomS3EndpointPrefixAndCaBundleAreKept() {
        ShippingConfig parsed = ShippingConfig.parse(config(
                "\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"us-east-1\","
                        + "\"endpoint\":\"https://seaweed.internal:8333\",\"prefix\":\"team/a/\",\"path_style\":true,"
                        + "\"ca_bundle\":\"/ca.pem\"},"
                        + "\"credentials\":{\"source\":\"static\",\"access_key_id\":\"a\",\"secret_access_key\":\"s\","
                        + "\"session_token\":\"t\"}", SIGNING));

        assertEquals(URI.create("https://seaweed.internal:8333"), parsed.store().endpoint());
        assertEquals("team/a/", parsed.store().prefix());
        assertTrue(parsed.store().pathStyle());
        assertEquals(Path.of("/ca.pem"), parsed.store().caBundle());
        assertEquals("t", parsed.credentials().sessionToken());
    }

    @Test
    void environmentAndFileCredentialsAreAccepted() {
        String store = "\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"}";

        ShippingConfig environment = ShippingConfig.parse(config(store, SIGNING,
                "\"credentials\":{\"source\":\"environment\"}"));
        ShippingConfig defaultProfile = ShippingConfig.parse(config(store, SIGNING,
                "\"credentials\":{\"source\":\"file\",\"path\":\"/home/x/.aws/credentials\"}"));
        ShippingConfig namedProfile = ShippingConfig.parse(config(store, SIGNING,
                "\"credentials\":{\"source\":\"file\",\"path\":\"/c\",\"profile\":\"prod\"}"));

        assertEquals(CredentialKind.ENVIRONMENT, environment.credentials().kind());
        assertEquals(CredentialKind.FILE, defaultProfile.credentials().kind());
        assertEquals("default", defaultProfile.credentials().profile());
        assertEquals(Path.of("/home/x/.aws/credentials"), defaultProfile.credentials().file());
        assertEquals("prod", namedProfile.credentials().profile());
    }

    @Test
    void theSameDocumentAlwaysParsesToAnEqualConfig() {
        assertEquals(ShippingConfig.parse(config(S3_STATIC, SIGNING)), ShippingConfig.parse(config(S3_STATIC, SIGNING)));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "not json|",
            "[]|",
            "{}|store",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"}}|signing",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"extra\":1}|extra",
            "{\"store\":{\"type\":\"tape\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|type",
            "{\"store\":{\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|type",
            "{\"store\":{\"type\":\"directory\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|directory",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|directory",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\",\"bucket\":\"graphs\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|bucket",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|credentials",
            "{\"store\":{\"type\":\"s3\",\"region\":\"r\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|bucket",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|region",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\",\"directory\":\"/d\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|directory",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|credentials",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\",\"endpoint\":\"ftp://x\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|endpoint",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\",\"endpoint\":\"no-scheme\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|endpoint",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\",\"endpoint\":\"https://bad host\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|endpoint",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"A_B\",\"region\":\"r\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|bucket",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"US EAST\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|region",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\",\"endpoint\":\"http://x:9\",\"ca_bundle\":\"/ca.pem\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|CA bundle",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\",\"prefix\":\"..\"},\"credentials\":{\"source\":\"environment\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|prefix",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"},\"credentials\":{\"source\":\"vault\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|source",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"},\"credentials\":{\"source\":\"static\",\"access_key_id\":\"a\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|secret_access_key",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"},\"credentials\":{\"source\":\"file\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|path",
            "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"},\"credentials\":{\"source\":\"environment\",\"path\":\"/x\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}|path",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_id\":1}}|key_file",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\"}}|key_id",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":-1}}|key_id",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":4294967296}}|key_id",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":\"one\"}}|key_id",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1,\"password\":\"x\"}}|password",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"interval_ms\":0}}|interval_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"interval_ms\":-5}}|interval_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"interval_ms\":5}}|interval_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"interval_ms\":99999999999}}|interval_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"interval_ms\":\"fast\"}}|interval_ms",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"max_object_bytes\":100}}|max_object_bytes",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"max_object_bytes\":99999999999}}|max_object_bytes",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"backlog_cap_bytes\":10}}|backlog_cap_bytes",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"retention_days\":0}}|retention_days",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"retention_days\":9999999}}|retention_days",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"ship\":{\"timeout\":5}}|timeout",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"iceberg\":{\"enabled\":\"yes\"}}|enabled",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"iceberg\":{\"commit_interval_s\":0}}|commit_interval_s",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"iceberg\":{\"table_retention_days\":0}}|table_retention_days",
            "{\"store\":{\"type\":\"directory\",\"directory\":\"/d\"},\"signing\":{\"key_file\":\"/k\",\"key_id\":1},\"iceberg\":{\"bogus\":true}}|bogus"})
    void aBadDocumentIsRejectedWithAMessageThatNamesTheField(String document, String field) {
        String message = rejection(document);

        assertTrue(message.startsWith("invalid shipping configuration: "), message);
        if (field != null && !field.isEmpty()) {
            assertTrue(message.contains(field), "'" + message + "' should name '" + field + "'");
        }
    }

    @Test
    void secretsNeverAppearInAnErrorMessage() {
        String leaky = "{" + "\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"},"
                + "\"credentials\":{\"source\":\"static\",\"access_key_id\":\"AKIASUPERSECRET\","
                + "\"secret_access_key\":\"wJalrSECRETVALUE\",\"session_token\":\"TOKENSECRET\",\"bogus\":1},"
                + "\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}";

        String message = rejection(leaky);

        assertTrue(message.contains("bogus"));
        for (String secret : new String[]{"AKIASUPERSECRET", "wJalrSECRETVALUE", "TOKENSECRET"}) {
            assertFalse(message.contains(secret), message);
        }
    }

    @Test
    void secretsNeverAppearWhenTheJsonIsBroken() {
        String broken = "{\"credentials\":{\"secret_access_key\":\"wJalrSECRETVALUE\"} oops";

        assertFalse(rejection(broken).contains("wJalrSECRETVALUE"));
    }

    @Test
    void secretsNeverAppearWhenAFieldHasTheWrongType() {
        String wrongType = "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"},"
                + "\"credentials\":{\"source\":\"static\",\"access_key_id\":\"AKIASUPERSECRET\","
                + "\"secret_access_key\":12345678},\"signing\":{\"key_file\":\"/k\",\"key_id\":1}}";

        String message = rejection(wrongType);

        assertTrue(message.contains("secret_access_key"));
        assertFalse(message.contains("AKIASUPERSECRET"));
        assertFalse(message.contains("12345678"));
    }

    @Test
    void theCredentialSourceNeverPrintsItsSecrets() {
        ShippingConfig parsed = ShippingConfig.parse(config(S3_STATIC, SIGNING));

        assertFalse(parsed.credentials().toString().contains("s3cr3tValue"));
        assertFalse(parsed.credentials().toString().contains("AKIAEXAMPLE"));
        assertFalse(parsed.toString().contains("s3cr3tValue"));
    }
}
