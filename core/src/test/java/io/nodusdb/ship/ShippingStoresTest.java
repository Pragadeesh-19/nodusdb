package io.nodusdb.ship;

import io.nodusdb.objectstore.DirectoryObjectStore;
import io.nodusdb.objectstore.s3.Credentials;
import io.nodusdb.objectstore.s3.CredentialsProvider;
import io.nodusdb.objectstore.s3.EnvironmentCredentialsProvider;
import io.nodusdb.objectstore.s3.FileCredentialsProvider;
import io.nodusdb.objectstore.s3.S3ObjectStore;
import io.nodusdb.objectstore.s3.StaticCredentialsProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShippingStoresTest {

    private static final String SIGNING = "\"signing\":{\"key_file\":\"/k\",\"key_id\":1}";
    private static final String STATIC = "\"credentials\":{\"source\":\"static\",\"access_key_id\":\"a\","
            + "\"secret_access_key\":\"s\"}";

    @TempDir
    Path root;

    private static ShippingConfig s3(String store, String credentials) {
        return ShippingConfig.parse("{" + store + "," + credentials + "," + SIGNING + "}");
    }

    @Test
    void aDirectoryStoreIsOpenedAtItsRootWithTheTableBesideIt() {
        ShippingConfig config = ShippingConfig.parse("{\"store\":{\"type\":\"directory\",\"directory\":\""
                + root.toString().replace("\\", "\\\\") + "\"}," + SIGNING + "}");

        ShippingStores.Opened opened = ShippingStores.open(config);

        assertInstanceOf(DirectoryObjectStore.class, opened.store());
        assertEquals(root.toAbsolutePath().toString().replace('\\', '/') + "/iceberg", opened.icebergLocation());
    }

    @Test
    void anS3StoreKeepsItsBucketAndPrefixInTheTableLocation() {
        ShippingConfig config = s3("\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"eu-west-1\","
                + "\"prefix\":\"team/a\"}", STATIC);

        ShippingStores.Opened opened = ShippingStores.open(config);

        assertInstanceOf(S3ObjectStore.class, opened.store());
        assertEquals("s3://graphs/team/a/iceberg", opened.icebergLocation());
        opened.store().close();
    }

    @Test
    void anS3StoreWithoutAPrefixPutsTheTableAtTheBucketRoot() {
        ShippingConfig config = s3("\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"us-east-1\"}",
                STATIC);

        ShippingStores.Opened opened = ShippingStores.open(config);

        assertEquals("s3://graphs/iceberg", opened.icebergLocation());
        opened.store().close();
    }

    @Test
    void aCustomEndpointWithPathStyleAddressingIsAccepted() {
        ShippingConfig config = s3("\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"us-east-1\","
                + "\"endpoint\":\"http://localhost:8333\",\"path_style\":true}", STATIC);

        ShippingStores.Opened opened = ShippingStores.open(config);

        assertInstanceOf(S3ObjectStore.class, opened.store());
        opened.store().close();
    }

    @Test
    void theStoreIsValidatedWhenTheConfigIsParsedNotWhenItIsOpened() {
        for (String store : new String[]{
                "\"store\":{\"type\":\"s3\",\"bucket\":\"A_B\",\"region\":\"us-east-1\"}",
                "\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"US EAST\"}",
                "\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\",\"endpoint\":\"http://x:9\","
                        + "\"ca_bundle\":\"/ca.pem\"}",
                "\"store\":{\"type\":\"s3\",\"bucket\":\"a.b.c\",\"region\":\"r\"}"}) {
            assertThrows(IllegalArgumentException.class, () -> s3(store, STATIC), store);
        }
    }

    @Test
    void theStaticCredentialsKindBuildsAFixedProvider() {
        CredentialsProvider fixed = ShippingStores.provider(new ShippingConfig.CredentialSource(
                ShippingConfig.CredentialKind.STATIC, "id", "secret", "token", null, null), Map::of);

        assertInstanceOf(StaticCredentialsProvider.class, fixed);
        assertEquals(new Credentials("id", "secret", "token"), fixed.current());
        assertTrue(fixed.current().hasSessionToken());
    }

    @Test
    void theEnvironmentKindReadsTheSuppliedEnvironment() {
        ShippingConfig.CredentialSource source = new ShippingConfig.CredentialSource(
                ShippingConfig.CredentialKind.ENVIRONMENT, null, null, null, null, null);

        CredentialsProvider provider = ShippingStores.provider(source,
                () -> Map.of("AWS_ACCESS_KEY_ID", "envid", "AWS_SECRET_ACCESS_KEY", "envsecret"));

        assertInstanceOf(EnvironmentCredentialsProvider.class, provider);
        assertEquals("envid", provider.current().accessKeyId());
    }

    @Test
    void anEnvironmentWithoutCredentialsIsRefusedWithoutEchoingAnything() {
        ShippingConfig.CredentialSource source = new ShippingConfig.CredentialSource(
                ShippingConfig.CredentialKind.ENVIRONMENT, null, null, null, null, null);

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> ShippingStores.provider(source, () -> Map.of("AWS_ACCESS_KEY_ID", "partial")));

        assertTrue(refused.getMessage().contains("AWS_SECRET_ACCESS_KEY"));
        assertFalse(refused.getMessage().contains("partial"));
    }

    @Test
    void theFileKindReadsTheNamedProfile() throws IOException {
        Path credentials = root.resolve("credentials");
        Files.writeString(credentials, "[default]\naws_access_key_id=wrong\naws_secret_access_key=wrong\n"
                + "[prod]\naws_access_key_id=fileid\naws_secret_access_key=filesecret\n");

        CredentialsProvider file = ShippingStores.provider(new ShippingConfig.CredentialSource(
                ShippingConfig.CredentialKind.FILE, null, null, null, credentials, "prod"), Map::of);

        assertInstanceOf(FileCredentialsProvider.class, file);
        assertEquals(new Credentials("fileid", "filesecret"), file.current());
    }

    @Test
    void aMissingProfileFileIsRefused() {
        ShippingConfig.CredentialSource source = new ShippingConfig.CredentialSource(
                ShippingConfig.CredentialKind.FILE, null, null, null, root.resolve("absent"), "default");

        assertThrows(RuntimeException.class, () -> ShippingStores.provider(source, Map::of));
    }

    @Test
    void anEmptyStaticCredentialIsRejectedByNameWhenTheConfigIsParsed() {
        String document = "{\"store\":{\"type\":\"s3\",\"bucket\":\"graphs\",\"region\":\"r\"},\"credentials\":"
                + "{\"source\":\"static\",\"access_key_id\":\"\",\"secret_access_key\":\"s\"}," + SIGNING + "}";

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> ShippingConfig.parse(document));

        assertTrue(refused.getMessage().contains("access_key_id"), refused.getMessage());
    }
}
