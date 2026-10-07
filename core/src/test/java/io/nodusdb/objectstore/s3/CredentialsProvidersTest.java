package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.FatalStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CredentialsProvidersTest {

    private static final String SECRET = "very-secret-value-123";

    @TempDir
    Path scratch;

    private Path write(String content) throws IOException {
        Path file = scratch.resolve("credentials");
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    @Test
    void staticCredentialsNeverChange() {
        Credentials credentials = new Credentials("AKID", SECRET);
        StaticCredentialsProvider provider = new StaticCredentialsProvider(credentials);

        assertSame(credentials, provider.current());
        assertSame(credentials, provider.refresh());
    }

    @Test
    void staticCredentialsNeedValues() {
        assertThrows(NullPointerException.class, () -> new StaticCredentialsProvider(null));
    }

    @Test
    void theEnvironmentSuppliesKeysAndAnOptionalToken() {
        Map<String, String> environment = new HashMap<>();
        environment.put("AWS_ACCESS_KEY_ID", "AKID");
        environment.put("AWS_SECRET_ACCESS_KEY", SECRET);

        EnvironmentCredentialsProvider provider = new EnvironmentCredentialsProvider(() -> environment);

        assertEquals(new Credentials("AKID", SECRET), provider.current());
        assertNull(provider.current().sessionToken());
    }

    @Test
    void theEnvironmentTokenIsUsedWhenPresentAndIgnoredWhenBlank() {
        Map<String, String> environment = new HashMap<>(Map.of("AWS_ACCESS_KEY_ID", "AKID",
                "AWS_SECRET_ACCESS_KEY", SECRET, "AWS_SESSION_TOKEN", "tok"));
        EnvironmentCredentialsProvider provider = new EnvironmentCredentialsProvider(() -> environment);
        assertEquals("tok", provider.current().sessionToken());

        environment.put("AWS_SESSION_TOKEN", "  ");

        assertNull(provider.refresh().sessionToken());
    }

    @Test
    void refreshingTheEnvironmentPicksUpNewValues() {
        Map<String, String> environment = new HashMap<>(Map.of("AWS_ACCESS_KEY_ID", "AKID1",
                "AWS_SECRET_ACCESS_KEY", "one"));
        EnvironmentCredentialsProvider provider = new EnvironmentCredentialsProvider(() -> environment);

        environment.put("AWS_ACCESS_KEY_ID", "AKID2");
        environment.put("AWS_SECRET_ACCESS_KEY", "two");

        assertEquals("AKID1", provider.current().accessKeyId());
        assertEquals(new Credentials("AKID2", "two"), provider.refresh());
        assertEquals("AKID2", provider.current().accessKeyId());
    }

    @Test
    void anEnvironmentWithoutKeysIsRefusedWithoutEchoingValues() {
        Map<String, String> onlyKey = Map.of("AWS_ACCESS_KEY_ID", "AKID-visible");
        Map<String, String> onlySecret = Map.of("AWS_SECRET_ACCESS_KEY", SECRET);

        assertThrows(IllegalArgumentException.class, () -> new EnvironmentCredentialsProvider(Map::of));
        assertThrows(IllegalArgumentException.class, () -> new EnvironmentCredentialsProvider(() -> onlyKey));
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> new EnvironmentCredentialsProvider(() -> onlySecret));
        assertFalse(refused.getMessage().contains(SECRET));
    }

    @Test
    void aBlankEnvironmentValueCountsAsMissing() {
        Map<String, String> blank = Map.of("AWS_ACCESS_KEY_ID", " ", "AWS_SECRET_ACCESS_KEY", SECRET);

        assertThrows(IllegalArgumentException.class, () -> new EnvironmentCredentialsProvider(() -> blank));
    }

    @Test
    void aProfileIsReadFromTheFile() throws IOException {
        Path file = write("[default]\naws_access_key_id = AKDEFAULT\naws_secret_access_key = " + SECRET + "\n");

        Credentials credentials = new FileCredentialsProvider(file, "default").current();

        assertEquals(new Credentials("AKDEFAULT", SECRET), credentials);
    }

    @Test
    void theRequestedProfileIsChosenAmongSeveral() throws IOException {
        Path file = write("""
                [default]
                aws_access_key_id = AKDEFAULT
                aws_secret_access_key = default-secret

                [prod]
                aws_access_key_id = AKPROD
                aws_secret_access_key = prod-secret
                aws_session_token = prod-token

                [profile dev]
                aws_access_key_id = AKDEV
                aws_secret_access_key = dev-secret
                """);

        assertEquals(new Credentials("AKPROD", "prod-secret", "prod-token"),
                new FileCredentialsProvider(file, "prod").current());
        assertEquals(new Credentials("AKDEV", "dev-secret"), new FileCredentialsProvider(file, "dev").current());
        assertEquals("AKDEFAULT", new FileCredentialsProvider(file, "default").current().accessKeyId());
    }

    @Test
    void commentsBlankLinesWindowsLineEndingsAndABomAreTolerated() throws IOException {
        Path file = write("﻿# a comment\r\n; another\r\n\r\n[default]\r\n  aws_access_key_id=AK\r\n"
                + "aws_secret_access_key =   spaced secret   \r\n# trailing\r\n");

        Credentials credentials = new FileCredentialsProvider(file, "default").current();

        assertEquals(new Credentials("AK", "spaced secret"), credentials);
    }

    @Test
    void theLegacySecurityTokenKeyIsAccepted() throws IOException {
        Path file = write("[default]\naws_access_key_id=AK\naws_secret_access_key=s\naws_security_token=legacy\n");

        assertEquals("legacy", new FileCredentialsProvider(file, "default").current().sessionToken());
    }

    @Test
    void aLaterSectionWithTheSameNameOverridesAnEarlierOne() throws IOException {
        Path file = write("[default]\naws_access_key_id=AK1\naws_secret_access_key=s1\n"
                + "[other]\nx=y\n[default]\naws_access_key_id=AK2\n");

        assertEquals(new Credentials("AK2", "s1"), new FileCredentialsProvider(file, "default").current());
    }

    @Test
    void valuesContainingEqualsSignsKeepThem() throws IOException {
        Path file = write("[default]\naws_access_key_id=AK\naws_secret_access_key=abc=def==\n");

        assertEquals("abc=def==", new FileCredentialsProvider(file, "default").current().secretAccessKey());
    }

    @Test
    void refreshingTheFilePicksUpRotatedKeys() throws IOException {
        Path file = write("[default]\naws_access_key_id=AK1\naws_secret_access_key=one\n");
        FileCredentialsProvider provider = new FileCredentialsProvider(file, "default");

        write("[default]\naws_access_key_id=AK2\naws_secret_access_key=two\naws_session_token=fresh\n");

        assertEquals("AK1", provider.current().accessKeyId());
        assertEquals(new Credentials("AK2", "two", "fresh"), provider.refresh());
        assertEquals("AK2", provider.current().accessKeyId());
    }

    @Test
    void aFailedRefreshKeepsTheLastGoodCredentials() throws IOException {
        Path file = write("[default]\naws_access_key_id=AK1\naws_secret_access_key=one\n");
        FileCredentialsProvider provider = new FileCredentialsProvider(file, "default");

        Files.delete(file);

        assertThrows(FatalStoreException.class, provider::refresh);
        assertEquals("AK1", provider.current().accessKeyId());
    }

    @Test
    void aMissingProfileOrMissingKeysAreRefusedWithoutEchoingSecrets() throws IOException {
        Path file = write("[default]\naws_access_key_id=AK\naws_secret_access_key=" + SECRET + "\n[half]\n"
                + "aws_access_key_id=AK\n");

        FatalStoreException missingProfile = assertThrows(FatalStoreException.class,
                () -> new FileCredentialsProvider(file, "absent"));
        FatalStoreException missingSecret = assertThrows(FatalStoreException.class,
                () -> new FileCredentialsProvider(file, "half"));

        assertFalse(missingProfile.getMessage().contains(SECRET));
        assertFalse(missingSecret.getMessage().contains(SECRET));
        assertTrue(missingProfile.getMessage().contains("profile 'absent'"));
    }

    @Test
    void aMissingFileIsRefusedByName() {
        FatalStoreException refused = assertThrows(FatalStoreException.class,
                () -> new FileCredentialsProvider(scratch.resolve("nothing"), "default"));

        assertTrue(refused.getMessage().contains("nothing"));
    }

    @Test
    void emptyValuesCountAsMissing() throws IOException {
        Path file = write("[default]\naws_access_key_id=AK\naws_secret_access_key=\n");

        assertThrows(FatalStoreException.class, () -> new FileCredentialsProvider(file, "default"));
    }
}
