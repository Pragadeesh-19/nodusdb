package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.FatalStoreException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FileCredentialsProvider implements CredentialsProvider {

    private static final String ACCESS_KEY_ID = "aws_access_key_id";
    private static final String SECRET_ACCESS_KEY = "aws_secret_access_key";
    private static final String SESSION_TOKEN = "aws_session_token";
    private static final String LEGACY_SESSION_TOKEN = "aws_security_token";
    private static final String PROFILE_PREFIX = "profile ";
    private static final char BYTE_ORDER_MARK = '﻿';

    private final Path file;
    private final String profile;
    private volatile Credentials loaded;

    public FileCredentialsProvider(Path file, String profile) {
        this.file = file;
        this.profile = profile;
        this.loaded = read();
    }

    @Override
    public Credentials current() {
        return loaded;
    }

    @Override
    public Credentials refresh() {
        loaded = read();
        return loaded;
    }

    private Credentials read() {
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FatalStoreException("the credentials file cannot be read: " + file.getFileName(), 0, e);
        }
        Map<String, String> values = profileValues(lines);
        String accessKeyId = values.get(ACCESS_KEY_ID);
        String secretAccessKey = values.get(SECRET_ACCESS_KEY);
        if (accessKeyId == null || accessKeyId.isEmpty() || secretAccessKey == null || secretAccessKey.isEmpty()) {
            throw new FatalStoreException("profile '" + profile + "' in the credentials file "
                    + file.getFileName() + " needs " + ACCESS_KEY_ID + " and " + SECRET_ACCESS_KEY, 0);
        }
        String token = values.getOrDefault(SESSION_TOKEN, values.get(LEGACY_SESSION_TOKEN));
        return new Credentials(accessKeyId, secretAccessKey, token == null || token.isEmpty() ? null : token);
    }

    private Map<String, String> profileValues(List<String> lines) {
        Map<String, String> values = new HashMap<>();
        boolean inProfile = false;
        for (int index = 0; index < lines.size(); index++) {
            String line = lines.get(index);
            if (index == 0 && !line.isEmpty() && line.charAt(0) == BYTE_ORDER_MARK) {
                line = line.substring(1);
            }
            line = line.strip();
            if (line.isEmpty() || line.charAt(0) == '#' || line.charAt(0) == ';') {
                continue;
            }
            if (line.charAt(0) == '[' && line.charAt(line.length() - 1) == ']') {
                inProfile = matches(line.substring(1, line.length() - 1).strip());
            } else if (inProfile) {
                int separator = line.indexOf('=');
                if (separator > 0) {
                    values.put(line.substring(0, separator).strip(), line.substring(separator + 1).strip());
                }
            }
        }
        return values;
    }

    private boolean matches(String section) {
        return section.equals(profile) || section.equals(PROFILE_PREFIX + profile);
    }
}
