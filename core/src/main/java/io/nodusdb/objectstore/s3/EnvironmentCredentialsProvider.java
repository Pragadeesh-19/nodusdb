package io.nodusdb.objectstore.s3;

import java.util.Map;
import java.util.function.Supplier;

public final class EnvironmentCredentialsProvider implements CredentialsProvider {

    static final String ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    static final String SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";
    static final String SESSION_TOKEN = "AWS_SESSION_TOKEN";

    private final Supplier<Map<String, String>> environment;
    private volatile Credentials loaded;

    public EnvironmentCredentialsProvider() {
        this(System::getenv);
    }

    public EnvironmentCredentialsProvider(Supplier<Map<String, String>> environment) {
        this.environment = environment;
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
        Map<String, String> variables = environment.get();
        String accessKeyId = variables.get(ACCESS_KEY_ID);
        String secretAccessKey = variables.get(SECRET_ACCESS_KEY);
        if (isBlank(accessKeyId) || isBlank(secretAccessKey)) {
            throw new IllegalArgumentException("the environment must set " + ACCESS_KEY_ID + " and "
                    + SECRET_ACCESS_KEY);
        }
        String token = variables.get(SESSION_TOKEN);
        return new Credentials(accessKeyId, secretAccessKey, isBlank(token) ? null : token);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
