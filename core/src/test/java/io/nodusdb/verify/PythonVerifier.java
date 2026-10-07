package io.nodusdb.verify;

import io.nodusdb.json.JsonObject;
import io.nodusdb.json.JsonParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class PythonVerifier {

    public static final String INTERPRETER_VARIABLE = "NODUS_PYTHON";
    public static final String REQUIRE_VARIABLE = "NODUS_REQUIRE_PYTHON_VERIFIERS";

    private static final int MODULE_MISSING = 77;
    private static final long TIMEOUT_SECONDS = 120;

    private PythonVerifier() {
    }

    public static Optional<JsonObject> run(Path scratch, String script, String... arguments) {
        Path file = extract(scratch, script);
        for (String interpreter : interpreters()) {
            Optional<String> output = attempt(interpreter, file, arguments);
            if (output.isPresent()) {
                return Optional.of(JsonParser.parseObject(output.get().getBytes(StandardCharsets.UTF_8)));
            }
        }
        if (System.getenv(REQUIRE_VARIABLE) != null) {
            throw new AssertionError("no Python interpreter with the modules " + script + " needs is available, and "
                    + REQUIRE_VARIABLE + " is set");
        }
        return Optional.empty();
    }

    private static List<String> interpreters() {
        List<String> candidates = new ArrayList<>();
        String configured = System.getenv(INTERPRETER_VARIABLE);
        if (configured != null && !configured.isBlank()) {
            candidates.add(configured);
        } else {
            candidates.add("python");
            candidates.add("python3");
        }
        return candidates;
    }

    private static Optional<String> attempt(String interpreter, Path script, String[] arguments) {
        List<String> command = new ArrayList<>();
        command.add(interpreter);
        command.add(script.toString());
        command.addAll(List.of(arguments));
        Process process;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(false).start();
        } catch (IOException notInstalled) {
            return Optional.empty();
        }
        try {
            Path error = Files.createTempFile("verify-stderr", ".txt");
            Path output = Files.createTempFile("verify-stdout", ".txt");
            try {
                process.getOutputStream().close();
                Thread drainErr = pump(process.getErrorStream(), error);
                Thread drainOut = pump(process.getInputStream(), output);
                if (!process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    throw new AssertionError(script.getFileName() + " did not finish in " + TIMEOUT_SECONDS + " s");
                }
                drainErr.join();
                drainOut.join();
                int exit = process.exitValue();
                if (exit == MODULE_MISSING) {
                    return Optional.empty();
                }
                if (exit != 0) {
                    throw new AssertionError(script.getFileName() + " failed with exit " + exit + ": "
                            + Files.readString(error, StandardCharsets.UTF_8));
                }
                return Optional.of(Files.readString(output, StandardCharsets.UTF_8));
            } finally {
                Files.deleteIfExists(error);
                Files.deleteIfExists(output);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while running " + script.getFileName());
        }
    }

    private static Thread pump(InputStream stream, Path target) {
        Thread thread = new Thread(() -> {
            try (InputStream in = stream) {
                Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ignored) {
                return;
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static Path extract(Path scratch, String script) {
        try (InputStream in = PythonVerifier.class.getResourceAsStream("/verify/" + script)) {
            if (in == null) {
                throw new IllegalArgumentException("no verifier script named " + script);
            }
            Path target = scratch.resolve(script);
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            return target;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
