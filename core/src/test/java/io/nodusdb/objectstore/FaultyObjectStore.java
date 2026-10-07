package io.nodusdb.objectstore;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class FaultyObjectStore implements ObjectStore {

    public enum Operation {
        PUT_IF_ABSENT, PUT, PUT_FILE, GET, GET_RANGE, HEAD, LIST, DELETE
    }

    public enum Fault {
        FAIL_BEFORE, FAIL_AFTER, CONFLICT, FATAL, EXPIRED, CRASH_BEFORE, CRASH_AFTER
    }

    public record Call(int ordinal, Operation operation, String key) {
    }

    public static final class SimulatedCrash extends RuntimeException {

        private static final long serialVersionUID = 1L;

        SimulatedCrash(String message) {
            super(message);
        }
    }

    private static final class Rule {

        private final Predicate<Call> matcher;
        private final Fault fault;
        private int remaining;

        Rule(Predicate<Call> matcher, Fault fault, int remaining) {
            this.matcher = matcher;
            this.fault = fault;
            this.remaining = remaining;
        }
    }

    private final ObjectStore delegate;
    private final List<Rule> rules = new ArrayList<>();
    private final List<Call> log = new ArrayList<>();
    private Random random;
    private double failBefore;
    private double failAfter;
    private double conflict;
    private boolean ignoreConditionalWrites;
    private String conflictExemptPrefix;
    private int ordinal;

    public FaultyObjectStore(ObjectStore delegate) {
        this.delegate = delegate;
    }

    public synchronized FaultyObjectStore failCall(int callOrdinal, Fault fault) {
        rules.add(new Rule(call -> call.ordinal() == callOrdinal, fault, 1));
        return this;
    }

    public synchronized FaultyObjectStore failNext(Operation operation, Fault fault) {
        return failNext(operation, 1, fault);
    }

    public synchronized FaultyObjectStore failNext(Operation operation, int times, Fault fault) {
        rules.add(new Rule(call -> call.operation() == operation, fault, times));
        return this;
    }

    public synchronized FaultyObjectStore failWhen(Predicate<Call> matcher, int times, Fault fault) {
        rules.add(new Rule(matcher, fault, times));
        return this;
    }

    public synchronized FaultyObjectStore randomFaults(long seed, double failBefore, double failAfter,
                                                       double conflict) {
        this.random = new Random(seed);
        this.failBefore = failBefore;
        this.failAfter = failAfter;
        this.conflict = conflict;
        return this;
    }

    public synchronized FaultyObjectStore exemptFromRandomConflicts(String keyPrefix) {
        this.conflictExemptPrefix = keyPrefix;
        return this;
    }

    public synchronized FaultyObjectStore stopRandomFaults() {
        this.random = null;
        return this;
    }

    public synchronized FaultyObjectStore ignoreConditionalWrites() {
        this.ignoreConditionalWrites = true;
        return this;
    }

    public synchronized List<Call> calls() {
        return List.copyOf(log);
    }

    public synchronized int callCount() {
        return log.size();
    }

    public synchronized long count(Operation operation) {
        return log.stream().filter(call -> call.operation() == operation).count();
    }

    @Override
    public PutResult putIfAbsent(String key, byte[] content, Map<String, String> metadata) {
        return invoke(Operation.PUT_IF_ABSENT, key, () -> {
            if (conditionalWritesIgnored()) {
                delegate.put(key, content);
                return PutResult.CREATED;
            }
            return delegate.putIfAbsent(key, content, metadata);
        }, () -> PutResult.CONFLICT);
    }

    @Override
    public void put(String key, byte[] content) {
        invoke(Operation.PUT, key, () -> {
            delegate.put(key, content);
            return null;
        }, null);
    }

    @Override
    public void putFile(String key, Path file, Map<String, String> metadata) {
        invoke(Operation.PUT_FILE, key, () -> {
            delegate.putFile(key, file, metadata);
            return null;
        }, null);
    }

    @Override
    public Optional<byte[]> get(String key) {
        return invoke(Operation.GET, key, () -> delegate.get(key), null);
    }

    @Override
    public Optional<byte[]> getRange(String key, long offset, int length) {
        return invoke(Operation.GET_RANGE, key, () -> delegate.getRange(key, offset, length), null);
    }

    @Override
    public Optional<ObjectInfo> head(String key) {
        return invoke(Operation.HEAD, key, () -> delegate.head(key), null);
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) {
        return invoke(Operation.LIST, prefix, () -> delegate.list(prefix, startAfter, maxKeys), null);
    }

    @Override
    public void delete(String key) {
        invoke(Operation.DELETE, key, () -> {
            delegate.delete(key);
            return null;
        }, null);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        for (String key : keys) {
            delete(key);
        }
    }

    @Override
    public int abortStaleUploads(String prefix, Duration olderThan, Instant now) {
        return delegate.abortStaleUploads(prefix, olderThan, now);
    }

    private synchronized boolean conditionalWritesIgnored() {
        return ignoreConditionalWrites;
    }

    private synchronized Fault next(Operation operation, String key) {
        Call call = new Call(++ordinal, operation, key);
        log.add(call);
        for (Rule rule : rules) {
            if (rule.remaining > 0 && rule.matcher.test(call)) {
                rule.remaining--;
                return rule.fault;
            }
        }
        if (random == null) {
            return null;
        }
        double draw = random.nextDouble();
        if (draw < failBefore) {
            return Fault.FAIL_BEFORE;
        }
        if (draw < failBefore + failAfter) {
            return Fault.FAIL_AFTER;
        }
        boolean conflictAllowed = conflictExemptPrefix == null || !key.startsWith(conflictExemptPrefix);
        if (operation == Operation.PUT_IF_ABSENT && conflictAllowed && draw < failBefore + failAfter + conflict) {
            return Fault.CONFLICT;
        }
        return null;
    }

    private <T> T invoke(Operation operation, String key, Supplier<T> action, Supplier<T> onConflict) {
        Fault fault = next(operation, key);
        if (fault == null) {
            return action.get();
        }
        return switch (fault) {
            case FAIL_BEFORE -> throw new TransientStoreException("injected failure before " + operation, 0);
            case FAIL_AFTER -> {
                action.get();
                throw new TransientStoreException("injected failure after " + operation, 0);
            }
            case CONFLICT -> {
                if (onConflict == null) {
                    throw new IllegalStateException("only a conditional write can conflict");
                }
                yield onConflict.get();
            }
            case FATAL -> throw new FatalStoreException("injected rejection of " + operation, 403);
            case EXPIRED -> throw new ExpiredCredentialsException("injected expired credentials", 403);
            case CRASH_BEFORE -> throw new SimulatedCrash("crash before " + operation + " " + key);
            case CRASH_AFTER -> {
                action.get();
                throw new SimulatedCrash("crash after " + operation + " " + key);
            }
        };
    }
}
