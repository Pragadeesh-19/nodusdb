package io.nodusdb.authz;

import io.nodusdb.authz.TupleResolver.Resolution;
import io.nodusdb.authz.TupleResolver.Resolved;
import io.nodusdb.authz.schema.CompiledSchema;
import io.nodusdb.authz.schema.SchemaCompiler;
import io.nodusdb.authz.schema.SchemaPlan;
import io.nodusdb.error.SchemaViolationException;
import io.nodusdb.error.StaleReadException;
import io.nodusdb.error.TokenLostException;
import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.KeyKind;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.symbols.SymbolTable;
import io.nodusdb.log.ShipWatermark;
import io.nodusdb.log.record.RecordBatch;
import io.nodusdb.log.record.RecordFormat;
import io.nodusdb.log.record.RecordType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public final class TupleStore {

    public static final int DEFAULT_DEPTH_LIMIT = 32;
    public static final Duration DEFAULT_SHIP_WAIT = Duration.ofSeconds(30);

    private static final long MAX_WAIT_NANOS = TimeUnit.DAYS.toNanos(3_650);

    private final GraphKernel kernel;
    private final NodeTypes types;
    private final int depthLimit;
    private final ReentrantLock writer = new ReentrantLock();
    private final ThreadLocal<CheckEvaluator> evaluators;
    private volatile CompiledSchema model;

    private TupleStore(GraphKernel kernel, int depthLimit) {
        this.kernel = kernel;
        this.depthLimit = depthLimit;
        this.types = new NodeTypes(kernel.symbols());
        this.evaluators = ThreadLocal.withInitial(() -> new CheckEvaluator(kernel, types, depthLimit));
        this.model = kernel.catalog().version() == 0
                ? CompiledSchema.EMPTY : SchemaCompiler.rebuild(kernel.catalog(), kernel.symbols());
    }

    public static TupleStore open(GraphKernel kernel) {
        return open(kernel, DEFAULT_DEPTH_LIMIT);
    }

    public static TupleStore open(GraphKernel kernel, int depthLimit) {
        Objects.requireNonNull(kernel, "kernel");
        if (depthLimit < 1) {
            throw new IllegalArgumentException("the depth limit must be at least 1: " + depthLimit);
        }
        return new TupleStore(kernel, depthLimit);
    }

    public GraphKernel kernel() {
        return kernel;
    }

    public int schemaVersion() {
        return model.version();
    }

    public int depthLimit() {
        return depthLimit;
    }

    public Token token() {
        return kernel.token();
    }

    public Token applySchema(String document) {
        Objects.requireNonNull(document, "document");
        writer.lock();
        try {
            claimStringKeys();
            SchemaPlan plan = SchemaCompiler.plan(document, kernel.catalog(), kernel.symbols());
            RecordBatch batch = new RecordBatch();
            int next = kernel.symbols().size();
            for (String name : plan.newSymbols()) {
                byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
                batch.symbol(next++, bytes, 0, bytes.length);
            }
            batch.schema(plan.version(), plan.digest(), plan.relationIds(), plan.typeSymbols(), plan.nameSymbols(),
                    plan.flags(), plan.canonicalDocument());
            batch.commit();
            long lsn = kernel.commit(batch);
            model = SchemaCompiler.rebuild(kernel.catalog(), kernel.symbols());
            return new Token(kernel.epoch(), lsn);
        } finally {
            writer.unlock();
        }
    }

    public Token add(String object, String relation, String subject) {
        return add(object, relation, subject, null);
    }

    public Token add(String object, String relation, String subject, String subjectRelation) {
        return write(new TupleTransaction().add(object, relation, subject, subjectRelation));
    }

    public Token remove(String object, String relation, String subject) {
        return remove(object, relation, subject, null);
    }

    public Token remove(String object, String relation, String subject, String subjectRelation) {
        return write(new TupleTransaction().remove(object, relation, subject, subjectRelation));
    }

    public Token write(TupleTransaction transaction) {
        return write(transaction, Durability.LOCAL);
    }

    public Token write(TupleTransaction transaction, Durability durability) {
        return write(transaction, durability, DEFAULT_SHIP_WAIT);
    }

    public Token write(TupleTransaction transaction, Durability durability, Duration shipTimeout) {
        Objects.requireNonNull(transaction, "transaction");
        Objects.requireNonNull(shipTimeout, "shipTimeout");
        if (durability != Durability.LAKE) {
            return writeLocal(transaction);
        }
        requireShipping();
        Token token = writeLocal(transaction);
        awaitShipped(token, shipTimeout);
        return token;
    }

    public void awaitShipped(Token token, Duration timeout) {
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("the shipping wait must be positive: " + timeout);
        }
        ShipWatermark watermark = requireShipping();
        requireFresh(token);
        watermark.awaitShipped(token.epoch(), token.lsn(), waitNanos(timeout));
    }

    private ShipWatermark requireShipping() {
        ShipWatermark watermark = kernel.shipWatermark();
        if (!watermark.configured()) {
            throw new UnsupportedFeatureException("shipping is not configured for this graph");
        }
        return watermark;
    }

    private static long waitNanos(Duration timeout) {
        try {
            return Math.min(timeout.toNanos(), MAX_WAIT_NANOS);
        } catch (ArithmeticException tooLong) {
            return MAX_WAIT_NANOS;
        }
    }

    private Token writeLocal(TupleTransaction transaction) {
        writer.lock();
        try {
            claimStringKeys();
            Resolution resolution = new TupleResolver(model, kernel.symbols()).resolve(transaction.operations());
            if (resolution.tuples().isEmpty()) {
                return kernel.token();
            }
            if (resolution.newSymbols().isEmpty() && resolution.tuples().size() == 1) {
                return commitSingle(resolution.tuples().get(0));
            }
            return commitAll(resolution);
        } finally {
            writer.unlock();
        }
    }

    public boolean check(String object, String permission, String subject) {
        return check(object, permission, subject, null);
    }

    public boolean check(String object, String permission, String subject, Token atLeast) {
        if (atLeast != null) {
            requireFresh(atLeast);
        }
        CompiledSchema current = model;
        ObjectNames.requireSchema(current);
        int objectType = ObjectNames.typeIndex(current, object);
        ObjectNames.typeIndex(current, subject);
        int definition = current.definition(objectType, permission);
        if (definition == CompiledSchema.NO_DEF) {
            throw new SchemaViolationException("type '" + current.typeName(objectType)
                    + "' has no relation or permission '" + permission + "'");
        }
        int objectNode = nodeOf(object);
        int subjectNode = nodeOf(subject);
        if (objectNode < 0 || subjectNode < 0) {
            return false;
        }
        return evaluators.get().check(current, objectNode, definition, subjectNode);
    }

    private void requireFresh(Token token) {
        long current = kernel.epoch();
        if (token.epoch() > current) {
            throw new StaleReadException("the token belongs to epoch " + token.epoch() + " but this graph is in epoch "
                    + current);
        }
        if (token.epoch() < current) {
            if (kernel.epochHistory().isLost(token.epoch(), token.lsn())) {
                throw new TokenLostException("the write at " + token + " was acknowledged but never became durable");
            }
            return;
        }
        if (token.lsn() > kernel.appliedLsn()) {
            throw new StaleReadException("the graph has applied LSN " + kernel.appliedLsn()
                    + " but the token needs " + token.lsn());
        }
    }

    private void claimStringKeys() {
        KeyKind kind = kernel.keyKind();
        if (kind == KeyKind.INTEGER) {
            throw new IllegalStateException("the graph is keyed by integers and cannot hold typed tuples");
        }
        if (kind == KeyKind.UNSET) {
            kernel.claimKeyKind(KeyKind.STRING);
        }
    }

    private Token commitSingle(Resolved tuple) {
        long lsn = tuple.add()
                ? kernel.addTuple(tuple.object(), tuple.relation(), tuple.subjectRelation(), tuple.subject())
                : kernel.removeTuple(tuple.object(), tuple.relation(), tuple.subjectRelation(), tuple.subject());
        return lsn == GraphKernel.UNCHANGED ? kernel.token() : new Token(kernel.epoch(), lsn);
    }

    private Token commitAll(Resolution resolution) {
        RecordBatch batch = new RecordBatch();
        int next = resolution.firstNewSymbol();
        for (byte[] name : resolution.newSymbols()) {
            batch.symbol(next++, name, 0, name.length);
        }
        for (Resolved tuple : resolution.tuples()) {
            batch.tuple(tuple.add() ? RecordType.TUPLE_ADD : RecordType.TUPLE_REMOVE, tuple.object(),
                    tuple.relation(), tuple.subjectRelation(), tuple.subject());
        }
        batch.commit();
        if (batch.size() > RecordFormat.WRITE_LIMIT_BYTES) {
            throw new IllegalArgumentException("a transaction of " + batch.size() + " bytes exceeds the limit of "
                    + RecordFormat.WRITE_LIMIT_BYTES + "; split it");
        }
        return new Token(kernel.epoch(), kernel.commit(batch));
    }

    private int nodeOf(String name) {
        SymbolTable symbols = kernel.symbols();
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return symbols.lookup(bytes, 0, bytes.length);
    }
}
