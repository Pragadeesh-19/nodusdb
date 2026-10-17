package io.nodusdb.bench;

import io.nodusdb.authz.Durability;
import io.nodusdb.authz.TupleTransaction;
import io.nodusdb.capi.GraphSession;
import io.nodusdb.capi.GraphSessions;
import io.nodusdb.chain.KeyFiles;
import io.nodusdb.json.JsonParser;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.past.PastTarget;
import io.nodusdb.log.LogConfig;
import io.nodusdb.ship.ShippingConfig;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class AsOfBench {

    private static final String SCHEMA = """
            schema 1
            type user
            type document {
              relation viewer: user
              permission view = viewer
            }
            """;
    private static final int GRANTS_PER_TRANSACTION = 5_000;
    private static final Duration WAIT = Duration.ofSeconds(120);

    @Param({"10000", "100000"})
    public int tuples;

    @Param({"snapshot", "replay"})
    public String from;

    private Path directory;
    private GraphSessions sessions;
    private GraphSession live;
    private Token target;

    @Setup(Level.Trial)
    public void shipAGraph() throws IOException {
        directory = Files.createTempDirectory("nodus-asof-bench-");
        Path key = directory.resolve("signing.pem");
        Path publicKey = directory.resolve("signing.pub");
        KeyFiles.generateTo(key, publicKey);
        ShippingConfig shipping = ShippingConfig.parse("{\"store\":{\"type\":\"directory\",\"directory\":\""
                + escape(directory.resolve("bucket")) + "\"},\"signing\":{\"key_file\":\"" + escape(key)
                + "\",\"key_id\":1,\"public_key_file\":\"" + escape(publicKey) + "\"},\"ship\":{\"interval_ms\":10}}");
        sessions = new GraphSessions();
        live = sessions.get(sessions.openDurable(directory.resolve("graph"), LogConfig.DEFAULT,
                GraphKernel.NO_MEMORY_LIMIT, shipping));
        Token schema = live.applySchema(SCHEMA);
        if ("replay".equals(from)) {
            live.checkpoint();
            awaitReference(schema.lsn());
        }
        for (int first = 0; first < tuples; first += GRANTS_PER_TRANSACTION) {
            TupleTransaction transaction = new TupleTransaction();
            for (int i = first; i < Math.min(tuples, first + GRANTS_PER_TRANSACTION); i++) {
                transaction.add("document:d" + i, "viewer", "user:u" + i, null);
            }
            target = live.writeTuples(transaction, Durability.LAKE);
        }
        if ("snapshot".equals(from)) {
            live.checkpoint();
            awaitReference(target.lsn());
        }
    }

    private void awaitReference(long lsn) {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (JsonParser.parseObject(live.statsJson()).requireObject("shipping")
                .requireObject("snapshot_references").requireLong("lsn") < lsn) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("the snapshot at LSN " + lsn + " was not referenced in time");
            }
            Thread.onSpinWait();
        }
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        live.close();
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path path : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    @Benchmark
    public Token buildAPastView() {
        GraphSession view = live.openPast(new PastTarget.AtToken(target), GraphKernel.NO_MEMORY_LIMIT, WAIT);
        try {
            return view.token();
        } finally {
            view.close();
        }
    }

    private static String escape(Path path) {
        return path.toString().replace("\\", "\\\\");
    }
}
