package io.nodusdb.bench;

import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Token;
import io.nodusdb.kernel.changes.ChangeRing;
import io.nodusdb.log.LogConfig;
import io.nodusdb.storage.DurableGraph;
import io.nodusdb.watch.ChangeBatch;
import io.nodusdb.watch.ChangeFeed;

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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class ChangeFeedBench {

    private static final int CHANGES_PER_READ = 1_000;
    private static final int WRITES = 4 * CHANGES_PER_READ;

    @Param({"ring", "log"})
    public String source;

    private Path directory;
    private GraphKernel kernel;
    private ChangeFeed feed;
    private Token cursor;

    @Setup(Level.Trial)
    public void writeTheChanges() throws IOException {
        if ("log".equals(source)) {
            directory = Files.createTempDirectory("nodus-feed-bench-");
            kernel = DurableGraph.open(directory, LogConfig.DEFAULT).kernel();
        } else {
            kernel = new GraphKernel();
            kernel.enableChanges(ChangeRing.DEFAULT_CAPACITY);
        }
        cursor = kernel.token();
        for (int i = 0; i < WRITES; i++) {
            kernel.addEdge(i, i + 1);
        }
        if (directory != null) {
            kernel.sync();
        }
        feed = new ChangeFeed(kernel, kernel.history().recordSources(), ChangeRing.DEFAULT_CAPACITY);
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        feed.close();
        kernel.close();
        if (directory != null) {
            try (Stream<Path> files = Files.walk(directory)) {
                for (Path path : files.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    @Benchmark
    public ChangeBatch readAThousandChanges() {
        return feed.read(cursor, ChangeFeed.NO_BOUND, CHANGES_PER_READ, Duration.ZERO);
    }
}
