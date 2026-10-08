package io.nodusdb.projection;

import io.nodusdb.chain.ChainObject;
import io.nodusdb.log.record.RecordType;
import io.nodusdb.objectstore.FaultyObjectStore.Fault;
import io.nodusdb.objectstore.FaultyObjectStore.Operation;
import io.nodusdb.ship.ChainBuilder;
import io.nodusdb.ship.ShipState.ProjectionPhase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProjectorRunnerTest {

    private static final long SECOND = 1_000_000_000L;
    private static final long T0 = ProjectionRig.DAY_ZERO + 3_600_000_000L;
    private static final ProjectionSettings FAST = ProjectionSettings.defaults()
            .withRetries(Duration.ofMillis(10), Duration.ofMillis(40), Duration.ofMillis(40));

    @TempDir
    Path root;

    private ProjectionRig rig;
    private ProjectorRunner runner;

    @AfterEach
    void tearDown() {
        if (runner != null) {
            runner.close();
        }
    }

    private void open(ProjectionSettings settings) {
        rig = new ProjectionRig(root, settings);
        rig.names.symbol(0, "document:readme").symbol(1, "user:alice").relation(1, "viewer");
    }

    private ProjectorRunner run(EdgeLogProjector projector, ProjectionSettings settings) {
        runner = ProjectorRunner.start(projector, rig.state, settings, "test-projector");
        return runner;
    }

    private void ship(long micros) {
        rig.ship(rig.script.transaction(micros, b -> b.tuple(RecordType.TUPLE_ADD, 0, 1, 0, 1)));
    }

    private static void waitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + 20 * SECOND;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertTrue(condition.getAsBoolean(), "the condition never became true");
    }

    private ProjectionPhase phase() {
        return rig.state.snapshot().projection().phase();
    }

    @Test
    void theProjectorCommitsInTheBackgroundOnceTheIntervalHasElapsed() {
        open(FAST);
        run(rig.projector(), FAST);
        waitUntil(() -> phase() == ProjectionPhase.ACTIVE);

        ship(T0);
        rig.advance(61);

        waitUntil(() -> rig.state.snapshot().projection().commits() == 1);
        assertEquals(1, rig.rows().size());
    }

    @Test
    void closingCommitsWhatIsStillPending() {
        open(FAST);
        run(rig.projector(), FAST);
        waitUntil(() -> phase() == ProjectionPhase.ACTIVE);
        ship(T0);
        ship(T0 + 1);

        runner.close();

        assertEquals(2, rig.rows().size());
        assertEquals(ProjectionPhase.CLOSED, phase());
        assertEquals(1, rig.table.load().orElseThrow().version());
    }

    @Test
    void closingAnIdleProjectorIsPrompt() {
        ProjectionSettings slow = FAST.withCommitInterval(Duration.ofHours(1));
        open(slow);
        run(rig.projector(), slow);
        waitUntil(() -> phase() == ProjectionPhase.ACTIVE);

        long started = System.nanoTime();
        runner.close();

        assertTrue(System.nanoTime() - started < 5 * SECOND);
        assertEquals(ProjectionPhase.CLOSED, phase());
    }

    @Test
    void closingDoesNotSitOutALongFailureBackoff() {
        ProjectionSettings patient = FAST.withRetries(Duration.ofMillis(10), Duration.ofMillis(40),
                Duration.ofSeconds(60));
        open(patient);
        rig.faulty.failNext(Operation.PUT_FILE, 1_000, Fault.FATAL);
        run(rig.projector(), patient);
        waitUntil(() -> phase() == ProjectionPhase.ACTIVE);
        ship(T0);
        rig.advance(61);
        waitUntil(() -> phase() == ProjectionPhase.FAILED);

        long started = System.nanoTime();
        runner.close();

        assertTrue(System.nanoTime() - started < 5 * SECOND);
        assertEquals(ProjectionPhase.CLOSED, phase());
    }

    @Test
    void aFencedProjectorStopsItsThreadAndKeepsItsPhaseUntilItIsClosed() {
        open(FAST);
        EdgeLogProjector other = rig.projector();
        run(rig.projector(), FAST);
        waitUntil(() -> phase() == ProjectionPhase.ACTIVE);
        ship(T0);
        other.drain();

        rig.advance(61);

        waitUntil(() -> phase() == ProjectionPhase.FENCED);
        assertEquals(1, rig.table.load().orElseThrow().version());
        runner.close();
        assertEquals(ProjectionPhase.CLOSED, phase());
    }

    @Test
    void anUnexpectedExceptionFailsTheProjectorInsteadOfKillingItAndItRecovers() {
        open(FAST);
        AtomicBoolean explode = new AtomicBoolean();
        StoreChainSource real = new StoreChainSource(rig.state.ring(), rig.faulty, ChainBuilder.keyring());
        ChainSource exploding = new ChainSource() {
            @Override
            public Optional<ChainObject> fetch(long seq) {
                if (explode.get()) {
                    throw new UnsupportedOperationException("simulated defect");
                }
                return real.fetch(seq);
            }

            @Override
            public OptionalLong oldestSeq() {
                return real.oldestSeq();
            }
        };
        run(rig.projector(exploding), FAST);
        waitUntil(() -> phase() == ProjectionPhase.ACTIVE);
        explode.set(true);
        ship(T0);

        waitUntil(() -> phase() == ProjectionPhase.FAILED);
        assertTrue(rig.state.snapshot().projection().lastError().contains("unexpected failure"));
        explode.set(false);
        rig.advance(61);

        waitUntil(() -> rig.state.snapshot().projection().commits() == 1);
        assertEquals(1, rig.rows().size());
    }

    @Test
    void closeCanBeCalledTwice() {
        open(FAST);
        run(rig.projector(), FAST);

        runner.close();
        runner.close();

        assertEquals(ProjectionPhase.CLOSED, phase());
    }
}
