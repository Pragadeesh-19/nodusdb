package io.nodusdb.ship;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.LongStream;

class ChainSimulationTest {

    private static final int SEEDS = Integer.getInteger("nodus.sim.seeds", 60);
    private static final int STEPS = Integer.getInteger("nodus.sim.steps", 500);
    private static final String SINGLE_SEED = "nodus.sim.seed";

    @TempDir
    Path directory;

    static LongStream seeds() {
        String single = System.getProperty(SINGLE_SEED);
        if (single != null) {
            return LongStream.of(Long.parseLong(single));
        }
        return LongStream.rangeClosed(1, SEEDS);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void followersAndTheWriterKeepEveryInvariantAcrossFaultsRestartsRetentionAndFencing(long seed)
            throws IOException {
        try (ChainSimulation simulation = new ChainSimulation(seed, directory)) {
            simulation.run(STEPS);
            simulation.settleAndVerify();
        }
    }
}
