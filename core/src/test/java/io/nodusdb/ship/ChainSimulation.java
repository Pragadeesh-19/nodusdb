package io.nodusdb.ship;

import io.nodusdb.chain.ChainBody;
import io.nodusdb.chain.ChainCodec;

import io.nodusdb.chain.ChainHash;
import io.nodusdb.chain.ChainLayout;
import io.nodusdb.chain.ChainObject;
import io.nodusdb.chain.ChainVerifier;
import io.nodusdb.log.SyncMode;
import io.nodusdb.log.simulation.LogHarness;
import io.nodusdb.objectstore.FaultyObjectStore;
import io.nodusdb.objectstore.MemoryObjectStore;
import io.nodusdb.objectstore.ObjectStoreException;
import io.nodusdb.replica.AntiRollbackMarker;
import io.nodusdb.replica.FollowerConfig;
import io.nodusdb.replica.FollowerCore;
import io.nodusdb.replica.FollowerState;
import io.nodusdb.replica.FollowerState.Phase;
import io.nodusdb.ship.ShipperRig.Session;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

final class ChainSimulation implements AutoCloseable {

    private static final long DAY_MILLIS = 86_400_000L;
    private static final long STEP_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
    private static final String PROBE_PREFIX = "_nodus/probe/";
    private static final int FOLLOWERS = 3;
    private static final int SETTLE_STEPS = 4_000;
    private static final double FAULT_RATE = 0.04;

    private final class Follower {

        final int id;
        final Path stateDirectory;
        final Path scratch;
        final FaultyObjectStore store = new FaultyObjectStore(rig.memory);
        SimSink sink;
        FollowerState state;
        FollowerCore core;
        long epoch;
        long lsn;
        long seq;

        Follower(int id) throws IOException {
            this.id = id;
            this.stateDirectory = Files.createDirectories(directory.resolve("follower-" + id));
            this.scratch = Files.createDirectories(directory.resolve("scratch-" + id));
            restart();
        }

        void restart() throws IOException {
            sink = new SimSink();
            state = new FollowerState();
            AntiRollbackMarker marker = AntiRollbackMarker.in(stateDirectory);
            FollowerConfig.Follow follow = new FollowerConfig.Follow(Duration.ofMillis(100), Duration.ofSeconds(1),
                    null, 2, null);
            core = new FollowerCore(store, ChainBuilder.keyring(), sink, marker, state, follow, scratch,
                    nanos::get);
            epoch = 0;
            lsn = 0;
            seq = 0;
        }
    }

    private final long seed;
    private final Random random;
    private final Path directory;
    private final AtomicLong storeMillis = new AtomicLong(1_000 * DAY_MILLIS);
    private final AtomicLong nanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    private final ShipperRig rig;
    private final List<Follower> followers = new ArrayList<>();
    private final TreeMap<Long, List<Long>> committed = new TreeMap<>();
    private final ChainRetention retention;
    private Session session;
    private int nonce;
    private int nextObject = 1;
    private long maxShipped;
    private long steps;
    private long fenceSeq = -1;
    private long fenceEpoch = -1;
    private boolean faultsOn;

    ChainSimulation(long seed, Path directory) throws IOException {
        this.seed = seed;
        this.random = new Random(seed);
        this.directory = directory;
        ShipSettings settings = ShipSettings.defaults().withRetention(Duration.ofDays(7));
        this.rig = new ShipperRig(new MemoryObjectStore(storeMillis::get), settings, SyncMode.SYNC);
        this.retention = new ChainRetention(rig.store, Duration.ofDays(7), storeMillis::get);
        this.rig.snapshotContent = this::render;
        this.session = rig.open(++nonce);
        for (int id = 0; id < FOLLOWERS; id++) {
            followers.add(new Follower(id));
        }
    }

    void run(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            steps++;
            nanos.addAndGet(STEP_NANOS);
            act();
        }
    }

    void settleAndVerify() throws IOException {
        disableFaults();
        ensureSession();
        for (int i = 0; i < SETTLE_STEPS && !writerSettled(); i++) {
            nanos.addAndGet(STEP_NANOS);
            stepWriter();
            ensureSession();
        }
        require(writerSettled(), "the writer never shipped its whole log: " + describeWriter());
        verifyChain();
        for (Follower follower : followers) {
            settle(follower);
            verifyConverged(follower);
        }
        require(maxShipped <= rig.log.lastLsn(), "a shipped LSN exceeds the log");
        verifyFencing();
    }

    @Override
    public void close() {
        if (session != null) {
            closeFeed(session);
        }
        rig.close();
    }

    private void act() throws IOException {
        int roll = random.nextInt(100);
        if (roll < 28) {
            appendTransaction();
        } else if (roll < 52) {
            stepWriter();
        } else if (roll < 82) {
            stepFollower(followers.get(random.nextInt(followers.size())));
        } else if (roll < 86) {
            publishSnapshot();
        } else if (roll < 89) {
            sweepRetention();
        } else if (roll < 92) {
            restartWriter();
        } else if (roll < 95) {
            restartFollower(followers.get(random.nextInt(followers.size())));
        } else if (roll < 98) {
            toggleFaults();
        } else {
            fenceWriter();
        }
    }

    private void appendTransaction() {
        int count = 1 + random.nextInt(4);
        int first = nextObject;
        nextObject += count;
        long lsn = rig.log.append(LogHarness.tuples(first, count));
        rig.log.awaitDurable(lsn);
        List<Long> edges = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            edges.add(SimSink.edge(first + i, first + i + 1));
        }
        committed.put(lsn, edges);
    }

    private void stepWriter() {
        if (session == null) {
            ensureSessionQuietly();
            return;
        }
        try {
            ShipperCore.Next next = session.core().step();
            maxShipped = Math.max(maxShipped, session.state().shippedLsn());
            if (next.cadence() == ShipperCore.Cadence.STOP || session.state().terminal()) {
                closeFeed(session);
                session = null;
            }
        } catch (ObjectStoreException | UncheckedIOException interruption) {
            return;
        }
    }

    private void ensureSession() {
        while (session == null) {
            try {
                session = rig.open(++nonce);
            } catch (ObjectStoreException | UncheckedIOException interruption) {
                require(faultsOn, "the writer could not open without faults: " + interruption);
            }
        }
    }

    private void ensureSessionQuietly() {
        try {
            session = rig.open(++nonce);
        } catch (ObjectStoreException | UncheckedIOException interruption) {
            session = null;
        }
    }

    private void restartWriter() {
        if (session != null) {
            closeFeed(session);
            session = null;
        }
        ensureSessionQuietly();
    }

    private void fenceWriter() {
        if (fenceSeq >= 0 || session == null) {
            return;
        }
        EpochClaims intruder = new EpochClaims(rig.memory, 0xF00D, 9, () -> 0);
        long claimed = intruder.claim(intruder.highestClaimed() + 1, 64);
        fenceSeq = ChainAudit.chainKeys(rig.memory).size();
        fenceEpoch = claimed;
    }

    private void publishSnapshot() {
        if (session == null || committed.isEmpty()) {
            return;
        }
        long lsn = committed.lastKey();
        byte[] content = render(lsn).getBytes(StandardCharsets.UTF_8);
        String key = ChainLayout.snapshotKey(lsn);
        long floor = Math.max(1, session.state().snapshot().chainSeq());
        rig.memory.putIfAbsent(key, content, Map.of(ChainHead.FLOOR_METADATA, String.valueOf(floor)));
        session.state().offerReference(new ChainBody.SnapshotRef(key, ChainHash.sha256(content), lsn));
    }

    private void sweepRetention() {
        storeMillis.addAndGet((1 + random.nextInt(9)) * DAY_MILLIS);
        if (session == null) {
            return;
        }
        ShipState.ReferenceStatus newest = session.state().snapshot().references();
        if (newest.seq() == 0) {
            return;
        }
        try {
            retention.sweep(new ChainRetention.Reference(newest.seq(), newest.lsn()), ChainRetention.NO_PROJECTOR);
        } catch (ObjectStoreException interruption) {
            return;
        }
    }

    private void toggleFaults() {
        if (faultsOn) {
            disableFaults();
            return;
        }
        faultsOn = true;
        rig.store.randomFaults(seed ^ steps, FAULT_RATE, FAULT_RATE, FAULT_RATE).exemptFromRandomConflicts(PROBE_PREFIX);
        for (Follower follower : followers) {
            follower.store.randomFaults(seed + 31L * follower.id + steps, FAULT_RATE, FAULT_RATE, 0);
        }
    }

    private void disableFaults() {
        faultsOn = false;
        rig.store.stopRandomFaults();
        for (Follower follower : followers) {
            follower.store.stopRandomFaults();
        }
    }

    private void restartFollower(Follower follower) throws IOException {
        follower.restart();
    }

    private void stepFollower(Follower follower) {
        follower.core.step();
        checkFollower(follower);
    }

    private void checkFollower(Follower follower) {
        FollowerState.Snapshot snapshot = follower.state.snapshot(nanos.get());
        require(snapshot.epoch() >= follower.epoch && snapshot.appliedLsn() >= follower.lsn
                && snapshot.chainSeq() >= follower.seq, "I2: follower " + follower.id + " moved backwards from ("
                + follower.epoch + ", " + follower.lsn + ", " + follower.seq + ") to " + snapshot);
        follower.epoch = snapshot.epoch();
        follower.lsn = snapshot.appliedLsn();
        follower.seq = snapshot.chainSeq();
        if (follower.sink.loaded()) {
            long applied = follower.sink.appliedLsn();
            require(stateAt(applied).equals(follower.sink.edges()), "I1: follower " + follower.id
                    + " holds a state that is not the log's prefix at LSN " + applied);
        }
        if (snapshot.phase() == Phase.STALLED) {
            requireLegitimateStall(follower, snapshot);
        }
    }

    private void requireLegitimateStall(Follower follower, FollowerState.Snapshot snapshot) {
        require(snapshot.stallReason().contains("fell behind retention"), "follower " + follower.id
                + " stalled for a reason the simulation never causes: " + snapshot.stallReason());
        long missing = snapshot.chainSeq() + 1;
        require(!rig.memory.exists(ChainLayout.chainKey(missing)), "follower " + follower.id
                + " reports a retention gap but object " + missing + " exists");
        require(rig.memory.list(ChainLayout.CHAIN_PREFIX, ChainLayout.chainKey(missing), 1).entries().size() > 0,
                "follower " + follower.id + " reports a retention gap but no later object exists");
    }

    private void settle(Follower follower) {
        for (int i = 0; i < SETTLE_STEPS; i++) {
            nanos.addAndGet(STEP_NANOS);
            follower.core.step();
            checkFollower(follower);
            Phase phase = follower.state.phase();
            if (phase == Phase.STALLED || (phase == Phase.CURRENT && follower.sink.appliedLsn() == lastChainLsn())) {
                return;
            }
        }
    }

    private void verifyConverged(Follower follower) {
        Phase phase = follower.state.phase();
        if (phase == Phase.STALLED) {
            return;
        }
        require(phase == Phase.CURRENT, "follower " + follower.id + " never became current: "
                + follower.state.snapshot(nanos.get()));
        require(follower.sink.appliedLsn() == lastChainLsn(), "follower " + follower.id + " is at LSN "
                + follower.sink.appliedLsn() + " but the chain ends at LSN " + lastChainLsn());
        require(stateAt(lastChainLsn()).equals(follower.sink.edges()), "I1: follower " + follower.id
                + " differs from the log at the end");
    }

    private void verifyChain() {
        List<String> keys = ChainAudit.chainKeys(rig.memory);
        require(!keys.isEmpty(), "the chain is empty");
        long first = ChainLayout.chainSeq(keys.get(0)).orElseThrow();
        ChainVerifier verifier = new ChainVerifier(ChainBuilder.keyring());
        ChainHash previous = null;
        long epoch = 0;
        long lastRecordsLsn = -1;
        long lastLsn = 0;
        for (int i = 0; i < keys.size(); i++) {
            require(ChainLayout.chainSeq(keys.get(i)).orElseThrow() == first + i, "I3: the chain has a gap at "
                    + keys.get(i));
            ChainObject object = ChainCodec.decode(rig.memory.get(keys.get(i)).orElseThrow());
            verifier.verify(object);
            require(previous == null || object.header().prev().equals(previous), "I3: " + keys.get(i)
                    + " does not extend the object before it");
            require(object.epoch() >= epoch, "I3: " + keys.get(i) + " goes back in epoch");
            previous = object.digest();
            epoch = object.epoch();
            if (object.body() instanceof ChainBody.Records records) {
                require(lastRecordsLsn < 0 || records.lsnFirst() == lastRecordsLsn + 1, "I3: " + keys.get(i)
                        + " starts at LSN " + records.lsnFirst() + " after LSN " + lastRecordsLsn);
                lastRecordsLsn = records.lsnLast();
                lastLsn = Math.max(lastLsn, records.lsnLast());
            } else if (object.body() instanceof ChainBody.SnapshotRef reference) {
                lastLsn = Math.max(lastLsn, reference.lsn());
            }
        }
        require(lastLsn == rig.log.lastLsn(), "I5: the chain ends at LSN " + lastLsn + " but the log ends at "
                + rig.log.lastLsn());
    }

    private void verifyFencing() {
        if (fenceSeq < 0) {
            return;
        }
        int late = 0;
        List<String> keys = ChainAudit.chainKeys(rig.memory);
        for (String key : keys) {
            long seq = ChainLayout.chainSeq(key).orElseThrow();
            if (seq <= fenceSeq) {
                continue;
            }
            ChainObject object = ChainCodec.decode(rig.memory.get(key).orElseThrow());
            if (object.epoch() < fenceEpoch) {
                late++;
            }
        }
        require(late <= 1, "I4: " + late + " objects of an epoch below " + fenceEpoch + " were committed after "
                + "the claim of that epoch");
    }

    private boolean writerSettled() {
        return session != null && session.state().shippedLsn() == rig.log.lastLsn()
                && session.state().phase() == ShipState.Phase.ACTIVE;
    }

    private String describeWriter() {
        return session == null ? "no session" : session.state().snapshot().toString();
    }

    private long lastChainLsn() {
        return rig.log.lastLsn();
    }

    private Set<Long> stateAt(long lsn) {
        Set<Long> edges = new TreeSet<>();
        for (Map.Entry<Long, List<Long>> entry : committed.headMap(lsn, true).entrySet()) {
            edges.addAll(entry.getValue());
        }
        return edges;
    }

    private String render(long lsn) {
        StringBuilder text = new StringBuilder("state ").append(lsn).append('\n');
        for (long edge : stateAt(lsn)) {
            text.append(edge).append('\n');
        }
        return text.toString();
    }

    private void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("seed=" + seed + " step=" + steps + ": " + message);
        }
    }

    private static void closeFeed(Session session) {
        try {
            session.feed().close();
        } catch (IOException ignored) {
            return;
        }
    }

}
