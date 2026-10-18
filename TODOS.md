# TODOS

## Authorization

### Decision log for permission checks

**What:** Record each check and its answer in a sampled, append-only decisions table written through Spillway's append mode.

**Why:** Auditors and incident responders ask "who was actually allowed in", and the edge log only answers "who could have been".

**Context:** Deferred in the 2026-10-06 CEO review (issue 9B) until a first audit customer sets sampling and retention needs. It reuses the append-only table mode, signed commits and the edge-log shipper from the relationship-lakehouse plan. The check path must stay near 40 us, so writes go through a lock-free ring drained off the hot path.

**Effort:** M
**Priority:** P2
**Depends on:** Spillway append-only mode, edge-log shipper, signed commits

### Iceberg REST catalog commit target

**What:** A second commit implementation that registers and commits the edge log through the Iceberg REST protocol (Polaris, Unity, Glue), beside the S3 compare-and-swap committer.

**Why:** Enterprise data platforms discover and govern tables through their catalog; without this, the edge log has to be registered by hand.

**Context:** Alternative 3B in the 2026-10-06 CEO review. The default stays zero-infrastructure (S3 conditional writes plus a writer epoch). The ObjectStore and commit interfaces from issue 11A make this pluggable.

**Effort:** M
**Priority:** P2
**Depends on:** S3 CAS committer, ObjectStore interface

### Intersection and exclusion in the schema language

**What:** Schema version 2 adds `&` (intersection) and `-` (exclusion) to permissions, with evaluator and Leopard support.

**Why:** Some enterprise models need "must be an employee and on the project" or "everyone except blocked users".

**Context:** D60 reserves the syntax and schema version 1 rejects it, so this needs no log format change. Exclusion can't be precomputed safely, so a check must still distinguish "denied because excluded" from "gave up" (D76). See `docs/designs/engineering-review-authz-wedge.md` section 5.

**Effort:** L
**Priority:** P2
**Depends on:** D76 check evaluator (Build Step 1), Leopard (Build Step 4)

### Move tuples between table pairs when a schema retypes a relation

**What:** When a new schema version turns a relation that holds tuples into a tupleset relation, or the reverse, move its tuples between the DIRECT and INDIRECT pairs inside the same transaction as the `SCHEMA` record. Let the same transaction retire a relation by removing its tuples first.

**Why:** Build Step 1 refuses both changes while the relation has stored tuples (the review's section 3.2 and D81 describe the move). Operators then have to remove the tuples and write them again.

**Context:** `SchemaGuard.requireApplicable` refuses the change at write time and at replay. The move needs the kernel to apply a `SCHEMA` record, then the removals and additions it implies, as one unit, with headroom counted for both pairs.

**Effort:** M
**Priority:** P2
**Depends on:** Build Step 1

### Check the native performance gate against a recorded toolchain baseline

**What:** Record the native-to-JVM ratios that CI measures on GraalVM 25, then lower the default limit of `python/benchmarks/native_ratio.py` from 50 to about twice the recorded ratios.

**Why:** The review proposed a limit of 5. The same code measured 9.5 for `add_edge` and 16.9 for `has_edge` on a laptop with GraalVM CE 22.0.2, because ahead-of-time code is slower than JIT code for off-heap access. A limit of 50 passes that build and still fails the CE 23 library, which was about 130 times slower than CE 22.

**Context:** Decision note I12 in `docs/decisions.md`. The first CI run on this branch gives the GraalVM 25 numbers.

**Effort:** S
**Priority:** P2
**Depends on:** The first CI run of Build Step 1

### Lookup APIs and permission-level watch

**What:** `LookupResources` ("what can alice view") and `LookupSubjects` ("who can view this document"), then standing lookups the engine keeps current from the change feed and streams as gained and lost members.

**Why:** `AccessWatch` re-checks triples the application registers (D188). An application that needs every document a user lost access to cannot register every triple in advance.

**Context:** Reverse evaluation through usersets and tuple-to-userset arrows, with pagination, and dependency tracking through nesting for the incremental part. The reachability closure in `kernel/closure` already answers member-to-group sets, so `LookupResources` can start from `MEMBER2GROUP` plus the INDIRECT scan. SpiceDB and OpenFGA (ListObjects) offer the plain lookups.

**Effort:** XL
**Priority:** P2
**Depends on:** Build Step 6

### Agent-memory Python API

**What:** The Python API for AI agent memory with provenance, accepted in the CEO plan (D3.5, scope row 5, build order item 7).

**Why:** It is the second market the CEO plan names, and no build step schedules it, so it would drop after Build Step 6.

**Context:** Needs a short product review of the API shape and a typed memory schema before code. It uses tuples for memories, the change feed for updates and `as_of` for provenance. See `docs/designs/relationship-lakehouse.md`.

**Effort:** M
**Priority:** P2
**Depends on:** Build Step 6

## Engine

### Low-latency writer-to-follower tail stream

**What:** An optional TCP stream of committed records from the writer to followers, verified against the signed lake chain.

**Why:** S3 replication gives about 200 to 500 ms visibility (D66); some customers will need near-instant follower reads.

**Context:** Tokens and dense LSNs already support it. It adds a network surface on the writer (auth, backpressure), which is why it waits for demand. S3 stays the durable and trusted path.

**Effort:** M
**Priority:** P3
**Depends on:** Build Step 3

### Automatic failover with leases

**What:** A writer lease object in S3, renewed by the writer, with automatic takeover after expiry and epoch fencing.

**Why:** Hands-off recovery for teams without on-call, instead of the manual audited takeover (D65).

**Context:** Needs bounded clock skew, and still loses unshipped `LOCAL` writes unless callers use `LAKE` durability. The epoch and CAS design in section 8 of the design doc is lease-ready.

**Effort:** M
**Priority:** P3
**Depends on:** Build Step 3, the deterministic simulation harness

### More than one writer through namespace sharding

**What:** One writer and one signed edge log per namespace (tenant or resource type), with cross-namespace checks routed or federated.

**Why:** A single writer tops out around 80,000 mixed changes per second; namespace sharding lifts that without consensus and keeps every per-log guarantee.

**Context:** Multi-writer support is out of scope in the current design. Epochs, signatures and LSNs stay per log. The hard part is group nesting across namespaces, since the reachability index is per kernel.

**Effort:** XL
**Priority:** P3
**Depends on:** The full relationship-lakehouse plan shipping first

### Persist the reachability closure in the snapshot

**What:** Write the `CLOSURE` table pair as a snapshot section, so an open or a follower bootstrap starts with a clean index instead of a background full pass.

**Why:** After a restart, checks through nested groups use the evaluator, with its depth limit, until the rebuild finishes. On a large org chart that is a window of slow checks after every deploy.

**Context:** Build Step 4 keeps the index derived and outside every format (D139, D145). Snapshot v3 is frozen (D77), so this is a new section and a version bump with its own review. Measure the rebuild time on a real org chart first; if it is a few seconds, this may not be worth a format change. Start in `kernel/closure/ClosureIndex.contentReplaced`, which today starts the full pass.

**Effort:** M
**Priority:** P3
**Depends on:** Build Step 4

### Keep each symbol's erase LSN across restarts

**What:** Store the LSN at which each symbol was erased, so a restarted writer knows it exactly instead of only the LSN of the snapshot it loaded.

**Why:** The lake rewrite leaves the rows of a name written again after its erasure alone by comparing row LSNs with the erase LSN (D183). After a restart, a name that was erased and written again before the loaded snapshot can have its later rows rewritten with the old pseudonym.

**Context:** The kernel keeps the LSNs in memory (`ErasedSymbols`). Snapshot v3 is frozen (D77): the `SYMBOLS` section has a flag byte but no LSN, so this needs a new section or a version bump with its own review. The case needs an erased name to be reused, which opaque ids avoid.

**Effort:** S
**Priority:** P3
**Depends on:** Build Step 5

### Checkpoints that do not pause writes

**What:** Write the snapshot from a consistent read view on a background thread while the writer continues, instead of holding the writer for the whole snapshot.

**Why:** A checkpoint pauses writes for 3.7 to 5.6 seconds at 30.6 million edges, so checkpoints stay manual (D202). With checkpoints that do not pause, nodus could checkpoint on a schedule and bound the replay of every `as_of` build, the growth of the local log and the time of a recovery.

**Context:** Needs a copy-on-write or versioned read of the adjacency tables while the snapshot streams, which touches the seqlock design (D48, D86), so it needs its own review. Start at `GraphKernel.checkpointLocked` and `DurableStore.checkpoint`; the snapshot writer already streams sections. The `as_of` stats (replayed objects and records, build time) measure the gain.

**Effort:** L
**Priority:** P2
**Depends on:** Build Step 6

## Shipping and lake

### as_of beyond the bucket from the Iceberg table

**What:** Build a past view from the rows of `nodus_log` when the target is older than the bucket's reach or before the newest erasure.

**Why:** `as_of` reads only the bucket (D192), whose history an erasure cut deletes (D166), so a graph with frequent erasures has time travel only back to its last erasure. The table keeps every row, with erased names as pseudonyms, so the Step 5 guarantee still holds.

**Context:** Only valid when the table starts at LSN 1: `EdgeLogProjector.begin` starts at the oldest retained chain object, so a table enabled late is partial. Record the first projected LSN as a table property first. The build replays rows by name into a replica kernel; it scans the whole table, and rows are not signed per object the way chain objects are.

**Effort:** M
**Priority:** P3
**Depends on:** Build Step 6

### Snapshot-backed NameResolver for a projector outside the writer

**What:** A second `NameResolver` that reads the symbol and relation sections of the latest snapshot, plus later `SYMBOL` and `SCHEMA` records, instead of the writer's live tables.

**Why:** Build Step 2 builds the Iceberg table inside the writer process (D99). This lets the projector run on another machine and keeps the table buildable from the bucket alone.

**Context:** The `NameResolver` interface lands in Build Step 2, so this is a drop-in implementation. Snapshot v3 stores symbols with erased flags (D77). The cost is a ranged read of a large snapshot. Such a projector also runs the lake erasure job, which needs the salt and each erased symbol's erase LSN (D183); both would come from the snapshot and the `ERASE` records after it.

**Effort:** M
**Priority:** P3
**Depends on:** Build Step 2, the snapshot v3 section directory

### Cloud role credential providers

**What:** `CredentialsProvider` implementations for EC2 IMDSv2, ECS container credentials and STS web identity (EKS IRSA), with refresh before expiry.

**Why:** Teams on AWS roles have no static keys. Without these providers they need a sidecar that writes a credentials file (D95).

**Context:** Build Step 2 ships the provider interface and the refresh on `ExpiredToken`. Each provider is a drop-in. The flows cannot be tested end to end in CI without cloud accounts, so they need careful fakes.

**Effort:** M
**Priority:** P2
**Depends on:** Build Step 2 (S3 client and `CredentialsProvider`)

### Prometheus and OpenTelemetry adapter for stats

**What:** An optional Python module that exports the `Graph.stats()` dict as Prometheus metrics and OpenTelemetry gauges, with no required dependency.

**Why:** A stuck shipper should page someone. The CEO plan (15A) promised this adapter, and D96 does not include it.

**Context:** The stats fields are stable from Build Step 2 and only grow. The adapter maps them to metrics; metric names become a contract, so pick them with a real alert in mind.

**Effort:** S
**Priority:** P2
**Depends on:** Build Step 2 (stats export)

### Signing key rotation and keyring tooling

**What:** A tool and Python helpers to generate a key, publish a keyring file, verify a keyring against a chain range and stage a rotation.

**Why:** A rotation done wrong stalls every follower at once. D93 makes trust operator-supplied, and nothing yet helps an operator rotate safely.

**Context:** Build Step 2 ships `generate_signing_key`, an explicit `key_id` in every chain object and the writer key id in every `EPOCH` record. The keyring file format is best designed with the follower verifier in Build Step 3.

**Effort:** M
**Priority:** P2
**Depends on:** Build Steps 2 and 3

### Merge Iceberg manifests

**What:** Merge the small manifests the projector writes (one per commit) into larger ones, and drop the manifests of expired snapshots.

**Why:** Each commit adds a manifest and every manifest list names all of them, so a table committed every minute grows its list by about 1,400 entries a day. Reads and commits slow down as the list grows.

**Context:** Build Step 2 writes one manifest per commit and carries all earlier ones forward. Snapshot expiry bounds the metadata file but not the manifest list. Build Step 5 added `ManifestReader` and existing and deleted entries to `ManifestWriter`, so a merge rewrites manifests the way the erasure overwrite already does.

**Effort:** M
**Priority:** P2
**Depends on:** Build Step 2

### Orphan data files by manifest reference

**What:** Find and delete data files that no manifest references, by reading the manifests instead of comparing LSN ranges.

**Why:** The LSN rule removes files from a crashed commit but not a stray copy left when an upload failed and its cleanup delete also failed.

**Context:** The data-file writer deletes a failed upload itself, so a leak needs two failures in a row. A reference-based sweep can use `IcebergTable.manifests` and `IcebergTable.entries` from Build Step 5.

**Effort:** S
**Priority:** P3
**Depends on:** Build Step 5

### Start a new chain after an unshipped gap

**What:** A deliberate operation that ends the old chain at its head and starts a new one from a fresh snapshot, for a directory that wrote while shipping was off.

**Why:** Such a directory is refused at open today (`NodusWriterFencedError`, "never shipped"). The only ways out are another bucket prefix or deleting the chain.

**Context:** The chain's epoch claims and `SNAPSHOT_REF` objects already express "history restarts here". The risk is a verifier that trusts the old chain's end as the whole history, so the break needs a signed marker.

**Effort:** M
**Priority:** P2
**Depends on:** Restore a graph from the bucket

### Set the `lake` wait through the C interface

**What:** An argument or a setting for the wait that `nodus_tuple_write` does for durability `lake`. It is 30 seconds now.

**Why:** A C caller that wants a shorter bound has to write locally and call `nodus_await_shipped` itself.

**Context:** Python already passes `timeout=`. Changing the signature of `nodus_tuple_write` breaks the ABI, so a new export is the likelier shape.

**Effort:** S
**Priority:** P3
**Depends on:** none

### Live rebuild of a follower that fell behind retention

**What:** Let a follower bootstrap a second graph in the background and swap it in when it falls behind retention, instead of stopping in STALLED (D116 chose stop and restart).

**Why:** Removes the last case where an unattended follower needs a restart; matters for fleets without a supervisor.

**Context:** `GraphSession` keeps `kernel`, `tuples` and `strings` as final fields on purpose, so a swap needs a holder indirection through about 25 methods, and peak memory is about double the graph. The detection already exists after Step 3 (STALLED reason "fell behind retention"); this is only the remedy. Start in `replica/FollowerCore` where the gap is detected.

**Effort:** M
**Priority:** P3
**Depends on:** Build Step 3

### Local follower checkpoint for fast restarts

**What:** Persist a follower's own snapshot so a restart loads from local disk and tails the chain, instead of downloading the newest snapshot (D109 chose a rollback marker only).

**Why:** A restart of a multi-gigabyte follower costs the download (about 10 to 15 s with parallel ranges) plus the 2 to 4 s load; a fleet restart multiplies it.

**Context:** The marker already stores `(seq, digest, epoch, lsn)`, the resume point a checkpoint needs. `SnapshotWriter` and `SnapshotReader` exist, so the work is the lifecycle (when to checkpoint, how to trust the file after a crash), not the format. It adds a second durable format for a cache that is always rebuildable.

**Effort:** M
**Priority:** P3
**Depends on:** Build Step 3

### Adaptive polling for large follower fleets

**What:** Back off the poll interval while the chain is idle (for example 100 ms up to 2 s) and snap back when an object appears.

**Why:** At the default 100 ms each follower makes 10 GET requests a second. 500 followers make 5,000 a second, near S3's 5,500 GET/s per prefix limit, and about 430 million 404s a day on an idle writer.

**Context:** The frozen visibility budget assumes a 100 ms poll (D66), so this trades first-write latency after an idle period for request cost. Tune it against real fleet traffic. Start in `replica/FollowerCore` where the idle cadence is chosen.

**Effort:** S
**Priority:** P3
**Depends on:** Build Step 3

### Split ShipState

**What:** Split `ship/ShipState.java` (523 lines) into the shipped position and wait, the backlog gate, the snapshot-reference mailbox, retention and projection status, and the event log.

**Why:** It is the largest class in the shipping packages and a monitor shared by five threads.

**Context:** Step 3 extracts only the event log (D124) and gives the follower its own `FollowerState`. Do the rest one part per commit with the existing `ShipStateTest` and the shipper crash tests as the safety net, starting with the reference mailbox.

**Effort:** M
**Priority:** P3
**Depends on:** none

### Salvage a large directory without loading its snapshot into memory

**What:** Read only the symbol table, the schema and the epoch history of the old directory's snapshot when salvaging, instead of loading the whole graph into a scratch kernel.

**Why:** Salvage decodes names with the same loader recovery uses, so a 30 million edge directory needs about as much memory as opening it.

**Context:** `storage/GraphSurvey` loads `snapshot.bin` into a throwaway `GraphKernel` and replays the log copy on top of it. The loader's adjacency step is the part to skip; `SnapshotReader` already reads the sections one by one.

**Effort:** S
**Priority:** P3
**Depends on:** none

## Release and packaging

### Wheels for more platforms

**What:** Build and publish wheels for musllinux (Alpine), Linux with glibc older than 2.34, and Windows arm64.

**Why:** A user on Alpine images, RHEL 8, Ubuntu 20.04, Debian 11, Amazon Linux 2 or a Windows ARM laptop gets pip's bare "no matching distribution" and no source package to fall back on.

**Context:** D50 chose manylinux_2_34 on purpose and the README documents it. The matrix is in `.github/workflows/wheels.yml`. Start with manylinux_2_28 and Windows arm64, which have hosted runners; musl is the hard one because native-image needs a musl toolchain.

**Effort:** L
**Priority:** P3
**Depends on:** D224 and D225 (the toolchain and the py3-none tags, which the new wheels follow)

### A source package that explains an unsupported platform

**What:** Publish an sdist whose only job is to fail with a readable message ("no prebuilt library for this platform; supported platforms are ...") when pip falls back to it.

**Why:** With wheels only, pip reports "Could not find a version that satisfies the requirement nodusdb" and the user cannot learn the cause.

**Context:** `CLAUDE.md` section 7 and D50 say wheels only; this would revise that. The sdist must never be chosen on a supported platform, where a wheel always wins.

**Effort:** S
**Priority:** P3
**Depends on:** the platform list above, and D227 (the page it points to)

### Pin the release workflow's actions

**What:** Pin every third-party action in `wheels.yml` by commit SHA and add `.github/dependabot.yml` for github-actions.

**Why:** The publish job holds `id-token: write`. `pypa/gh-action-pypi-publish@release/v1`, `graalvm/setup-graalvm@v1`, `ilammy/msvc-dev-cmd@v1` and `pypa/cibuildwheel@v2.23.4` can change underneath the workflow. The reviewer gate on the `pypi` environment limits the damage but does not read the action's code.

**Context:** Do it as its own change after 0.1.0 so a pin mistake is not mistaken for a release mistake. Without Dependabot the pins go stale.

**Effort:** S
**Priority:** P2
**Depends on:** D228

## Completed

### Watch and as_of

Done in Build Step 6. `changes(after)` and `watch()` read committed changes by token from a ring in memory, the writer's log and the bucket chain; `AccessWatch` reports gained and lost access for registered checks. `as_of(token or time)` opens a read-only past view rebuilt from the bucket, on writers and followers, never before the newest erasure. Decisions D187 to D222.

### Erasure

Done in Build Step 5. `erase(name)` scrubs memory and followers at once; the graph directory, the bucket (a signed `REDACTION` cut) and the Iceberg table (an overwrite and a full expiry) are scrubbed in the background, and `wait_erased(token)` waits for all of them. Decisions D159 to D186.

### Deterministic simulation harness for the distributed steps

Done in Build Step 3. `ChainSimulation` (`core/src/test/java/io/nodusdb/ship`) drives the real shipper and follower step functions over a faulty store with a virtual clock from a seed, and checks the prefix, monotone, no-fork, fencing and convergence invariants. CI runs 300 seeds, and `-Dnodus.sim.seed=N` replays one. `S3ChaosTest` repeats the worst network cases over HTTP through `FaultProxy`.

### Restore a graph from the bucket

Done in Build Step 3. `Restore` and `restore()` rebuild a graph directory from the newest verified snapshot and the chain after it, with `GraphInstaller` renaming a complete directory into place. `WriterTakeover` and `Graph(takeover=True)` build a new writer on top of it, and `Salvage` lists what an old writer never shipped.
