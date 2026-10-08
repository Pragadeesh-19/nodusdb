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

## Engine

### Deterministic simulation harness for the distributed steps

**What:** Run the writer, the lake shipper, a fake S3 and followers on a simulated clock, inject failures from a seed, and replay any failure exactly.

**Why:** Split-brain, lost-write and stale-read bugs live in rare interleavings of the commit protocol, followers and takeover; seeded simulation finds and reproduces them.

**Context:** Gate for Build Step 3, scheduled as milestone M5 in `docs/designs/engineering-review-build-step-3.md` (D117). The local every-prefix crash harness (D73) is the seed. The protocol to simulate is in `docs/designs/engineering-review-authz-wedge.md` sections 8 and 9.

**Effort:** L
**Priority:** P1
**Depends on:** Build Step 1 (LogStore I/O seam)

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

## Shipping and lake

### Snapshot-backed NameResolver for a projector outside the writer

**What:** A second `NameResolver` that reads the symbol and relation sections of the latest snapshot, plus later `SYMBOL` and `SCHEMA` records, instead of the writer's live tables.

**Why:** Build Step 2 builds the Iceberg table inside the writer process (D99). This lets the projector run on another machine and keeps the table buildable from the bucket alone.

**Context:** The `NameResolver` interface lands in Build Step 2, so this is a drop-in implementation. Snapshot v3 stores symbols with erased flags (D77). The cost is a ranged read of a large snapshot.

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

**Context:** Build Step 2 writes one manifest per commit and carries all earlier ones forward. Snapshot expiry bounds the metadata file but not the manifest list. A merge needs a manifest entry reader, which the Avro decoder now makes straightforward.

**Effort:** M
**Priority:** P2
**Depends on:** Build Step 2

### Orphan data files by manifest reference

**What:** Find and delete data files that no manifest references, by reading the manifests instead of comparing LSN ranges.

**Why:** The LSN rule removes files from a crashed commit but not a stray copy left when an upload failed and its cleanup delete also failed.

**Context:** The data-file writer deletes a failed upload itself, so a leak needs two failures in a row. A reference-based sweep needs the same manifest entry reader as the merge above.

**Effort:** S
**Priority:** P3
**Depends on:** Merge Iceberg manifests

### Restore a graph from the bucket

**What:** A tool that downloads the newest snapshot reference, verifies it and the chain after it, and rebuilds a graph directory.

**Why:** Shipping is one-way today. The chain and the snapshots hold everything needed, but only the projector reads them back.

**Context:** Scheduled as milestone M1 of Build Step 3 (`docs/designs/engineering-review-build-step-3.md`, D105 to D120). `ChainHead`, `ChainVerifier`, `ChainCursor` and the snapshot reader already do the hard parts. Open reconciliation refuses a directory that is behind the chain, so a restore also decides what happens to a stale directory.

**Effort:** M
**Priority:** P1
**Depends on:** Build Step 2

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

## Completed
