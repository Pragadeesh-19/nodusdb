# Engineering review: Build Step 3, followers, restore, takeover and the simulator

Status: decisions D105 to D126 accepted on 2026-10-08. Implementation scope is Build Step 3 (section 12).
Contracts: [`engineering-review-authz-wedge.md`](engineering-review-authz-wedge.md) sections 8 and 9, and
[`engineering-review-build-step-2.md`](engineering-review-build-step-2.md). Where this document and those disagree
about Build Step 3, this document wins (section 2 lists every amendment).

Step 2 made the writer copy its log to a bucket as a signed chain. Step 3 reads that chain back. A follower
rebuilds the graph from the newest snapshot and follows the chain. Restore does the same into a directory. Takeover
turns a restore into a new writer with a higher epoch. The deterministic simulator is the gate: it runs the real step
functions through thousands of seeded failure orders and checks six invariants after every step.

---

## 1. First principles

1. **The chain is the only truth.** A follower, a restored directory and a new writer are all functions of a
   verified prefix of the chain. Nothing else is trusted: not the bucket's listing order, not object timestamps, not
   a key found in the bucket.
2. **Verification is mandatory and local.** A follower holds only the public key from its own configuration. An object
   whose signer is unknown is an error, never a pass.
3. **Position is monotone.** A follower's applied `(epoch, lsn)` and its persisted chain `seq` never decrease. A head
   behind the persisted `seq` is a rollback and stops the follower.
4. **Atomicity is the transaction.** A reader sees whole transactions. The write section on a follower is one
   transaction, the same as on the writer.
5. **Fail visibly.** A follower that cannot continue says why and in which state. It never reports CURRENT while it
   is stuck.

```
 writer --ShipperCore--> _nodus/chain/{seq}.obj   (signed, linked)        _nodus/snapshots/{lsn}.nsnap
                                |                                                  |
                  +-------------+--------------+                                   |
                  |                            |                                   |
          FollowerCore.step             Restore (same replay)  <------------------ AnchorFinder
                  |                            |
          ReplicaWriter (per transaction)   GraphInstaller -> directory -> DurableGraph.open
                  |                                                       |
        replica GraphKernel  <- reads (check, khop, token)         Takeover = claim + restore + open
```

## 2. Amendments to the frozen design and to Step 2

| Where | Text before | Text now | Decision |
|---|---|---|---|
| Step 2 retention (`ChainRetention`) | keep the last records object before the newest reference | keep every object from the one holding LSN `L+1`, where `L` is the reference's snapshot LSN | D106 |
| Wedge 8.5 takeover step 3 | first commit retried until it lands, rebasing inside the shipper | claim, restore, open; retry the restore if the open finds the head moved | D110 |
| Wedge 8.4 step 1 | bootstrap from the newest `SNAPSHOT_REF` | the newest reference whose snapshot object and covering records exist, walking back to older references | D111 |
| Wedge 13 | codes -1 to -14 | adds -15 `NodusChainTrustError` | D121 |
| Wedge 9 | follower reads wait for a token | adds an optional staleness bound for reads without a token | D112 |

## 3. Decision log

| ID | Issue | Decision |
|---|---|---|
| D105 | Scope | All of Build Step 3 in five milestones on one branch, each mergeable and green: M1 foundations and restore, M2 follower core, M3 follower API, M4 takeover and salvage, M5 simulator, real-server chaos, docs and CI |
| D106 | Retention anchor | `ChainRetention` walks back from the newest reference to the object that holds LSN `L+1` and keeps everything from there. A regression test writes, takes a slow snapshot, writes more, ages the clock eight days, sweeps, then bootstraps a follower |
| D107 | Replica seam | `GraphKernel` gets an explicit role. A package-private `ReplicaWriter` does reserve, seqlock, apply and the applied-LSN update. Every write entry point is refused up front with `NodusUnsupportedError`. The epoch comes from the epoch history |
| D108 | Follower config | A shared `StoreConfig` (store, credentials, request timeout) is parsed once. `ShippingConfig` adds signing, ship and Iceberg. `FollowerConfig` adds a required public key and no private key. The split is a behavior-preserving first commit |
| D109 | Follower state | The graph is not persisted. A checksummed marker `(seq, digest, epoch, lsn)` is written atomically in an optional `state_directory`. Every start bootstraps from the newest verified reference |
| D110 | Takeover | Compose proven parts: claim the next epoch (fences the old writer), restore, open with shipping (the reconcile and the `EPOCH` handoff record come from the existing open), and retry the restore if the open reports the head moved. `ShipperCore` and `OpenReconciler` do not change |
| D111 | Gap detection | A follower GETs `seq+1` each poll. On a 404 it LISTs one key after its position at most once a second. A later key means the prefix was deleted |
| D112 | Stalled reads | A follower serves its last verified state by default. An optional `max_staleness_ms` makes reads raise `NodusStaleReadError` once the follower has not confirmed the head for that long. Checks with a token fail closed already |
| D113 | One verified fetch | `ChainFetch.fetch(store, seq, keyring, Trust)` with `WHEN_KEY_KNOWN` (writer, projector) and `REQUIRED` (follower, restore) replaces the copies in `ChainHead` and `StoreChainSource` |
| D114 | Schema refresh | `TupleStore.currentModel()` compares `kernel.catalog().version()` with the compiled model's version and rebuilds under a lock when they differ |
| D115 | API surface | `Graph(follow=Follower...)` in Python and one new export `nodus_open_follower`. Every other export works on a follower handle. `restore`, `takeover` and `salvage` are module functions |
| D116 | Fell behind retention | The follower goes STALLED with the reason "fell behind retention; restart to rebuild" and keeps serving under D112. A restart rebuilds from the newest reference. A live rebuild is a TODO |
| D117 | Simulator | A single-threaded scheduler with a virtual clock and one seeded `Random` picks the next actor and the next fault. The writer, the followers and the takeover are the real step functions. Failing seeds are pinned as tests |
| D118 | Real-server chaos | A `FaultProxy` in the test sources (JDK sockets) forwards to SeaweedFS and drops, truncates or delays the Nth response on command |
| D119 | Apply window | One seqlock section per transaction, with the applied LSN advanced inside it |
| D120 | Snapshot download | Up to four 8 MiB ranges in flight into a pre-sized temporary file, a size check against `HEAD` first, then one SHA-256 pass over the finished file compared with the signed reference |
| D121 | Trust errors | A signature, link, epoch, sequence or rollback violation raises `ChainTrustException`, which maps to the new code -15 and the Python class `NodusChainTrustError`. It is distinct from -1 so operators can alert on forgery |
| D122 | Damaged marker | A marker that fails its checksum, has an unknown version or is cut short stops the open with `NodusCorruptLogError` and names the file. The operator deletes it to accept the loss of rollback memory. It is never silently ignored |
| D123 | Reservation unit | Memory is reserved per transaction, inside that transaction's write section, not once per chain object. A limit hit leaves the follower at the last whole transaction and STALLED with the reason. This refines D119's text, and the end state is a valid prefix either way |
| D124 | Event log | The bounded, sequence-numbered event queue is extracted from `ShipState` into `EventLog` and shared with `FollowerState`. Nothing else in `ShipState` moves (D21 keeps the rest of the split a TODO) |
| D125 | Takeover shape | `Takeover` is a small orchestrator over three injected steps (claim, restore, open) so the simulator can substitute the open and unit tests can inject faults between steps |
| D126 | Read gate | The staleness bound and the token wait time live in `FollowerGate`, consulted by `GraphSession` in the one place every read already passes through (`requireOpen`). The token wait uses `read_wait_ms` from the config, so `nodus_check` does not change |

## 4. Bootstrap algorithm

Let a reference `R` be a `SNAPSHOT_REF` chain object at sequence `r` naming a snapshot at LSN `L`. The chain records
objects `k0 .. r-1` in front of it, and the object `k0` holds LSN `L+1` (`lsnFirst(k0) <= L+1 <= lsnLast(k0)`; a
checkpoint ends on a transaction boundary, so `L+1` begins a transaction).

```
find anchor:
  for each snapshot key in descending LSN (LIST _nodus/snapshots/):
      locate its reference object: scan chain objects from the floor hint (object metadata) upward for the
      SNAPSHOT_REF whose path equals the key; ChainFetch(REQUIRED) verifies its signature
      HEAD the snapshot: size must equal what the reference commits to when it is known
      walk back from r-1 to k0 by prev links: each fetched object must hash to the next object's prev
      if every object is present and the walk reaches lsnFirst <= L+1: this is the anchor
      else try the next older reference
  none found: BOOTSTRAPPING, waiting (an empty bucket is normal before the writer ships)
download: D120 ranges into a temporary file; SHA-256 equals the reference's hash
load:     SnapshotReader.load into a fresh replica kernel (bulk path, not yet shared)
apply:    objects k0 .. r-1, skipping records with LSN <= L; then the reference object itself (no records)
cursor:   ChainCursor.after(r, digest(R), epoch(R), lastLsn) and continue with r+1
```

Rollback check at bootstrap: `ChainHead` (REQUIRED trust) gives the head `h`. If a marker exists: `h.seq < marker.seq` is
a rollback, and `h.seq == marker.seq` with a different digest is a fork. Both raise `ChainTrustException` and the
follower is STALLED. While following, passing the marker's `seq` requires the same digest.

## 5. Follower state machine

```
                 anchor found, snapshot loaded
 BOOTSTRAPPING ------------------------------------> CATCHING_UP --(404 on seq+1)--> CURRENT
      ^   |                                              |   ^                         |  ^
      |   | verification failure, rollback,              |   | object arrives          |  | newer object appears
      |   | fell behind retention, memory limit          |   +-------------------------+  |
      |   v                                              | transient store error           |
      |  STALLED <----- verification failure ------------+----------> LAGGING <-----------+
      |   (terminal for the process; reads continue under D112)        |
      +----------------------- store recovers ------------------------+
 close -> CLOSED
```

`CURRENT` means the last poll found no newer object. `LAGGING` means the store failed transiently since. `STALLED`
carries a reason string and an event. The state machine is the pure function `FollowerCore.step()` over a store, a
clock, a marker and a replica writer, so the simulator drives it directly (D117).

## 6. Replica kernel

`GraphKernel.openReplica(maxMemoryBytes)` returns a kernel in the replica role. `applyReplicated(records, lsnFirst,
lsnLast)` hands the bytes to `ReplicaWriter`, which splits them into transactions with `TransactionTracker` and, per
transaction (D119, D123):

```
reservation.prepare(transaction bytes)        EPOCH records are skipped by the reservation, applied by the applier
sequence.beginWrite()
  reservation.reserve()                       memory limit: NodusMemoryError, nothing of this transaction applied
  applier.apply(each record)
  appliedLsn = commit LSN; lastCommitMicros = the commit record's timestamp
sequence.endWrite()
notify waiters
```

An LSN gap or a repeat is a `ChainTrustException`. Every public write method (`addEdge`, `removeTuple`, `commit`,
`claimKeyKind`, `checkpoint` and the batch calls) calls `requireWritable()` first. `epoch()` and `token()` read the
epoch history, which the applied `EPOCH` records and the loaded snapshot both populate.

## 7. Restore, takeover and salvage

**Restore** `restore(follower config, target)`: bootstrap into a replica kernel, apply to the head, then
`GraphInstaller` writes `snapshot.bin`, `FORMAT`, the tripwire and an empty `log/` into `target.restore-tmp` and
renames it to `target`. The salt, the schema, the symbols and the epoch history come from the snapshot and the
records. A crash leaves no directory that looks like a graph. The target must not exist or must be empty.

**Takeover** (D110, D125), run by the process that will become the writer:

```
claim   EpochClaims.claim(latest + 1)        the old writer fails its next epoch check: FENCED
loop up to 3 times:
  restore into <path>                         head h1
  DurableGraph.open(<path>, shipping)         reconcile: claims its own epoch, writes EPOCH(e, key, handoff = h1)
      WriterFencedException "older than the chain"  -> the old writer landed one object after our read:
                                                      delete <path>, restore again (at most one more object exists)
      success                                 -> await the EPOCH record in the bucket
report  { claimed epoch, epoch, handoff LSN, restored objects }
```

The claim leaves at most one in-flight object from the old writer (it checks before every commit), so the loop
converges in two rounds. The claimed epoch number stays unused, like any orphan claim. Two operators racing both
succeed in claiming; the later epoch wins and the earlier new writer is fenced on its next commit.

**Salvage** `salvage(old directory, follower config)` is read-only. It reads the old directory's epoch history, scans the chain for
the first `EPOCH` record of a later epoch and takes its handoff LSN `h` (without one, the chain head's last LSN,
reported as provisional). It then lists the old log's whole transactions above `h` through `LogTailReader` without
truncating or writing anything, and refuses when another process holds the directory lock. A trimmed range is
reported as unavailable.

## 8. Configuration

```
{ "store":      { same blocks as ShippingConfig },
  "credentials": { same },
  "trust":      { "key_id": 1, "public_key_file": "/etc/nodus/signing.pub" },        required
  "follow":     { "poll_interval_ms": 100, "read_wait_ms": 1000, "max_staleness_ms": 30000,
                  "download_parallelism": 4, "request_timeout_ms": 10000,
                  "state_directory": "/var/lib/nodus/follower" } }                  every field optional
```

Unknown keys are rejected. A `signing` block, a `key_file` or anything that names a private key is an error by
construction, because `FollowerConfig` has no field for it. Secrets are never echoed.

## 9. Error contract

| Code | Class | Follower and recovery meaning |
|---|---|---|
| -4 | `NodusStaleReadError` | a token the follower has not reached after `read_wait_ms`, or the staleness bound passed |
| -8 | `NodusWriterFencedError` | the old writer after a takeover; also a restore target behind the chain |
| -9 | `NodusCorruptLogError` | a damaged anti-rollback marker, or a damaged salvage log |
| -10 | `NodusUnsupportedError` | any write on a follower |
| -13 | `NodusTokenLostError` | a token above a handoff, on a follower as on a writer |
| -15 | `NodusChainTrustError` (new) | bad signature, unknown signer, broken link, epoch regression, rollback or fork |

## 10. Failure modes

| Codepath | Failure | Test | Handling | Caller sees |
|---|---|---|---|---|
| anchor | newest reference's records were deleted | retention regression, simulator | D106 prevents; else try an older reference | follower bootstraps |
| anchor | snapshot object missing or truncated | store with a deleted object | size check, then older reference | follower bootstraps or BOOTSTRAPPING |
| download | range fails or is cut | `FaultProxy` | per-range retry | slower bootstrap |
| download | bytes differ from the signed hash | corrupted object | reject, try an older reference | STALLED if none |
| fetch | forged or unsigned object | tamper table, both trust modes | `ChainTrustException` | STALLED, -15 on open |
| fetch | object with an unknown key id | keyring without it | REQUIRED trust throws | STALLED |
| follow | bucket rolled back | marker test, simulator | head below marker seq | STALLED, -15 |
| follow | fork at the same seq | digest differs | digest compare | STALLED |
| follow | next object deleted by retention | simulator, fake clock | once-a-second LIST | STALLED "fell behind retention" |
| follow | store down or slow | faulty store | LAGGING with backoff | stale answers or -4 |
| follow | credentials expired | provider test | one refresh, then STALLED | stats reason |
| apply | memory limit mid-object | limit test | prefix applied, STALLED | `NodusMemoryError` in stats |
| apply | reader during apply | stress with latency assertion | per-transaction section | microsecond stall |
| schema | writer migrates the schema | follower check after the migration | `currentModel()` | correct answers |
| marker | torn write | crash at every step of the write | temp and atomic rename | old or new, never partial |
| marker | damaged file | flip every byte | fail closed (D122) | open error naming the file |
| restore | crash at any step | crash at each step | temp directory, rename last | no half graph |
| restore | non-empty target | unit | refuse | `NodusUnsupportedError` |
| takeover | old writer lands one more object | proxy and simulator | restore again | opens |
| takeover | old writer still alive | real server | claim fences it | old writer -8 on its next commit |
| takeover | two operators | simulator | later epoch wins | earlier one -8 |
| takeover | bucket empty or chain unverifiable | unit | refuse before claiming where possible | -15 or -1 with a message |
| salvage | log trimmed, torn or locked | unit | report, never write | listing or refusal |
| tokens | write lost in a takeover | follower token test | epoch history | -13 |

No row is silent with neither a test nor handling.

## 11. Invariants checked by the simulator

- **I1 prefix**: every follower's graph digest at LSN `n` equals the digest of the committed chain prefix ending at `n`.
- **I2 monotone**: a follower's applied `(epoch, lsn)` and persisted seq never decrease.
- **I3 no fork**: at most one chain object exists per seq, and the final chain verifies end to end.
- **I4 fencing**: no object of epoch `e` is committed after an epoch above `e` was claimed, apart from at most one in flight.
- **I5 acknowledged `lake` writes survive**: every write acknowledged with durability `lake` is in the final chain, across takeovers.
- **I6 tokens do not lie**: a check with a token returns state at least as new as the token, or `-4` or `-13`.

The scheduler picks the next actor from `Random(seed)`, advances a virtual clock, and may crash an actor between two
steps or fail a store call. A failure prints the seed and the step number; the seed is pinned as its own test.

## 12. Sequencing

Each row is one or more commits, green on the full suite before the next starts. Refactors land before features.

| # | Change | Decisions | Verify |
|---|---|---|---|
| 1.1 | `ChainRetention` anchor walk-back | D106 | slow-snapshot regression bootstraps after eight idle days |
| 1.2 | `ChainFetch` with `Trust`; `ChainHead` and `StoreChainSource` use it | D113 | tamper table in both modes; the existing 438 shipping tests |
| 1.3 | `StoreConfig`, `StoreFactory`, `FollowerConfig`; `ShippingConfig` on the shared parser | D108 | all existing config cases byte-identical; follower cases |
| 1.4 | `SnapshotDownloader` | D120 | truncation, flipped bytes, range retry, size mismatch |
| 1.5 | `AnchorFinder`, `ChainReplay` | D111 | anchor matrix; replay into a sink equals the writer digest |
| 1.6 | kernel role, `ReplicaWriter`, `TupleStore.currentModel` | D107, D114, D119, D123 | write-refusal table, torn-read stress, schema migration |
| 1.7 | `GraphInstaller`, `Restore`, error -15 | D121 | restored directory equals the writer digest; crash at each step |
| 2.1 | `EventLog` extraction; `FollowerState` | D124 | `ShipStateTest` unchanged |
| 2.2 | `AntiRollbackMarker` | D109, D122 | crash at each write step; flip every byte |
| 2.3 | `FollowerCore`, `FollowerRunner`, `FollowerRuntime` | D111, D116 | transition table; writer and follower in one JVM |
| 3.1 | `FollowerGate`, token wait, `nodus_open_follower`, stats | D112, D126 | stale reads, waits, staleness bound |
| 3.2 | Python `Follower`, `Graph(follow=)`, `restore` | D115 | native run on three systems |
| 4.1 | `Takeover` | D110, D125 | old writer fenced; one more object; two operators |
| 4.2 | `Salvage`, exports, Python | D110 | trimmed, torn, locked, provisional |
| 5.1 | Simulator | D117 | 300 seeds in CI, replay by seed |
| 5.2 | `FaultProxy` chaos against SeaweedFS | D118 | lost PUT reply; cut snapshot GET; old writer fenced |
| 5.3 | Benchmarks, docs, CI | | follower catch-up throughput; `check` before and after `currentModel` |

### 12.1 Performance expectations (to measure, not claims)

- Verification per object: one Ed25519 verify (tens of microseconds) and one SHA-256 over up to 1 MiB (about 1 ms).
- Apply: the writer's apply path costs 1 to 2 microseconds per tuple, so a 1 MiB object of about 30,000 tuples applies in about 60 ms.
- Catch-up ceiling: one object per round trip, so about 15 MiB/s at 70 ms per GET, above the 2.5 MB/s a saturated writer produces.
- Bootstrap: download (four ranges, about 150 to 250 MB/s on S3) plus the 2 to 4 second load at 30M edges.
- Idle cost: 10 GET/s per follower at the default 100 ms plus one LIST/s. 500 followers reach 5,000 GET/s (TODOS).
- Reads: `currentModel()` adds one volatile read and one int compare. `check` is measured before and after.

## 13. NOT in scope

- Live rebuild of a follower that fell behind retention (TODO, D116).
- A local follower checkpoint for fast restarts (TODO, D109).
- Adaptive polling for large fleets (TODO).
- Splitting the rest of `ShipState` (TODO, D124 extracts only the event log).
- Automatic failover and leases, the writer-to-follower stream, multi-writer namespaces (TODOS, unchanged).
- Starting a new chain after a gap of unshipped records (TODOS; now depends on restore, which this step builds).
- Redaction objects (Step 5), Leopard (Step 4), watch and `as_of` (Step 6).
- Cloud role credential providers, key rotation tooling, Prometheus adapter (TODOS, unchanged).

## 14. What already exists

| Need | Existing | Use |
|---|---|---|
| Verify an object | `ChainVerifier`, `ChainRecords.verify` | behind `ChainFetch` |
| Follow a chain | `ChainCursor` | unchanged; the follower's position |
| Find the head | `ChainHead` | uses `ChainFetch`; rollback check |
| Read objects | `StoreChainSource` | uses `ChainFetch` |
| Load a snapshot | `SnapshotReader.load`, `SnapshotWriter` | bootstrap and install |
| Apply records | `RecordApplier`, `TransactionTracker` | `ReplicaWriter` |
| Claim an epoch | `EpochClaims` | takeover |
| Continue a chain and write the handoff | `OpenReconciler`, `SegmentedLog.beginTenure` | takeover's open |
| Store parsing | `ShippingStores`, `ShippingConfig` | split into `StoreConfig` and `StoreFactory` |
| Fault injection | `FaultyObjectStore`, `SimulatedDisk`, `ShipperRig`, `ChainAudit` | the simulator's world |
| Graph equality | `GraphDigest` | the I1 oracle |
| Real server in CI | SeaweedFS job, `RealS3ContractTest` | chaos tests |

## 15. Parallelization

| Step | Modules | Depends on |
|---|---|---|
| M1 foundations and restore | `ship/`, `chain/`, `config/`, `kernel/`, `authz/`, `storage/`, `replica/` | none |
| M2 follower core | `replica/`, `ship/` (event log) | M1 |
| M3 follower API | `capi/`, `python/`, `replica/` | M2 |
| M4 takeover and salvage | `replica/`, `capi/`, `python/` | M1, M3 |
| M5a simulator | `core/src/test/.../sim/` | M2, M4 |
| M5b `FaultProxy` and chaos | `core/src/test/.../objectstore/` | M1 |
| M5c benchmarks and docs | `bench/`, `python/benchmarks/`, `docs/` | M3 |

Lane A: M1 then M2 then M3 then M4 (shared `replica/` and `kernel/`). Lane B: the `FaultProxy` class alone can be built
at any time. Lane C: docs follow each milestone. Everything else is sequential.

## 16. Review summary

- Step 0: scope accepted as the full Step 3 (D105).
- Architecture: 7 issues (retention anchor, replica seam, follower config, follower state, takeover shape, gap detection, stalled reads). One P1 in shipped code.
- Code quality: 4 issues (verified fetch DRY, schema refresh, API surface, rebuild policy).
- Tests: coverage plan for 62 paths, 6 regressions, 9 end-to-end flows, 2 method decisions (simulator, chaos).
- Performance: 2 issues (apply window, snapshot download).
- Outside voice: skipped by the owner.
- TODOS proposed: 4 added (live rebuild, local checkpoint, adaptive polling, the `ShipState` split).
