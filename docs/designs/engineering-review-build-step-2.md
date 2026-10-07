# Engineering review: Build Step 2, the shipper and the lake projection

Status: decisions D88 to D104 accepted on 2026-10-07. Implementation scope is Build Step 2 (section 11).
Contracts: [`engineering-review-authz-wedge.md`](engineering-review-authz-wedge.md). Where this document and that one
disagree about Build Step 2, this document wins (section 2 lists every amendment).

Step 2 is the writer side of "the log is the lake". The writer copies its log to object storage as signed chain
objects, a write can wait until that copy exists (`LAKE` durability), and the same history becomes an Iceberg table
that other engines read with SQL. Followers, takeover and the full simulation harness are Build Step 3.

---

## 1. Scope and the boundary with Step 3

```
                        Build Step 2 (this review)                      Build Step 3
 +--------------------------------------------------------------+   +----------------------+
 | writer: tail reader, shipper, signed chain objects, CAS      |   | followers: verify,   |
 | commit, epoch claim, fencing detection, snapshot upload,     |   | bootstrap, states,   |
 | retention, LAKE durability, backlog, stats                   |   | stale-read waits     |
 | lake: Iceberg projection of the chain (Avro, metadata)       |   | takeover and salvage |
 | api: config document, wait call, stats call, error -14       |   | full simulation      |
 +--------------------------------------------------------------+   | harness, MinIO chaos |
                                                                    +----------------------+
```

Step 2 tests the commit protocol with a seeded fault-injecting store and a crash after every store call (D101).
That is the seed of the Step 3 harness, not the harness itself.

## 2. Amendments to the frozen design

| Section | Frozen text | Step 2 text | Decision |
|---|---|---|---|
| 4.9 | trim bound is `min(snapshot_lsn, shipped_lsn)` | unchanged for local trim; chain retention also stops at the projector's committed LSN | D89 |
| 6 step 5 | `LAKE` waits without the monitor | the wait is a separate call made outside every lock; a timeout raises error -14 | D92 |
| 8.1 | head is found by position | head is found from the newest `SNAPSHOT_REF`, then a listing after its position | D103 |
| 8.3 | handles 412 and timeouts | adds 409 retry, unknown-outcome compare, and a capability probe | D91 |
| 13 | codes -1 to -13 | adds -14 `NodusShipTimeoutError` | D92 |
| 7 | 13 columns | unchanged | none |

## 3. Decision log

| ID | Issue | Decision |
|---|---|---|
| D88 | Scope | Full writer-side Step 2 in six milestones on one branch, mergeable after each |
| D89 | Projector input | The projector reads signed chain objects, through a bounded in-memory ring first. Local trim stays `min(snapshot, shipped)`. Chain retention stops at `min(7 days rule, projected_lsn)` |
| D90 | Open against a bucket | Reconcile at open. Empty bucket: start with a `SNAPSHOT_REF`. Local at or ahead of the head with matching epochs: claim `max(local, head) + 1` and resume. Local behind or epochs disagree: refuse with `NodusWriterFencedError` |
| D91 | Commit protocol | 409 means back off and retry the same bytes; timeouts, resets and 5xx are unknown outcomes (GET, compare, retry the identical PUT only if absent); 4xx auth errors are fatal and shown in stats; a startup probe refuses stores that ignore `If-None-Match` |
| D92 | `LAKE` wait | Native write returns the token at once; `nodus_await_shipped` waits outside the session monitor and the Python lock; timeout raises `NodusShipTimeoutError` (-14) carrying the token |
| D93 | Signing key | Operator-supplied Ed25519 key (raw seed or PKCS#8) and explicit `key_id`; verifiers trust a keyring from their own configuration, never the bucket |
| D94 | Snapshot upload | Multipart upload with 64 MiB parts and per-part retry; runs on its own thread after the local rename; `SNAPSHOT_REF` is committed only after the object and its SHA-256 are verified |
| D95 | Credentials | `CredentialsProvider` with explicit keys and an environment or file source that re-reads on `ExpiredToken` or 403; cloud role providers are a TODO |
| D96 | Stats | One JSON export `nodus_stats_json` and `Graph.stats()`; threshold crossings go to the `nodusdb` logger once per crossing |
| D97 | Trim wiring | `ShippingLogStore` wraps the log and clamps `trim`; a `ShipWatermark` interface (shipped LSN, await) is reached through one kernel accessor; `SegmentedLog`, `VolatileLog` and `DurableStore` do not change |
| D98 | Parquet writer | A `ColumnSource` interface; `DeltaMemTable` implements it; the footer learns field ids, a timestamp logical type and a no-key-column mode; existing lake output stays byte-identical |
| D99 | Names | A `NameResolver` interface with a kernel-backed implementation; the projector runs inside the writer; schema version per event comes from `SCHEMA` records in stream order and is stored with the projected LSN in the Iceberg snapshot properties |
| D100 | Config transport | One JSON document through one new export `nodus_open_durable_shipping`, parsed and validated by one Java class; unknown keys are rejected; secrets are never echoed |
| D101 | Testability | `ShipperCore` is a deterministic step function; `ShipperRunner` is a thin loop; tests use a manual clock, a faulty store and a seed, and crash the writer after every store call |
| D102 | CI verifiers | pyiceberg, fastavro and botocore as test-only packages; botocore-generated SigV4 vectors are committed; MinIO runs as a pinned container on the Ubuntu job only |
| D103 | Head discovery | Newest `SNAPSHOT_REF` (snapshot objects carry the chain position at upload start as metadata, a lower bound only), then a listing with `start-after`; a missing hint falls back to a full listing; the result is always verified by signature |
| D104 | Performance proof | JMH baselines with `-prof gc`, recorded in `bench/baseline/shipping-windows-dev.json`; a CI step compares `add_tuple` with shipping off and on and fails above a ratio of 3 |

## 4. Architecture

```
 TupleStore.write ---> GraphKernel ---> ShippingLogStore ---> SegmentedLog (fsync)
                                              | trim clamp          | durableLsn
                                              |                     v
                                              |              LogTailReader (read only, whole transactions)
                                              |                     |
                                              |                     v
                                              |              ShipperCore.step(now)  <--- ShipperRunner thread
                                              |                     |  sign, PUT chain/{seq} If-None-Match
                                              |                     v
                                              +--- ShipWatermark <- ObjectStore ---------------------------+
                                                    shippedLsn      (s3 | directory)                        |
                                                    awaitShipped            |                               |
                                                                            v                               |
                                              SnapshotUploader (multipart) -> SNAPSHOT_REF                  |
                                              ChainRetention (7 days, newest ref, projected_lsn)            |
                                                                                                            |
              ChainReader (ring, then GET) --> EdgeLogProjector --> ColumnSource --> ParquetWriter --> data files
                                                    |                                                       |
                                                    +--> Avro manifest, manifest list, metadata v{N}.json, version-hint
                                                                                                            |
                                                             commit: PUT metadata If-None-Match <-----------+
```

Packages (planned):

| Package | Contents |
|---|---|
| `io.nodusdb.json` | strict JSON reader and writer, no dependency |
| `io.nodusdb.objectstore` | `ObjectStore`, `PutOutcome`, `ObjectStoreException` (retryable and fatal), `MemoryObjectStore`, `DirectoryObjectStore`, `FaultyObjectStore` (test scope) |
| `io.nodusdb.objectstore.s3` | `S3ObjectStore`, `S3Config`, `SigV4Signer`, `CredentialsProvider` and two implementations, `MultipartUpload`, `ConditionalWriteProbe` |
| `io.nodusdb.chain` | `ChainObject`, `ChainHeader`, `ChainKind`, `ChainCodec`, `SigningKey`, `ChainSigner`, `ChainVerifier` |
| `io.nodusdb.ship` | `LogTailReader`, `ShipperCore`, `ShipperRunner`, `ShipperState`, `ShippingConfig`, `ShippingLogStore`, `ShipWatermark`, `OpenReconciler`, `EpochClaim`, `SnapshotUploader`, `ChainRetention`, `BacklogGuard`, `ShippingStats` |
| `io.nodusdb.lake.iceberg` | `EdgeLogProjector`, `EdgeLogColumns`, `NameResolver`, `IcebergCommitter`, `TableMetadata`, `ManifestWriter`, `ManifestListWriter` |
| `io.nodusdb.lake.iceberg.avro` | `AvroWriter` (object container files) and the fixed manifest schemas |

Dependency direction: `ship` depends on `log`, `chain` and `objectstore`; `lake.iceberg` depends on `lake.parquet`,
`chain` and `objectstore`; the kernel depends on none of them and reaches shipping only through `ShipWatermark`.

### 4.1 Shipper states

```
 DISABLED          shipping not configured; every path is the Step 1 path
 STARTING          open reconciliation (section 5)
 ACTIVE            shipping; stats show shipped LSN and backlog
 RETRYING          transient failure; backoff, same bytes
 FAILED            fatal configuration or authorization error; operator action; backlog grows
 FENCED            a higher epoch exists; terminal for the process (-8 on every write)
 CLOSED

 STARTING -> ACTIVE <-> RETRYING
 ACTIVE | RETRYING -> FAILED (4xx auth, bucket missing) | FENCED
```

## 5. Open sequence (D90, D103)

```
open durable graph with shipping configured
 1 recover the local log and snapshot (unchanged)
 2 connect the store; run the conditional-write probe; refuse the store if it ignores If-None-Match
 3 find the head: newest SNAPSHOT_REF (snapshots folder), then LIST chain/ with start-after
 4 reconcile:
```

| Bucket | Local | Action |
|---|---|---|
| empty | fresh or trimmed | upload the local snapshot, commit `SNAPSHOT_REF` first; stats record the history gap before it |
| head at LSN H, epoch history matches | last LSN >= H | claim epoch `max(local, head) + 1` with `If-None-Match`; on 412 try the next epoch; resume at H + 1 |
| head at LSN H | last LSN < H | refuse: `NodusWriterFencedError` naming both positions |
| head at LSN H | epoch at H differs from the head's | refuse: `NodusWriterFencedError` |
| head found, a higher epoch claim exists | any | refuse: `NodusWriterFencedError` |

```
 5 write the local EPOCH record for the claimed epoch (SegmentedLog.open)
 6 start ShipperRunner and, when configured, the projector
```

The epoch is chosen before `SegmentedLog.open`, so `DurableGraph` takes it from the reconciler instead of
`latestEpoch() + 1` (`DurableGraph.java:92`). With shipping off the old line runs unchanged.

## 6. Commit protocol (D91)

```
PUT _nodus/chain/{S}.obj  If-None-Match: *     bytes are built once; every retry sends identical bytes
```

| Answer | Action |
|---|---|
| 200 | committed; `shipped_lsn = lsn_last`; wake `awaitShipped` callers |
| 409 | back off, retry the same PUT, then compare as for 412 once the retry budget is spent |
| 412 | GET `{S}`; ours (epoch, nonce, prev) = committed; foreign with a higher epoch = FENCED; foreign with a lower epoch = lost the open race, FENCED for this process |
| timeout, reset, 5xx | unknown: GET `{S}`; ours = committed; absent = retry the identical PUT |
| 401, 403, 404 bucket | FAILED; not retried; shown in stats; one credentials refresh first on `ExpiredToken` |
| before each commit | HEAD `_nodus/epoch/{E+1}.json`; present = FENCED |

Chain objects are built from whole transactions only. A transaction that exceeds the object size limit ships alone.
Upload cadence is 100 ms or 1 MiB. One PUT is in flight at a time, because each object names its predecessor.

## 7. `LAKE` durability and backlog (D92, D96)

```
write(tx, durability)                      wait(token, timeout)
  [session monitor, Python lock]             [no monitor, no Python lock]
  apply, append, fsync                       shipWatermark.awaitShipped(lsn, timeout)
  return token                               timeout -> NodusShipTimeoutError(token)
```

`graph.write(tx, durability="lake", timeout=...)` performs both calls and releases its lock between them. Concurrent
writers share one upload, so the PUT count stays below the write count. `wait_shipped(token)` is public. Without
shipping configured, `lake` still raises `NodusUnsupportedError`, now with the message "shipping is not configured".

Backlog is the bytes of local segments above `shipped_lsn`. At 50% of the cap one warning goes to the `nodusdb` logger
and stats; at the cap every write, `LOCAL` and `LAKE`, raises `NodusLogBacklogError` (-7) until shipping catches up.

## 8. Shipping configuration (D100)

One UTF-8 JSON document. Unknown keys are rejected, errors name the field, and secrets are never echoed.

| Block | Keys |
|---|---|
| `store` | `type` (`s3` or `directory`), `bucket`, `prefix`, `region`, `endpoint`, `path_style`, `ca_bundle`, `directory` |
| `credentials` | `source` (`static`, `environment`, `file`), keys for `static`, `file` path |
| `signing` | `key_file`, `key_id` |
| `ship` | `interval_ms` (100), `max_object_bytes` (1 MiB), `backlog_cap_bytes` (1 GiB), `retention_days` (7), `request_timeout_ms` |
| `iceberg` | `enabled`, `commit_interval_s` (60), `table_retention_days` |

The `directory` store is for development and shared filesystems with atomic create-exclusive. It implements
put-if-absent with a hard link to a fully written temporary file.

## 9. Stats (D96)

`Graph.stats()` returns a dict with: `shipping.state`, `shipped_lsn`, `durable_lsn`, `backlog_bytes`,
`backlog_cap_bytes`, `last_commit_age_ms`, `consecutive_failures`, `last_error`, `epoch`, `chain_seq`,
`snapshot_age_s`, `history_gap_before_lsn`, and `projection.state`, `projected_lsn`, `iceberg_snapshot_id`,
`last_projection_commit_age_s`. Fields are added, never renamed.

## 10. Iceberg projection (D89, D98, D99)

- The projector reads chain objects in sequence, from a bounded ring (default 32 MiB) first and by GET after that.
  It verifies signatures on objects it fetched.
- It applies whole transactions only and writes the 13 columns of the wedge design's section 7 through
  `ColumnSource`: `lsn`, `commit_ts` (timestamp, microseconds, UTC), `txn_lsn`, `epoch`, `event`, `object_type`,
  `object_id`, `relation`, `subject_type`, `subject_id`, `subject_relation`, `schema_version`, `detail`.
- Names come from `NameResolver`. `schema_version` is tracked from `SCHEMA` records in stream order.
- A row group is flushed whenever 122,880 rows are buffered, so memory stays bounded at any write rate.
- Each commit: write data files, write the manifest and manifest list (Avro, with the required key-value metadata:
  `schema`, `schema-id`, `partition-spec`, `partition-spec-id`, `format-version`, `content`), write
  `metadata/v{N}.metadata.json` with `If-None-Match`, then write `version-hint.text`. Partition spec is
  `day(commit_ts)`, sort order is `lsn`.
- The snapshot summary carries `nodus.projected.lsn`, `nodus.projected.epoch`, `nodus.projected.schema_version` and
  `nodus.commit.v1` (signed). A restart reads the highest `v{N}` metadata file, not the hint, so a missing hint
  never loses progress.
- Orphan data files from a crashed commit are never referenced and are removed by the next successful commit.

## 11. Build Step 2: sequencing

Each row is one or more commits, green on the full suite before the next starts. Refactors land before features.
Milestones: M1 = 2.0 to 2.2, M2 = 2.3, M3 = 2.4 and 2.5, M4 = 2.6, M5 = 2.7 and 2.8, M6 = 2.9 to 2.11.

| # | Change | Decisions | Verify |
|---|---|---|---|
| 2.0 | Native-image spike (Ed25519, SHA-256, HttpClient over HTTP and HTTPS, trust store) with go or no-go; strict JSON codec; error classes and codes (-14, backlog, fenced) | D93, D100 | spike written up in `docs/decisions.md`; JSON fuzz; every code maps in Java and Python |
| 2.1 | `ObjectStore` seam, memory, directory and faulty stores, shared contract suite | D101 | one suite on every backend; N threads racing one key give one winner |
| 2.2 | S3 client: SigV4, credentials, status mapping, probe, multipart; in-JDK fake S3; MinIO job | D91, D94, D95, D102 | botocore golden vectors; fake server in ignore mode is refused; MinIO on Ubuntu |
| 2.3 | Chain codec, signing keys, verifier | D93 | golden corpus; flip every byte and truncate at every length |
| 2.4 | `LogTailReader`, `ShipWatermark`, `ShippingLogStore` | D97 | tail beside a live writer; transaction across a segment roll; trim clamp; Windows delete |
| 2.5 | `ShipperCore`, runner, open reconciliation, epoch claim, head discovery, `awaitShipped`, backlog | D89 to D92, D101, D103 | crash after every store call; reconcile matrix; outcome table |
| 2.6 | Snapshot upload, `SNAPSHOT_REF`, retention | D94, D89 | writers never paused; crash between upload and reference; fake-clock retention |
| 2.7 | Golden bytes of today's lake output, then `ColumnSource` and footer extensions | D98 | golden test green before and after; PyArrow checks unchanged |
| 2.8 | Avro writer, Iceberg metadata, projector, `NameResolver`, expiry, signed property | D99, D102 | pyiceberg scan equals the log row for row; fastavro reads every file; kill and resume |
| 2.9 | C ABI, Python `Shipping`, `stats()`, `wait_shipped`, `generate_signing_key`, logger | D92, D96, D100 | every error code; config rejection cases; native run on three systems with the directory store |
| 2.10 | JMH baselines and the ratio gate step | D104 | gate passes; fails with a deliberate synchronous PUT in `commit` |
| 2.11 | Docs: README, `docs/architecture.md`, `docs/decisions.md`, `docs/lake.md`, TODOS | | no stale "lake is unsupported" text |

### 11.1 Regression tests that land first

- R1 The Parquet refactor leaves lake output byte-identical: the golden test lands before the refactor (2.7).
- R2 `TupleStoreTest`, `GraphSessionAuthzTest` and `test_authz.py::test_lake_durability_is_not_available_yet` change
  from "lake is always unsupported" to "unsupported unless shipping is configured".
- R3 With shipping off, the directory layout, the open path and the write path are unchanged, and the commit path
  allocates nothing new (JMH `-prof gc`).
- R4 Python moves the `LAKE` wait outside its lock; the parallel-readers test must still pass.

## 12. Failure modes

| Codepath | Failure | Test | Handling | Caller sees |
|---|---|---|---|---|
| shipper PUT | reply lost after the object landed | faulty store, lost response | GET and compare, adopt | committed, no fence |
| shipper PUT | 409 storm | faulty store | back off, same bytes | longer `LAKE` waits, retries in stats |
| open | store ignores `If-None-Match` | fake server ignore mode | refuse to enable | `NodusUnsupportedError` naming the store |
| open | local behind the head | reconcile matrix | refuse | `NodusWriterFencedError` |
| open | epoch claim returns 412 | reconcile matrix | next epoch | opens |
| commit | higher epoch claim appears | outcome table | FENCED, terminal | -8 on every write |
| shipper | crash between PUT and bookkeeping | crash after every store call | head discovery adopts our object | nothing |
| shipper | credentials expire | provider test | one refresh, then FAILED | stats `FAILED`, backlog grows, `LAKE` waits time out |
| shipper | bucket unreachable for hours | faulty store | warn at 50%, refuse at the cap | -7, clears on recovery |
| `LAKE` wait | store slow | manual clock | timeout | -14 carrying the token |
| `LAKE` wait | close during the wait | unit | wait raises closed | closed error |
| snapshot | multipart part fails | fake server | retry the part; abort and clean up on give-up | snapshot age grows in stats |
| snapshot | crash between upload and reference | crash test | orphan removed by retention | nothing |
| retention | object above `projected_lsn` | fake clock | kept | nothing |
| projector | crash after data files, before metadata | kill and resume | resume from the highest metadata file; orphans removed | no duplicate rows |
| projector | metadata CAS returns 412 | outcome test | re-read the head; fence if foreign | none or -8 |
| tail reader | segment deleted under the reader | Windows CI | trim clamp prevents it; a real gap is an error | `NodusCorruptLogError` |
| trim | checkpoint while shipping lags | unit | clamp | disk grows, backlog warning |
| config | secret in a validation error | unit | never echoed | field name only |

No row is silent with neither a test nor handling.

## 13. Performance expectations (to measure, not claims)

- `LOCAL` writes with shipping on stay within 1.25 times of shipping off; the CI gate fails at 3 (D104).
- A `LAKE` write waits one upload round trip: about 130 to 300 ms with the default cadence.
- A saturated writer makes about 864,000 PUTs a day at the default cadence. Idle writers make none. The README
  carries a cost table and the `interval_ms` setting.
- Catching up after an outage ships 1 MiB objects one at a time; at 50 to 100 ms each that is about 15 MiB per second,
  above the write rate the design targets.
- The projector is single-threaded at low priority; its native buffers are bounded and appear in stats.

## 14. NOT in scope

- Followers, bootstrap, verification, takeover, salvage, the full simulation harness: Build Step 3.
- Leopard, erasure and redaction (`REDACTION` objects are rejected until Step 5), watch, `as_of`: Steps 4 to 6.
- Cloud role credential providers (TODO), signing key rotation tooling (TODO), Prometheus and OpenTelemetry adapter
  (TODO), a portable snapshot-backed `NameResolver` (TODO).
- An Iceberg REST catalog target, compaction of small data files, adaptive chain object size while catching up.
- Backfilling lake history from before shipping was enabled: the history before the first `SNAPSHOT_REF` does not
  exist in the lake, and `stats()` reports where it starts.

## 15. What already exists

| Need | Existing | Use |
|---|---|---|
| Read segments and records | `LogFileSystem`, `LogChannel`, `ChannelWindow`, `RecordReader`, `SegmentHeader` | reused by `LogTailReader`; `LogRecovery` is not reused because it truncates and deletes |
| Durable position | `LogStore.durableLsn()` | bound for the tail reader |
| Trim | `SegmentedLog.trim`, `DurableStore.checkpoint` | unchanged; clamped by the wrapper |
| Parquet pages, Snappy, dictionary, statistics | `lake.parquet`, `lake.codec` | reused through `ColumnSource` |
| Name tables | `kernel.symbols()`, `kernel.catalog()` | behind `NameResolver` |
| Error codes -7 and -8 | `ErrorCode`, `python/nodusdb/errors.py` | Java exception classes added |
| Hashing and signatures | JDK `MessageDigest`, `Signature` (Ed25519) | no dependency |
| HTTP | JDK `HttpClient`; `com.sun.net.httpserver` for the test fake | no dependency |
| Native gate pattern | `python/benchmarks/native_ratio.py` | pattern for the shipping ratio step |
| Crash harness pattern | D73 | extended to "crash after every store call" |

## 16. Parallelization

| Step | Modules | Depends on |
|---|---|---|
| 2.0 | `json`, `error`, `python` | none |
| 2.1, 2.2 | `objectstore` | 2.0 |
| 2.3 | `chain` | 2.0 |
| 2.4 | `ship`, `log` (read only) | none |
| 2.5, 2.6 | `ship`, `storage` | 2.1 to 2.4 |
| 2.7 | `lake.parquet` | none |
| 2.8 | `lake.iceberg` | 2.1, 2.3, 2.7 |
| 2.9 | `capi`, `python` | 2.5, 2.6, 2.8 |
| 2.10, 2.11 | `bench`, `.github`, `docs` | 2.9 |

Lane A: 2.0, 2.1, 2.2, 2.3 (new packages, no conflicts). Lane B: 2.4, 2.5, 2.6 (`ship`, touches `storage` wiring).
Lane C: 2.7 then 2.8 (`lake`). Lanes A and C can run in parallel worktrees; lane B follows lane A. The build will
run sequentially on this branch unless asked otherwise.

## 17. Risks with a gate

- **Native image and TLS (2.0).** If Ed25519 or `HttpClient` do not work in the native image, or HTTPS trust cannot
  be configured through `ca_bundle`, the fallback is plain HTTP for loopback and MinIO plus a documented TLS
  terminator, and this document is revised before 2.1 starts.
- **Hand-written formats (2.2, 2.8).** Signatures, Avro and Iceberg metadata are judged by botocore, fastavro and
  pyiceberg (D102). A disagreement with any of them blocks the milestone.
