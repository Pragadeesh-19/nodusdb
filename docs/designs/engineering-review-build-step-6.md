# Engineering review: Build Step 6, watch and as_of

Status: decisions D187 to D203 accepted on 2026-10-16. Implementation scope is Build Step 6 (section 11), built on
branch feat/authz-step6 in three milestones. Choices made while building are recorded in
[`docs/decisions.md`](../decisions.md) after D203; where they differ from this document they win.
Contracts: [`engineering-review-authz-wedge.md`](engineering-review-authz-wedge.md) sections 4.5, 8, 9 and 13, and
the CEO plan [`relationship-lakehouse.md`](relationship-lakehouse.md) decisions 5A and 6A. Where this document and
those disagree about Build Step 6, this document wins (section 2 lists every amendment).

A watch lets an application learn what changed after a token it holds: every tuple added or removed, every schema,
epoch and erasure, in log order, in whole transactions. `AccessWatch` turns that feed into gained and lost access
for the checks an application registers. `as_of` rebuilds the graph as it stood at a past token or instant from the
signed bucket, and returns a read-only graph that answers every read the live graph answers.

---

## 1. First principles

1. **The log is the only history.** Every past state and every change since a token is a function of a prefix of
   the log (wedge 1). Watch reads the log; as_of replays it. Neither keeps a second copy of the truth.
2. **The token is the cursor.** `(epoch, lsn)` already orders commits, names freshness and marks lost writes
   (D79). A watch cursor and an as_of target are tokens, so the existing lost-token rule (-13) applies unchanged.
3. **Readers move at commit points only** (wedge L4). A watch batch and a past view both end on a commit point.
4. **Ids are stable, names are looked up late.** A symbol id is never reused (D162) and a relation id never
   changes (D81). The feed carries ids until it is read and resolves names then, so an erased name is read as its
   pseudonym from the moment `erase` returns, wherever the change came from.
5. **The past must not bring back what was erased.** A past view starts from a snapshot at or after the newest
   erasure, so neither the snapshot nor any record replayed after it holds an erased name (the D177 and D178
   argument). Erasures after the view was built are applied to the view before its next read.
6. **Costs have owners.** A past view is a second copy of the graph. It is an explicit handle with a memory budget,
   never a cache hidden behind `check`.

```
WATCH                                                  AS_OF
writer commit / replica apply                          graph.as_of(when) on a writer or a follower
  RecordApplier -> ChangeRing (ids, lazy, 1 producer)    writer: wait until the call-time token is shipped
                                                         resolve the target: token, or newest commit_ts <= t
changes(after = token, limit, wait)                      AnchorFinder(bounds: erase floor <= snapshot <= target)
  lost token -> -13; nothing new -> long poll            snapshot -> replica kernel (shared history budget)
  ring covers?        -> copy out                        replay chain objects, prefetch 8, stop at target commit
  local log covers?   -> LogTailReader (writer)          closure full pass on the builder thread
  chain covers?       -> ChainFetch (writer, follower)   -> PastGraph handle: check, has_edge, khop, token, advance
  otherwise           -> -17                           each read: apply erasures newer than the view
  ids -> names at read time -> ChangeWire
Python AccessWatch: after each batch, re-check registered (object, permission, subject) -> gained / lost
```

## 2. Amendments to the frozen design

| Where | Text before | Text now | Decision |
|---|---|---|---|
| CEO 6A | pull cursor by LSN over a bounded ring; `NodusCursorExpiredError` | pull by token over a bounded ring, then the local log, then the bucket chain; -17 `NodusHistoryExpiredError` only when none holds the cursor | D189, D196 |
| CEO 5A | lake snapshot plus replay into a scratch kernel; cache windows | bucket snapshot plus chain replay into a read-only replica kernel; an explicit handle with `advance` replaces cache windows | D192, D194 |
| CEO item 4 | watch delivers gained and lost access | the engine delivers tuple changes; `AccessWatch` (Python) derives gained and lost access for registered checks | D188 |
| Wedge 13 | codes -1 to -16 | adds -17 `NodusHistoryExpiredError` | D196 |

## 3. Decision log

| ID | Issue | Decision |
|---|---|---|
| D187 | Scope | Watch and as_of on one branch in three milestones, each green before the next: M1 watch, M2 as_of, M3 simulation, chaos, benchmarks and docs |
| D188 | What a watcher receives | The engine returns committed changes after a token: `add`, `remove`, `schema` (version), `epoch` (epoch) and `erase` (pseudonym), the event names of the lake table (D68), in LSN order and whole transactions, with a long-poll wait. Python `AccessWatch` re-checks its registered triples after each non-empty batch at that batch's token and reports flips. It reports the net change per batch, not the LSN of each flip, and costs one check per registered triple per batch |
| D189 | Watch reach | A bounded ring of ids, allocated on the first watch call; a cursor older than the ring is read from the writer's local log, then from the bucket chain. -17 only when none holds the record after the cursor |
| D190 | Cursor rules | A cursor that names a lost write raises -13 (D79). A cursor ahead of the node waits up to the call's wait and then returns an empty batch with the same cursor. A transaction is never split: the limit is soft and a batch holds at least one whole transaction. Names are resolved when the batch is read |
| D191 | C wire | `ChangeWire`, the shape of `TupleWire`: one UTF-8 blob, four name lengths per change, long columns (lsn, commit lsn, epoch, commit micros), a kind byte per change. The caller passes buffers and gets the sizes it needs to grow and retry (D15) |
| D192 | as_of source | The bucket only: the newest snapshot at or below the target plus chain replay, on writers and followers alike. A graph without shipping raises -10; `Shipping.directory` serves one machine. Reach is the bucket retention and never before the newest erasure |
| D193 | as_of and erasure | The anchor snapshot must be at or above the newest erase LSN, so no snapshot or record that holds an erased name is ever downloaded. A target below it raises -17. A view records the erased-symbol count it was built with; before each read it applies every newer erasure of its parent to its own symbol table |
| D194 | as_of API | `graph.as_of(when)` returns a read-only `PastGraph` over a replica kernel: every read export works on its handle, writes raise -10, `advance(later)` replays forward, `token` and `commit_time` report its position. Closing the parent closes its views |
| D195 | Recent instants | On the writer, `as_of` first waits until its token at call time is shipped (bounded by the call's timeout, else -14), so every commit up to the call is in the chain. A follower resolves against the chain it can read. A view always reports the commit it stands on. An instant in the future is refused |
| D196 | Error code | -17 `NodusHistoryExpiredError` for a watch cursor and an as_of target alike; the message names what ran out (ring, log and chain; retention; the newest erasure) |
| D197 | Closure in a view | The builder runs the reachability full pass on its own thread before returning the view, so a past check behaves like the live one and does not raise -5 while the index is dirty. `as_of`'s timeout bounds only the shipping wait |
| D198 | Reuse of the follower path | `AnchorFinder.candidates(AnchorBounds)` and `ChainReplay` with an optional stop LSN; `RecordSlice.between(first, last)`. Followers pass `AnchorBounds.NONE`. Signature, link and pinned-reference checks stay in one place |
| D199 | Simulation | `ChainSimulation` gains I7 (the batches a node serves for a cursor equal the committed tuple records of the log after it, or the read fails with -17 or -13 exactly when that history is gone or lost) and I8 (a past view at LSN n has the digest the writer recorded at n) |
| D200 | Catch-up reads | `ChangeFeed` keeps up to 4 open fallback readers, keyed by the next LSN they return, least recently used first, closed after 60 s idle. A poll that continues a reader's position does not locate again. The API stays stateless |
| D201 | View memory | A view's kernel has its own limit (default the parent's) and charges a history budget shared by all views of one graph (`history_max_memory_mb` on `Shipping` and `Follower`, default the parent's limit, unlimited when the parent is). A build that would pass either raises -3 and frees what it took |
| D202 | Replay cost | Replay prefetches up to 8 chain objects. A view's stats report its snapshot LSN, the objects and records replayed and the build time. Checkpoints stay manual, because one pauses writes; the README tells operators to checkpoint on a schedule (TODOS: checkpoints that do not pause writes) |
| D203 | Code shape | `kernel.changes.ChangeRing` (hot path, kernel rules). Package `io.nodusdb.watch`: `ChangeFeed`, `ChangeSource` with ring, log and chain sources, `ChangeExtractor`, `ChangeWire`. Package `io.nodusdb.history`: `TargetResolver`, `PastViewBuilder`, `PastViews`. `GraphSession` delegates. Python `nodusdb/watch.py` and `nodusdb/history.py` |

## 4. The change ring (D189, D203)

```
entry (3 longs): [ lsn ][ kind u8 | relation u16 | subject_relation u16 | spare ][ object u32 | subject u32 ]
kinds: ADD, REMOVE, SCHEMA (version in the third long), EPOCH (epoch), ERASE (symbol id), COMMIT (commit micros)
producer: RecordApplier.apply and the single-tuple fast path, under the writer lock; COMMIT ends a transaction
publish:  a volatile entry count is advanced only at a commit point
reader:   locate the cursor by binary search on lsn inside [count - capacity, count); copy out whole transactions;
          re-read the count; if the window moved past the copied start, discard and fall back
```

The ring holds no names and no `SYMBOL` or `GRAPH_CONFIG` records. It is allocated under the writer lock on the first
`changes` call (`watch_buffer_changes`, default 1,048,576 entries, 24 MiB on the heap, not charged to the native
budget, like result buffers in D55). Before that the commit path pays one null check. `KernelWriteBench` with a
watcher attached must stay within 10% of `main` (D4).

## 5. Reading changes (D188 to D191, D200)

```
changes(after = (e, n), limit, wait):
  e newer than the node's epoch, or n beyond its applied LSN  -> wait up to `wait`; then an empty batch, cursor (e, n)
  e older and n above handoff(e)                              -> -13 NodusTokenLostError
  applied == n                                                -> long poll until a commit or `wait`; maybe empty
  source = first of: ring (holds n + 1), open fallback reader at n + 1, local log (writer), chain
           none -> -17 NodusHistoryExpiredError
  read whole transactions until `limit` changes (at least one transaction)
  ChangeExtractor: records -> entries (log and chain bytes go through the same extractor as the ring)
  names from the live symbol table and catalog; an erased id reads as its pseudonym; epoch from the epoch history
  next cursor = (epoch, commit LSN) of the last transaction in the batch
```

The long poll waits on a notifier the kernel signals at each published commit, only while a reader waits. Chain reads
locate the object holding `n + 1` by a binary search over sequence numbers and then stream; local log reads use
`LogTailReader`. Both kinds of reader stay open in the D200 cache.

Python:

```
batch = graph.changes(after=token, limit=1000, timeout=0.0)      ChangeBatch(changes, next)
for batch in graph.watch(after=token, timeout=1.0): ...           stops when the graph closes
Change(kind, object, relation, subject, subject_relation, detail, token, commit_time)
access = AccessWatch(graph, [(object, permission, subject), ...], after=token)
access.poll(timeout=1.0) -> [AccessChange(object, permission, subject, allowed, token)]
```

`AccessWatch` evaluates every triple once at creation and reports nothing for that baseline. After each non-empty
batch it re-checks every triple with `at_least=batch.next` and yields the ones whose answer changed. Its names are
the application's own, so erasing a name does not remove it from a watch the application holds.

## 6. as_of (D192 to D195, D197, D198, D201, D202)

```
as_of(when, timeout, max_memory_mb):
  no bucket (no shipping, not a follower)                       -> -10
  writer: await_shipped(token at call, timeout)                  -> -14 on timeout
  floor = newest erase LSN of the live kernel
  token target: lost -> -13; beyond what the node can read -> -4 after the follower's read wait
  time target: aware datetime, not in the future
  anchors = AnchorFinder.candidates(AnchorBounds(floor, target LSN or chain head, target micros))
            a time bound reads the 48-byte header of each candidate snapshot for its last commit_ts
  none -> -17 (retention, the newest erasure, or no anchor at or below the target)
  kernel = replica kernel; MemoryBudget limited to the view limit and charging the shared history budget
  load the snapshot (sha256 checked), replay chain objects (8 prefetched, verified as a follower does)
  stop at the target commit point: the token's LSN, or the last commit point with commit_ts <= t
  closure full pass; register under the parent; remember the parent's erased count
PastGraph: check, has_edge, degree, khop, common_neighbors, token, commit_time, stats; advance(later); close
  every read: if the parent's erased count grew, erase the new ids in the view's symbol table first
  writes, erase, checkpoint, wait_* -> -10
```

C: `nodus_open_as_of(parent, kind, epoch, lsn_or_micros, max_memory_bytes, wait_ms, position_out, status)` returns a
new handle; `nodus_past_advance(view, kind, epoch, lsn_or_micros, wait_ms, position_out)`; `nodus_destroy` closes a
view without a checkpoint. `position_out` receives epoch, LSN and commit micros.

## 7. Failure modes

| Codepath | Failure | Test | Handling | Caller sees |
|---|---|---|---|---|
| ring | wraps while a reader copies | concurrency test | count re-read after the copy; fall back | correct batch, from a later source |
| ring | commit path regression | `KernelWriteBench` | 10% gate | nothing |
| feed | cursor older than every source | unit, simulation I7 | | -17 |
| feed | cursor names a write lost in a takeover | takeover test, I7 | epoch history | -13 |
| feed | a segment is trimmed under an open reader | unit | reader dropped, chain tried | correct batch or -17 |
| feed | chain object unreadable or forged | chaos, trust test | verified as a follower does | -15 |
| feed | writer closed during a long poll | unit | the wait wakes | "graph is closed" |
| wire | caller buffer too small | unit, Python | sizes returned, retry | nothing |
| as_of | no shipping | unit | | -10 |
| as_of | writer's shipping lags past the timeout | unit | | -14 |
| as_of | target before the newest erasure or retention | unit, Python | no download | -17 |
| as_of | view exceeds its limit or the shared budget | unit | build freed | -3, parent untouched |
| as_of | snapshot hash or chain signature fails | trust test | | -15 |
| as_of | erase after the view was built | unit, Python grep | scrub before the next read | the name is unknown to the view |
| as_of | parent closed with views open | unit | views closed first | "graph is closed" on the view |
| AccessWatch | triple names an unknown type | Python | the check's own error | -6 at creation |

## 8. Test plan

```
ChangeRing            empty, one entry, wrap, binary search at both ends, commit-only publish, lazy allocation,
                      reader racing a writer (validation), fast path and batch path and replica path feed it
ChangeExtractor       every record type from ring, log and chain bytes gives the same entries
ChangeFeed            ring -> log -> chain boundaries, -17, -13, cursor ahead, soft limit with a large transaction,
                      long poll wakes and times out, erased name read as pseudonym, reader cache continue and expiry
ChangeWire            round trip, empty batch, non-ASCII names, buffer growth
AnchorFinder, replay  bounds by floor, LSN and time; stop inside an object; follower suites unchanged
PastViewBuilder       token and time targets, recent time on the writer (shipping wait), future time, -10, -17, -3,
                      -15, closure clean on return, advance, digest equals the writer's at that LSN
PastViews             parent close, scrub on read after erase, shared budget across two views
API                   C exports and Python changes, watch, AccessWatch, as_of, PastGraph; -17 in the error contract
simulation            I7 and I8 in ChainSimulation under dropped replies, takeovers, retention and cuts
chaos                 S3ChaosTest: a follower watcher and an as_of build through dropped replies
end to end            Python: revoke on the writer, AccessWatch on a follower yields "lost"; erase, then the feed
                      and an open past view show only the pseudonym
```

## 9. Configuration and stats

`Graph(watch_buffer_changes=1048576)`; `Shipping(history_max_memory_mb=None)` and `Follower(history_max_memory_mb=None)`
(JSON `"history": {"max_memory_mb": N}`). The stats document gains:

```
"watch":   {"buffer_changes": 1048576, "allocated": true, "oldest_lsn": 4012, "fallback_readers": 1,
            "expired": 0}
"history": {"views": 1, "used_bytes": 41943040, "limit_bytes": 0}
```

A view's own stats add `"as_of": {"epoch": 3, "lsn": 812, "commit_micros": ..., "snapshot_lsn": 700,
"replayed_objects": 4, "replayed_records": 9120, "build_ms": 310}`.

## 10. Benchmarks

`KernelWriteBench` on `main` and on the branch with a watcher attached (gate 10%). `ChangeFeedBench`: changes read
per second from the ring and from the local log. `AsOfBench`: build time against snapshot size and replay distance.
Results go to `bench/baseline/watch-windows-dev.txt`.

## 11. Sequencing

| # | Milestone | Verify |
|---|---|---|
| M1 | `ChangeRing` and its kernel hooks, `ChangeExtractor`, sources, `ChangeFeed` with the reader cache, `ChangeWire`, -17, C `nodus_changes`, Python `changes`, `watch`, `AccessWatch`, stats | unit, concurrency, Python end to end, write benchmark gate |
| M2 | `AnchorBounds`, stop LSN, `RecordSlice.between`, `TargetResolver`, `PastViewBuilder`, `PastViews`, shared budget, C `nodus_open_as_of` and `nodus_past_advance`, Python `as_of` and `PastGraph`, stats | unit, follower suites, Python end to end with erasure |
| M3 | Simulation I7 and I8, S3 chaos cases, benchmarks, README, CHANGELOG, decisions, TODOS | CI, contention gate |

## 12. NOT in scope

- Lookup APIs and permission-level deltas maintained by the engine (D188; TODOS).
- as_of beyond the bucket or before the newest erasure, from the Iceberg table (D192; TODOS).
- Automatic or non-pausing checkpoints (D202; TODOS).
- Server-side filters on the feed (by object type); applications filter the batch.
- Push delivery through callbacks across the C boundary; the feed is pulled (6A).
- The agent-memory API, accepted in the CEO plan but scheduled in no build step (TODOS).

## 13. What already exists

| Need | Existing | Reused |
|---|---|---|
| Cursor and lost writes | `Token`, `EpochHistory.isLost`, `TupleStore.requireFresh` | yes |
| Apply hook on every path | `RecordApplier.apply`, `ReplicaWriter` | yes, one hook plus the fast path |
| Local log reads | `LogTailReader` | yes, as a fallback source |
| Verified chain reads | `ChainFetch`, `ChainCursor` | yes |
| Anchor search and bootstrap | `AnchorFinder`, `ChainReplay`, `SnapshotDownloader`, `RecordSlice` | yes, with bounds (D198) |
| Read-only kernel | `GraphKernel.openReplica`, `KernelReplicaSink` | yes, as the past view |
| Read exports | `nodus_check`, `nodus_has_edge`, `nodus_khop`, `nodus_token` | yes, unchanged on a view handle |
| Binary wire | `TupleWire` | same shape for `ChangeWire` |
| Memory limits | `MemoryBudget`, -3 | yes, plus a shared history budget |
| Erasure state | `ErasedSymbols`, `SymbolTable.erase` | yes, for the floor and the scrub |
| Digest | `GraphDigest` | yes, for I8 |
