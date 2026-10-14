# Engineering review: Build Step 5, erasure

Status: decisions D159 to D175 accepted on 2026-10-14. Implementation scope is Build Step 5 (section 11), built on
branch feat/authz-step5 in four milestones.
Contracts: [`engineering-review-authz-wedge.md`](engineering-review-authz-wedge.md) sections 4, 7, 8 and 11. Where
this document and those disagree about Build Step 5, this document wins (section 2 lists every amendment).

Erasing a name removes every tuple that touches it and replaces the name with a salted pseudonym everywhere nodusdb
keeps it: memory, the local log and snapshot, every follower, the bucket and the Iceberg table. The local erase is one
transaction and returns a token. The copies that live outside the process are scrubbed in the background, and
`wait_erased(token)` waits until every layer is clean.

---

## 1. First principles

1. **A name lives in seven places.** Memory (the symbol table), the local log segments, the local snapshot, every
   follower's memory, the bucket's chain objects, the bucket's snapshot objects, and the Iceberg data files with their
   manifest statistics. An erase is done only when all of them are clean.
2. **Only `SYMBOL` records and the symbol table hold a name.** Tuples, epochs and schema records hold ids. So the log
   is clean once every record at or below the `SYMBOL` record is gone, and the graph is clean once the symbol table
   entry is scrubbed.
3. **Signed history is not edited, it is cut.** Rewriting a hash-linked, signed object changes its fingerprint and
   breaks every reader that follows the link. Every record after the `ERASE` record is clean by construction, so the
   bucket is scrubbed by publishing a scrubbed snapshot after the erase and deleting everything before it.
4. **Status is derived, never trusted from memory.** The kernel counts erased symbols. The bucket and the lake record
   how many erased symbols they cover. A restart or a takeover compares the counts and finishes what is missing.
5. **The name being erased is never written to disk again.** The background jobs keep it in memory only, as a hint.

```
erase("user:alice")                                       writer
  [TupleEraser] remove every tuple touching the id (chunks) -> [ERASE(id, pseudonym), TXN_COMMIT] -> token
        |
        v  applied like any record: memory scrubbed on the writer and on every follower
  [ErasureRunner] background, one tick per layer, coverage compared with the kernel's erased count
        |-- local:  coalesced checkpoint at L >= erase LSN; log trimmed through L once shipped
        |-- bucket: object holding L+1 has no erased SYMBOL -> REDACTION(cut) -> delete older objects and snapshots
        '-- lake:   projector thread: rewrite matching data files -> overwrite commit -> expire older snapshots
  wait_erased(token): every configured layer covers the erase, or NodusErasurePendingError (-16)
```

## 2. Amendments to the frozen design

| Where | Text before | Text now | Decision |
|---|---|---|---|
| Wedge 11, chain | objects that hold the string are rewritten; a signed `REDACTION` lists them | an erasure cut: a scrubbed snapshot after the erase, a signed `REDACTION` cut entry, then every older chain object and snapshot object is deleted | D166 |
| Wedge 8.2, kind 3 | list of (seq, old sha256, replacement path, new sha256) | `erase_lsn u64, erased_count u32, first_kept_seq u64, snapshot_lsn u64` | D166, D167 |
| Wedge 8.4, step 3 | an object superseded by a `REDACTION` is accepted by its replacement hash | no object is ever replaced; a follower behind a cut stops as "fell behind retention" (D116) | D166 |
| Wedge 11, flow | one transaction removes every tuple and erases | removals in committed chunks, then `[ERASE, TXN_COMMIT]`; a repeated erase finishes an interrupted one | D164 |
| Wedge 13 | codes -1 to -15 | adds -16 `NodusErasurePendingError` | D170 |

## 3. Decision log

| ID | Issue | Decision |
|---|---|---|
| D159 | Scope | Every layer in Step 5, in four milestones on one branch, each green before the next: M1 local erase, M2 bucket cut, M3 Iceberg rewrite, M4 simulation, chaos, docs and benchmarks |
| D160 | Waiting | `erase(name)` commits the local erase and returns a token. Background jobs scrub the other layers with retries. `wait_erased(token, timeout)` waits for every configured layer; `stats()["erasure"]` reports each one |
| D161 | Iceberg time travel | The redaction commit expires every older snapshot and deletes their manifests and the replaced data files. Time travel after an erase reaches back to the erase; the rows themselves stay, with the pseudonym |
| D162 | Reuse | An erased id keeps its pseudonym forever. The same string written later gets a new id and starts with no tuples |
| D163 | Symbol scrub | The entry gets an erased flag (bit 63 of its metadata), its bytes are zeroed in place and it points to the pseudonym. The hash slot stays occupied so probe chains hold; a lookup never matches an erased entry, and a bucket rebuild leaves erased ids out. `resolve` runs under the symbol table's seqlock |
| D164 | Erase transaction | Removals of every tuple touching the id (both partitions, both directions, self tuples once) in committed chunks of at most 30,000, then `[ERASE(id, pseudonym), TXN_COMMIT]`. A name that does not exist is a no-op returning the current token. A symbol used by the schema is refused with `NodusSchemaError`. Followers and integer-keyed graphs refuse erase |
| D165 | Local scrub | The runner checkpoints once the oldest unscrubbed erase is `erasure_checkpoint_delay_s` old (default 60, 0 means at once); erases in the window share one checkpoint. Local is clean when the local snapshot is at or above the newest erase LSN and the log is trimmed through it |
| D166 | Bucket scrub | An erasure cut. After a checkpoint at `L` has been shipped and referenced, the runner reads the object holding `L+1` (D106). If it holds a `SYMBOL` record of an erased id, the cut waits for the next checkpoint. Otherwise the shipper commits a signed `REDACTION` cut entry and the runner deletes every chain object before the kept one and every snapshot object below `L`. Deletion repeats until done; retention honours the newest cut |
| D167 | Coverage | The kernel counts erased symbols (restart-safe: snapshot flags plus replay). Bucket coverage is the newest cut's `erased_count`, kept in a local file `erasure.coverage` (counts and LSNs, no names). Lake coverage is the `nodus.redaction.v1` property of the table's current snapshot. A layer is pending while its count is below the kernel's |
| D168 | Lake match | With the name in memory (the erasing process), files are pruned by manifest bounds and rows match the name. Without it (restart, takeover), every name in every file is hashed with the salt and compared with the pseudonyms of erased ids |
| D169 | Lake commit | One overwrite snapshot replaces the matching data files, with a signed `nodus.redaction.v1` property (`erase_lsn`, `erased_count`, removed and added files with sha256). The projector's buffered rows are scrubbed before its next commit. The job runs on the projector thread, so it never races an append |
| D170 | Timeout error | -16 `NodusErasurePendingError`, naming the pending layers and carrying the token |
| D171 | Code shape | Package `io.nodusdb.erasure`: `ErasureRunner`, `ErasureCoverage`, `ErasureStatus`, `BucketCut`. `SymbolTable.erase` in `kernel.symbols`, `Pseudonym` beside it, `TupleEraser` in `authz`, `LakeRedaction` in `projection`. Existing classes delegate |
| D172 | API | Python `Graph.erase(name) -> Token`, `Graph.wait_erased(token, timeout=None)`, `stats()["erasure"]`. C `nodus_erase` and `nodus_await_erased` |
| D173 | Followers | A follower applies `ERASE` like any record, which scrubs its memory. It refuses `erase`. A follower behind a cut stops with "fell behind retention" and rebuilds on restart (D116) |
| D174 | Salt | A durable graph uses the salt of its snapshot. An in-memory graph draws a random salt per kernel. Anyone who can read a snapshot can test a guessed name against a pseudonym; this is documented |
| D175 | Copies out of reach | Documented, not built: a salvaged old directory, `pre-v2/` until `upgrade_cleanup()`, bucket versioning or replication, backups, and freed disk blocks |

## 4. Symbol scrub (D163)

```
entry id: [ location u64 ][ meta u64 = hash32 | length << 32 | ERASED << 63 ]
erase(id, pseudonym):           inside the symbol table's write section
  zero the old bytes in the slab
  append the pseudonym to the slab; location -> pseudonym; meta -> hash(pseudonym) | len | ERASED
  the bucket slot keeps id + 1, so later probes still pass through it
lookup(bytes):  an ERASED entry never matches
resolve(id):    seqlock read; an erased id resolves to its pseudonym
rebuild:        erased ids are left out of the new bucket table
```

The pseudonym is `"erased:" + hex(sha256(salt || utf8))[0:16]` (23 bytes, stored NUL padded to 24 in the record).
Snapshot v3 already carries the flag (`SYMBOLS` section, bit 0): the writer sets it and stores the pseudonym, the
reader restores the entry as erased.

## 5. Bucket cut (D166, D167)

```
REDACTION body (kind 3): erase_lsn u64 | erased_count u32 | first_kept_seq u64 | snapshot_lsn u64

runner tick, bucket layer pending (kernel erased count > covered count):
  1 wait for a local checkpoint at L >= newest erase LSN that is shipped and referenced (SNAPSHOT_REF committed)
  2 K = object holding L+1; if K has a SYMBOL record of an erased id -> wait for the next checkpoint
  3 shipper commits REDACTION(erase_lsn, erased_count, K, L)            signed, via a ShipState mailbox
  4 delete chain objects with seq < K and snapshot objects with lsn < L  repeatable; under the retention lock
  5 erasure.coverage <- (erased_count, erase_lsn, K)                    atomic replace
```

Every record after `L` comes after the `ERASE` record, so it cannot hold a scrubbed name. Followers, restore and
takeover already bootstrap from the newest snapshot. Retention and the cut share one lock, and retention never keeps
an object below the newest cut.

## 6. Lake rewrite (D161, D168, D169)

```
projector tick, lake layer pending and projected LSN >= newest erase LSN:
  candidates = files whose bounds can hold a hinted name, or every file without hints
  for each candidate: read rows; a row matches when a name part is a hinted name or salted(name) is an erased pseudonym
  rewrite matching files with ("erased", 16 hex) in place of (type, id); keep the sort and the partition
  commit an overwrite snapshot with nodus.redaction.v1 (signed); expire every older snapshot
  delete the replaced data files and the manifests and manifest lists no snapshot references
```

## 7. Failure modes

| Codepath | Failure | Test | Handling | Caller sees |
|---|---|---|---|---|
| erase | more than 30,000 tuples | unit | committed chunks, then ERASE | token after the last chunk |
| erase | crash between chunks | crash test | name intact, some tuples gone | erase again finishes it |
| erase | schema symbol | unit | refused before journaling | `NodusSchemaError` |
| erase | follower or integer graph | unit | refused | `NodusUnsupportedError` |
| reader | lookup during the scrub | concurrency test | seqlock retry | old id or absent, never torn bytes |
| local | checkpoint fails | injected fault | retried next tick, stats error | pending |
| bucket | object holding L+1 holds the name | unit | wait for the next checkpoint | pending |
| bucket | crash after REDACTION, before deletes | crash test | deletes repeat on the next tick | pending until done |
| bucket | follower behind the cut | follower test | STALLED, restart rebuilds | stats reason |
| lake | crash before the overwrite commit | crash test | files written but unreferenced, orphan sweep | pending |
| lake | name hint lost (restart) | unit | salted full scan | pending longer |
| any | takeover mid erase | takeover test | counts compared on open | pending, then clean |
| wait | timeout | unit | | `NodusErasurePendingError` with layers |

## 8. Test plan

```
SymbolTable.erase       flag, zeroed bytes, lookup misses, resolve -> pseudonym, rebuild skips, seqlock readers
Pseudonym               fixed vectors, salt sensitivity
TupleEraser             object side, subject side, usersets, self tuple, chunking, idempotence, schema refusal
snapshot                erased flag round trip, a v3 file with erased symbols loads
kernel apply            writer, recovery replay, follower apply, reservation of pseudonym bytes
ErasureRunner           local coalescing, bucket cut steps, lake trigger, coverage file, restart and takeover
BucketCut               cut entry codec, straddling object, deletions under retention lock, retention honours cut
LakeRedaction           pruned and full scan, rewrite rows, overwrite commit, expiry, buffered rows scrubbed
API                     Python erase, wait_erased, stats, -16; C exports
simulation              erase actor in ChainSimulation: no erased name in the bucket once covered; followers converge
end to end              a Python test greps every file in the directory and the bucket for the name after wait_erased
```

The last test is the acceptance test of the whole step: after `wait_erased`, no byte sequence equal to the name exists
anywhere under the graph directory, the bucket or the Iceberg table.

## 9. Configuration and stats

`Shipping(erasure_checkpoint_delay_s=60)` and the same key for a graph without shipping. The stats document gains:

```
"erasure": {"erased_symbols": 3, "newest_erase_lsn": 812,
            "local": {"covered_lsn": 812, "pending": false},
            "bucket": {"configured": true, "covered_count": 2, "pending": true},
            "lake": {"configured": true, "covered_count": 2, "pending": true},
            "last_error": ""}
```

## 10. Sequencing

| # | Milestone | Verify |
|---|---|---|
| M1 | `Pseudonym`, `SymbolTable.erase`, snapshot flags, kernel apply and reservation, `TupleEraser`, `ErasureRunner` local layer, `ErasureStatus`, API and -16 | unit, crash, follower, Python grep of the directory |
| M2 | REDACTION codec, ShipState cut mailbox, `BucketCut`, retention honours cuts, `erasure.coverage`, takeover | unit, bucket grep, simulation erase actor |
| M3 | `LakeRedaction`, overwrite commit, expiry, buffered rows, coverage property | unit, pyiceberg read, lake grep |
| M4 | S3 chaos for the cut, benchmarks, README, CHANGELOG, decisions | CI, contention gate |

## 11. NOT in scope

- Copies outside nodusdb: salvaged directories, `pre-v2/`, bucket versioning, backups, disk blocks (D175).
- Erasing schema names (types and relations).
- Rewriting chain objects in place (rejected for D166).

## 12. What already exists

| Need | Existing | Reused |
|---|---|---|
| `ERASE` record | `RecordBatch.erase`, `RecordReader` | yes |
| Salt | snapshot v3 `SALT`, `DurableGraph` | yes |
| Erased flag on disk | snapshot v3 `SYMBOLS` flags | yes |
| Pseudonyms in the projection | `EdgeLogDecoder`, `StreamNames` | yes |
| Checkpoint and trim | `DurableStore.checkpoint`, `ShippingLogStore` | yes |
| Prefix deletion | `ChainRetention`, D106 | yes, plus the cut |
| Mailbox to the shipper | `ShipState` snapshot references | same pattern for the cut entry |
| Parquet read and write | `lake.parquet`, `DataFileWriter` | yes |
| Snapshot expiry | `IcebergTable.prepareExpiry` | extended to "all but current" |
