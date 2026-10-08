# Engineering review: the authorization wedge

Status: decisions D58 to D87 accepted on 2026-10-06. Implementation scope is Build Step 1 (section 14).
Source plan: [`relationship-lakehouse.md`](relationship-lakehouse.md). Where this document and the
source plan disagree, this document wins (see its amendments section).

This document freezes every contract that becomes permanent once a customer writes data: the
tuple shape, the log and snapshot formats, the schema record, the object-storage layout, the
commit and signing protocol, the lake table and the token semantics. Code is staged; formats are
not. Each later build step gets its own short review against these contracts. Build Step 2 is reviewed in
[`engineering-review-build-step-2.md`](engineering-review-build-step-2.md) and Build Step 3 in
[`engineering-review-build-step-3.md`](engineering-review-build-step-3.md).

The review ran in two passes. The first pass produced D58 to D76. An independent adversarial
review of the first draft found eleven defects, three of which would have lost customer data;
the fixes are D77 to D87.

---

## 1. First principles

A permission check answers one question: does a chain of facts connect a subject to an object
through a relation, given the rules? Everything here follows from four physical facts.

1. **A check must not leave the process.** A network hop costs 100 us to 1 ms. A probe into
   resident memory costs tens of nanoseconds.
2. **History must be readable by engines we do not write.** Auditors use SQL, and Iceberg on
   object storage is the format every engine reads.
3. **One ordered log is the only source of truth.** The DRAM graph, the follower copies, the lake
   table and every time-travel replay are deterministic functions of a prefix of that log.
4. **Object storage is slow, has no locks, but has compare-and-swap.** S3 conditional writes
   (`If-None-Match` since August 2024, `If-Match` since November 2024) give one linearization
   point per object key and nothing else.

Two consequences shape the whole design:

- The pair **(epoch, LSN)** is the single coordinate of the system. It orders commits, it is the
  consistency token, a follower's position, a watch cursor, a lake watermark and the axis of time
  travel.
- Different consumers want different shapes of the same log. Followers want small, frequent,
  verifiable updates. SQL engines want large, infrequent commits. So the log ships twice from one
  source: as signed raw segments for followers, and as a periodically committed Iceberg table for
  SQL (D83).

---

## 2. Decision log

| ID | Issue | Decision |
|---|---|---|
| D58 | Review scope | Freeze all permanent contracts now; implement Build Step 1 only |
| D59 | Tuple shape | `(object, relation, subject, subject_relation)`; subject relation 0 means a direct subject |
| D60 | Policy language | Union, computed userset, tuple-to-userset; schema is a versioned canonical document. Intersection and exclusion are a later schema version |
| D61 | Kernel layout | Reverses CEO 13A. The 64-bit key packs relation, subject relation and id; tables are not per relation. Refined by D85 |
| D62 | Log shape | One log of typed variable-length records, CRC32C; `symbols.nodus` is absorbed |
| D63 | Atomicity | `TXN_COMMIT` records plus an `AUTOCOMMIT` flag; every reader applies whole transactions only |
| D64 | Ordering | Dense u64 LSN, `EPOCH` as its own record, monotone `commit_ts`. Refined by D79 |
| D65 | Failover | No automatic failover; audited manual takeover; per-write durability `LOCAL` or `LAKE` |
| D66 | Follower freshness | Default wait 1 s; documented visibility budget; fully consistent reads at the writer |
| D67 | Code shape | `TupleStore` layer, one `GraphKernel.apply(Txn)`, `LogStore` replaces `Persistence` |
| D68 | Lake schema | Denormalized typed string columns, `day(commit_ts)` partitions, LSN sort. Refined by D83 (one table) |
| D69 | Erasure | `ERASE` record, pseudonym, signed redaction; opaque ids recommended. Refined by D84 |
| D70 | Tamper evidence | Signed hash chain with content hashes; follower anti-rollback. Refined by D83, D84 |
| D71 | Recovery | Torn tail truncates; mid-log corruption refuses to open. Lake repair dropped by D80 |
| D72 | Format tests | Golden binary corpus, parser fuzzing, cross-version CI job |
| D73 | Crash tests | Fault-injecting channel; crash at every write and force prefix |
| D74 | Interner | Off-heap, charged to `MemoryBudget`, seqlock reads, no 2 GB cap. Supersedes D47 |
| D75 | Parallel reads | Lock-free reads per handle; volatile closed flag; per-thread scratch. Closes D51 |
| D76 | Check evaluator | Iterative, allocation-free, cycle-safe; depth limit 32 with `NodusCheckDepthError` |
| D77 | Snapshot v3 | A complete state image: config, schema and id table, symbols with erased flags, salt, epoch history, adjacency |
| D78 | Upgrade ordering | Copy to `pre-v2/`, tripwire first, install, `FORMAT`, then delete; ambiguous directories are refused |
| D79 | Tokens | Token is `(epoch, lsn)`; an `EPOCH` record on every writer open and takeover; `NodusTokenLostError` |
| D80 | Torn vs corrupt | A persisted forced-through mark decides; segments forced before rolling; no lake repair |
| D81 | Relation ids | Append-only `(type, relation) -> id` table inside every `SCHEMA` record; ids never reused |
| D82 | CAS handling | On 412, read and compare; fence only on a higher epoch; the takeover's first commit is `EPOCH` only |
| D83 | Two tiers | Signed raw segments on object storage for followers; one Iceberg table with typed rows committed about every 60 s |
| D84 | Signed bootstrap | Snapshot references and redactions are signed chain entries; raw segments retained 7 days |
| D85 | Edge partition | Two table pairs: DIRECT and INDIRECT (usersets and tupleset relations) |
| D86 | Write ordering | Validate, append and fsync under the writer monitor; the seqlock covers only memory changes |
| D87 | fsync failure | `NodusIndeterminateError`; the log is poisoned until reopen; recovery decides the outcome |

Corrections to the brief, verified against code or sources:

- A Leopard check is not constant time; `MEMBER2GROUP(u) ∩ GROUP2GROUP(G) != ∅` costs
  O(min(|A|, |B|)) probes (Zanzibar, USENIX ATC 2019).
- A 50 ms stale-read wait cannot work on an object-storage follower (section 9).
- Magic bytes belong in file headers, not in each record.
- Signing Iceberg metadata alone does not cover data files (section 8).

---

## 3. Tuple model and kernel key (D59, D61, D85)

```
object#relation@subject[#subject_relation]
document:readme#viewer@user:alice                 direct subject, subject_relation = 0
document:readme#viewer@group:eng#member           userset subject
```

### 3.1 Key

```
 63  62           47 46            31 30                         0
+---+---------------+----------------+----------------------------+
| 0 | relation u16  | subj_rel u16   | id (31 bits)               |
+---+---------------+----------------+----------------------------+
outgoing table, indexed by object: id = subject
incoming table, indexed by subject: id = object
```

### 3.2 Two table pairs (D85)

| Pair | Holds | Used by |
|---|---|---|
| DIRECT | tuples with `subject_relation = 0` whose relation is not a tupleset relation | the direct probe |
| INDIRECT | userset tuples (`subject_relation != 0`) and tuples of tupleset relations (the left side of `->` in some permission) | the fallback scan |

The schema decides which pair a relation's tuples go to. A schema change that turns a relation
into a tupleset relation moves its existing tuples between pairs inside the same transaction as
the `SCHEMA` record.

### 3.3 Invariants

- **K1** `pack(0, 0, id) == id` for `0 <= id <= MAX_NODE_ID` (`2^31 - 10`). Untyped graphs use
  relation 0 in the DIRECT pair, so every v1 graph is a valid v2 graph without re-keying.
- **K2** Relation id 0 is reserved for untyped graphs. Typed relation ids are 1 to 65535 and come
  from the schema's id table (D81). Subject relation 0 means a direct subject.
- **K3** A direct check is one probe: `DIRECT.outgoing.contains(object, pack(rel, 0, subject))`.
- **K4** The fallback scans only `INDIRECT.outgoing(object)` and filters by relation bits.
- **K5** Traversal masks the id with `key & 0x7FFF_FFFF` before indexing `visited`; today it
  casts `(int) neighbor` (`KHopTraversal.java:55`).

Memory is 32 bytes per node for the two pairs, independent of the number of relations.

---

## 4. Local log and snapshot formats (D62 to D64, D77 to D81, D87)

### 4.1 Directory layout

```
graph-dir/
  FORMAT        "nodus-format 2\n"; written last by create or upgrade (atomic rename)
  nodus.wal     tripwire: a 16-byte v1-style header carrying version 2
  log/{base_lsn:020d}.nlog    segments, rolled at 64 MiB; a segment is forced before the next is created
  log/FORCED    forced-through mark (section 4.6)
  snapshot.bin  snapshot v3 (section 4.8)
  lock          directory lock (unchanged)
```

Segments exist because trimming deletes a prefix of the log. Whole segments whose last LSN is at
or below the trim watermark are deleted without rewriting live data.

### 4.2 Segment header (32 bytes, big-endian)

| Offset | Size | Field | Rule |
|---:|---:|---|---|
| 0 | 4 | magic | `0x4E4F4455` ("NODU"), as in v1 |
| 4 | 2 | version | `2` |
| 6 | 2 | flags | `0`; any other value is refused |
| 8 | 8 | created_micros | informational |
| 16 | 8 | base_lsn | LSN of the first record |
| 24 | 4 | header_crc32c | CRC32C over bytes 0 to 23 |
| 28 | 4 | reserved | `0` |

### 4.3 Record envelope

Records are 8-byte aligned; integers are big-endian.

| Offset | Size | Field | Rule |
|---:|---:|---|---|
| 0 | 4 | len | total bytes incl. envelope, padding and CRC; multiple of 8; 32 to 16 MiB |
| 4 | 1 | type | section 4.4; an unknown type is corruption, never skipped |
| 5 | 1 | flags | bit 0 `AUTOCOMMIT` (tuple records only); other bits must be 0 |
| 6 | 2 | reserved | must be 0 |
| 8 | 8 | lsn | section 4.5 |
| 16 | len-20 | payload and zero padding | per type |
| len-4 | 4 | crc32c | over bytes 0 to len-5 |

### 4.4 Record types

| Type | Name | Payload (offsets from record start) | Size |
|---|---|---|---|
| `0x01` | GRAPH_CONFIG | 16 key_kind u8 (1 integer, 2 string), 17 reserved [7] | 32 |
| `0x10` | TUPLE_ADD | 16 object u32, 20 relation u16, 22 subject_relation u16, 24 subject u32 | 32 |
| `0x11` | TUPLE_REMOVE | as TUPLE_ADD | 32 |
| `0x10`/`0x11` with AUTOCOMMIT | single write | as above, then 28 commit_ts_micros u64 | 40 |
| `0x20` | SYMBOL | 16 id u32, 20 byte_length u32, 24 UTF-8 bytes | align8(28+n) |
| `0x30` | SCHEMA | 16 schema_version u32, 20 sha256 [32] of the document, 52 doc_length u32, 56 id_count u32, 60 id table entries (relation id u16, type_symbol u32, name_symbol u32, flags u16: bit0 membership, bit1 tupleset, bit2 retired), then the UTF-8 document | variable |
| `0x40` | TXN_COMMIT | 16 first_lsn u64, 24 record_count u32, 28 reserved u32, 32 commit_ts_micros u64 | 48 |
| `0x50` | EPOCH | 16 epoch u64, 24 writer_key_id u32, 28 reserved u32, 32 handoff_lsn u64 (last durable LSN of the previous tenure) | 48 |
| `0x60` | ERASE | 16 symbol id u32, 20 reserved u32, 24 pseudonym [24] ("erased:" + 16 hex, NUL padded) | 56 |

Ids are u32 on disk because `MAX_NODE_ID = 2^31 - 10` (`NodeIds.java:5`). Type and relation
names in the schema id table are symbols, so they share the interner and the `SYMBOL` records
that precede the `SCHEMA` record in its transaction.

### 4.5 Ordering rules

- **L1** LSN starts at 1 and increases by exactly 1 per record. No gaps within a tenure.
- **L2** A transaction is the records `first_lsn .. first_lsn + record_count - 1`, contiguous,
  followed by its `TXN_COMMIT`. Only tuple records may carry `AUTOCOMMIT`. `GRAPH_CONFIG`,
  `SYMBOL`, `SCHEMA`, `EPOCH` and `ERASE` always sit inside a transaction.
- **L3** A `SYMBOL` record precedes, in the same transaction, the first record using its id.
- **L4** A commit point is a `TXN_COMMIT` or an `AUTOCOMMIT` record. Every reader (recovery,
  shipper, follower, as_of) changes state only at commit points.
- **L5** `commit_ts = max(wall_clock_micros, last_commit_ts + 1)`; monotone in LSN order and
  persisted through the snapshot.
- **L6** Every writer tenure starts with a transaction `[EPOCH, TXN_COMMIT]`. A new tenure begins
  on every open of a durable graph (D79) and on every takeover. `handoff_lsn` is the last LSN the
  previous tenure made durable; the new tenure continues at `handoff_lsn + 1`. LSNs above a
  tenure's handoff point that were acknowledged in ASYNC mode and lost are never valid again.
- **L7** The token returned by a write is `(epoch, lsn)` of its commit point.
- **L8** `GRAPH_CONFIG` claims the key kind once. A later `GRAPH_CONFIG` with a different kind is
  refused before journaling.

### 4.6 Recovery and the forced-through mark (D71, D80, D87)

`log/FORCED` holds `(magic, segment base_lsn u64, byte offset u64, crc32c)`. After a group-commit
fsync succeeds, the writer records the forced position in memory, and a background step rewrites
and forces `log/FORCED` at most every 100 ms. Every byte below the mark is known to have been on
disk. The mark lags the truth by at most 100 ms and never runs ahead of it.

```
open:
  read FORCED (absent or bad CRC -> mark = start of the oldest segment)
  scan records from the oldest segment
  at the first bad record R (bad len, bad CRC, unknown type, bad header):
     position(R) <  mark  -> MID-LOG CORRUPTION -> NodusCorruptLogError(segment, offset, lsn)
                             operator may pass force_truncate=true to cut there, which is logged
     position(R) >= mark  -> TORN TAIL -> truncate to the last commit point, report bytes and
                             discarded records; delete later segments (they were never forced)
  records after the last commit point are discarded in every case
  write the new tenure's [EPOCH, TXN_COMMIT] (L6)
```

Why a mark and not a content scan: after a power cut, unforced page-cache writes reach disk out
of order, so a zeroed block can precede a later persisted block in a legitimate torn tail.
Content scans also trust user bytes, and a crafted `SYMBOL` string can contain a valid-looking
envelope. The mark depends on neither.

Residual risk, accepted and documented: a bit flip inside the last 100 ms of forced data,
followed by a crash before the mark catches up, is classified as a torn tail.

fsync failure (D87): the write that waited gets `NodusIndeterminateError`. The log is poisoned:
every later write fails until the graph is reopened, and recovery decides whether the record
survived. Retrying fsync after a failure is not safe on Linux, so the writer never retries.

### 4.7 Version detection and upgrade (CEO 16A, D78)

| Directory has | Meaning |
|---|---|
| `FORMAT` = 2 | v2; open normally |
| no `FORMAT`, `nodus.wal` v1, no `pre-v2/`, no `.upgrade/` | v1; open refused with `NodusUpgradeRequiredError` |
| no `FORMAT`, and `nodus.wal` v2 or `pre-v2/` or `.upgrade/` present | interrupted upgrade; `upgrade()` resumes, open refuses |
| no files at all | new graph, created as v2 |

A v1 build opening a v2 directory reads `snapshot.bin` first (`RecoveryManager.java:42-44`), which
v1 rejects as an unknown snapshot version, and otherwise `nodus.wal`, which v1 rejects with
"unsupported log version 2" (`WalFormat.java:40-42`). It never sees an empty directory.

`upgrade(dir)`:

1. Take the lock. Check free space for a full copy. Recover the v1 graph through the v1 code
   (snapshot, WAL, `symbols.nodus`).
2. Copy every v1 file into `pre-v2/` and force the copies and the directory. Never move.
3. Atomically replace `nodus.wal` with the tripwire. From here a v1 build refuses the directory.
4. Build the v2 files in `.upgrade/`: segment 1 holding `[GRAPH_CONFIG, EPOCH(1), SYMBOL*,
   TXN_COMMIT]`, and `snapshot.bin` v3 at that LSN. Edge keys are unchanged (K1). Force all.
5. Move the v2 files into place, then write `FORMAT` by atomic rename. `FORMAT` is the commit
   point of the upgrade.
6. Delete the v1 leftovers (`symbols.nodus`) and `.upgrade/`. Keep `pre-v2/` until
   `upgrade_cleanup()` is called.
7. Reopen and compare the edge digest and the symbol count with step 1. A mismatch raises
   `NodusUpgradeError` and leaves `pre-v2/` in place.

Every prefix of these steps is exercised by the crash harness (D73). Rollback before step 5 is
`upgrade()` resuming or the operator restoring `pre-v2/`; after step 5 it is restoring `pre-v2/`.

### 4.8 Snapshot v3 (D77)

```
header (48 bytes): magic "NODS" | version 3 u16 | flags u16 | lsn u64 | epoch u64 |
                   last_commit_ts u64 | node_capacity u32 | section_count u32 | header_crc32c u32 | rsv u32
sections, in this order, each: [ kind u32 | length u64 | crc32c u32 | rsv u32 | body ]
  1 CONFIG          key_kind, graph flags
  2 SCHEMA          current schema record body (version, sha256, id table, document)
  3 SYMBOLS         count u32, then per id: flags u8 (bit0 erased) | len u32 | bytes
  4 SALT            32 bytes (erasure pseudonym salt; secret)
  5 EPOCH_HISTORY   count u32, then (epoch u64, first_lsn u64, handoff_lsn u64)
  6..9 ADJACENCY    DIRECT out, DIRECT in, INDIRECT out, INDIRECT in:
                    per node: degree u32, then packed keys u64
                    validation: unpack each key; id < node_capacity; relation known or 0
```

A snapshot alone reconstructs the complete state at its LSN. Every segment at or below
`snapshot.lsn` can therefore be deleted locally (subject to section 4.9). Version 2 snapshots are
read only by `upgrade()`. The v2 loader's checks `neighbor >= nodeCapacity`
(`SnapshotFile.java:234`) and `degree > nodeCapacity` (`:155`) reject packed keys, which is why v3
validates unpacked ids instead.

### 4.9 Trim watermark and backlog

```
trim_lsn = snapshot_lsn                                  object-storage shipping off
trim_lsn = min(snapshot_lsn, shipped_lsn)                shipping on (shipped = committed segment chain)
delete every local segment whose last LSN <= trim_lsn
backlog  = bytes of local segments above shipped_lsn
backlog >= 50% of cap        -> stats warning
backlog >= cap (default 1 GiB) -> new writes fail with NodusLogBacklogError(lsn range, bytes)
```

---

## 5. Schema (D60, D81)

A `SCHEMA` record carries one complete canonical document plus the relation id table. Versions
increase by one. Applying a schema is atomic.

```
schema 1
type user
type group { relation member: user | group#member }
type folder { relation viewer: user | group#member  relation parent: folder }
type document {
  relation parent: folder
  relation editor: user | group#member
  relation viewer: user | group#member
  permission view = viewer + editor + parent->view
}
```

- `relation r: T | T#rel` declares stored tuples and their allowed subject types.
- `permission p = a + b + c->p2` is a union of computed usersets and tuple-to-userset arrows.
- `&` and `-` are reserved; schema version 1 rejects them.
- Relation ids (D81): the id table is append-only. A new relation gets the next free id. A
  removed relation keeps its id with the `retired` flag and is never reused. A schema that retires
  a relation with live tuples is refused unless the same transaction removes those tuples.
- Flags: `membership` when the relation appears as `T#rel` in some subject list (Leopard input);
  `tupleset` when it appears on the left of `->` (D85 INDIRECT pair).
- Validation rejects unknown types or relations, disallowed subject types, and static cycles
  among permissions that do not pass through a stored relation.

---

## 6. Write and read paths (D67, D75, D86)

```
write(txn, durability) on the writer
  [writer monitor]
   1 validate: schema, ids, key kind, log not poisoned, tenure active
   2 [seqlock section A, microseconds] reserve memory for every op (MemoryBudget, interner, both pairs)
        refusal -> NodusMemoryError, nothing journaled
   3 LogStore.append(records + commit point); SYNC: fsync now, outside any seqlock section
        fsync failure -> poison, NodusIndeterminateError (D87)
   4 [seqlock section B, microseconds] mutate interner and both table pairs; publish applied (epoch, lsn)
  [release monitor]
   5 LAKE durability only: wait, without the monitor, until shipped_lsn >= lsn
   6 return token (epoch, lsn)
```

- SYNC: a change becomes visible to readers only after it is durable.
- ASYNC: the change is visible before the background fsync, as today.
- Bulk loads (`add_edges_from`) keep their prefix semantics as one autocommit record per edge.
- `LAKE` returns `NodusUnsupportedError` until the segment shipper exists (Build Step 2).

Reads take no monitor: a volatile `closed` check, a per-thread scratch buffer, the kernel and
interner seqlocks, then a copy into the caller's buffer. A read racing `close()` may return a
valid answer from just before close; native memory stays valid because `Arena.ofAuto()` frees
only unreachable memory. Python keeps `threading.local()` result buffers and holds its `RLock`
for writes and close only.

---

## 7. Lake table (D68, D83)

One Iceberg format-version-2 table, `nodus_log`, append-only, partitioned by `day(commit_ts)`,
sorted by `lsn`. One table means schema events and tuples share one commit and one order.

| Column | Type | Rule |
|---|---|---|
| lsn | long | record LSN |
| commit_ts | timestamptz | commit point's `commit_ts` |
| txn_lsn | long | LSN of the commit point |
| epoch | long | writer tenure |
| event | string | `add`, `remove`, `schema`, `epoch`, `erase` |
| object_type, object_id | string | split at the first `:`; `''` for non-tuple events |
| relation | string | |
| subject_type, subject_id | string | |
| subject_relation | string | `''` for a direct subject |
| schema_version | int | |
| detail | string | schema document for `schema`; pseudonym for `erase`; `''` otherwise |

All columns are required (Spillway writes no nulls). Strings use dictionary encoding (D40).
Erased subjects appear as their pseudonym. Documented SQL views: `nodus_edge_log` (`event in
('add','remove')`) and `nodus_schema_log` (`event = 'schema'`).

The table is committed about every 60 seconds (configurable) from whole transactions only. Each
commit writes `metadata/version-hint.text` so catalog-less engines find the head, expires
snapshots older than the retention window, and records in a snapshot property
`nodus.commit.v1` a signed record of `{prev, lsn_first, lsn_last, chain_seq, data files with
sha256}` for long-term tamper evidence. Snowflake reads the table through `METADATA_FILE_PATH`
with refresh, and REST catalogs are a later option (TODOS).

---

## 8. Object storage protocol (D65, D82 to D84; Build Steps 2 and 3)

### 8.1 Layout

```
s3://bucket/prefix/
  _nodus/chain/{seq:020d}.obj      signed chain objects; THE commit point for followers
  _nodus/snapshots/{lsn:020d}.nsnap snapshot v3 files, referenced by signed chain objects
  _nodus/epoch/{E:020d}.json        tenure claims
  iceberg/data/*.parquet, iceberg/metadata/*    the nodus_log table
```

### 8.2 Chain objects

```
header  magic "NCHN" | version u16 | kind u8 | rsv u8 | seq u64 | epoch u64 | writer nonce u64 |
        prev_sha256 [32] | key_id u32 | rsv u32
kind 1 RECORDS       lsn_first u64, lsn_last u64, then the exact v2 record bytes of whole transactions
kind 2 SNAPSHOT_REF  snapshot path, sha256, lsn
kind 3 REDACTION     list of (seq, old sha256, replacement path, new sha256)
trailer sha256(header || body) [32] | ed25519 signature [64]
```

`prev_sha256` links each object to the previous one. Crypto is the JDK's built-in Ed25519.

### 8.3 Commit (writer, epoch E, next seq S)

```
before each commit: HEAD _nodus/epoch/{E+1}.json -> exists: FENCED
PUT _nodus/chain/{S}.obj  If-None-Match: *
  200            -> committed; shipped_lsn = lsn_last
  412 or timeout -> GET {S} and compare (epoch, writer nonce, prev):
                      ours                      -> committed
                      foreign, epoch >  E       -> FENCED (NodusWriterFencedError on every write)
                      foreign, epoch <  E       -> only possible during a takeover: re-bootstrap from
                                                   the new head, rebase, retry at S+1
                      absent after timeout      -> retry the same PUT
```

Upload cadence is about every 100 ms or 1 MiB, whichever comes first. `LAKE` durability waits
for this commit.

### 8.4 Follower verification and bootstrap (D84)

1. Bootstrap from the newest `SNAPSHOT_REF` chain object that is at or after the newest
   `REDACTION`. Verify the signature, then the snapshot's sha256.
2. For each next chain object: verify the signature, `prev`, `seq = last + 1`, and
   `lsn_first = applied_lsn_tail + 1`; reject epochs lower than the last applied `EPOCH`.
3. An object superseded by a signed `REDACTION` is accepted by its replacement hash.
4. Apply whole transactions; persist `(seq, applied epoch and lsn)`; never accept a head below
   the persisted `seq` (anti-rollback).

Any failure moves the follower to STALLED with the reason in `stats()`.

Retention: chain objects and snapshots older than 7 days (configurable) are deleted by the writer
once a newer signed `SNAPSHOT_REF` exists. The Iceberg table is the long-term history.

### 8.5 Takeover (D65, D82)

```
operator: nodus takeover --epoch E+1 --key K
 1 PUT _nodus/epoch/{E+1}.json If-None-Match: *   (claim; fails if already claimed)
 2 bootstrap from the chain head
 3 first commit: a RECORDS object holding only [EPOCH(E+1, K, handoff = head lsn_last), TXN_COMMIT],
   retried until it lands, rebasing past any last commit of the old writer
 4 report: "LSNs above <handoff> acknowledged by the old writer and never shipped are abandoned;
   run `nodus salvage <old-dir>` on the old disk to list them"
old writer: fails its next HEAD or PUT -> FENCED. Exposure window: at most one upload interval
plus the S3 timeout.
```

---

## 9. Tokens, freshness and states (D66, D79)

```
visibility on a follower = wait for the next segment upload (<= 100 ms)
                         + PUT (30 to 100 ms) + follower poll (<= 100 ms) + GET (20 to 100 ms)
                         -> typically 200 to 300 ms, worst case about 500 ms
```

| Call | Where | Rule |
|---|---|---|
| `check(...)` | any | current state |
| `check(..., at_least=(e, n))` | writer | fresh, unless the token is lost |
| `check(..., at_least=(e, n))` | follower | wait up to `timeout` (default 1 s) for applied >= (e, n); else `NodusStaleReadError` |
| `check(..., fully_consistent=True)` | writer only | followers raise `NodusUnsupportedError` |

Token lost: a token `(e, n)` with `e` older than the applied epoch and `n > handoff(e)` names a
write that was never durable. It raises `NodusTokenLostError` on every node.

```
follower: BOOTSTRAPPING -> CATCHING_UP -> CURRENT <-> LAGGING
             ^                  | verification failure or gap
             +-- operator ---- STALLED
writer:   ACTIVE -> FENCED (terminal for the process)
          ACTIVE -> POISONED (fsync failure; reopen required)
```

---

## 10. Leopard index (CEO 4A, 14A; Build Step 4, design frozen)

- `MEMBER2GROUP(u)`: groups `g` with a stored tuple `g#member@u` (membership relations, D81 flag).
- `GROUP2GROUP(g)`: `g` plus every group nested in `g`, transitively.
- `u` is a member of `g` exactly when `MEMBER2GROUP(u) ∩ GROUP2GROUP(g) != ∅`, costing
  O(min(|MEMBER2GROUP(u)|, |GROUP2GROUP(g)|)) probes.
- Nesting child `c` under parent `p` adds `GROUP2GROUP(c)` to every ancestor of `p`. Past
  `W_max` (default 4096) insertions, the affected ancestors become DIRTY.
- Removing a nesting edge recomputes the affected ancestors from their children; past `W_max`
  they become DIRTY.
- Checks touching a DIRTY group use the D76 evaluator for that group, so answers stay exact.
- Rebuilds run on the writer in slices of at most `W_max` work between writes.
- Invariant **R1**: for every clean group, `GROUP2GROUP(g)` equals a naive BFS closure, checked by
  an oracle after every operation.

## 11. Erasure (D69, D84; Build Step 5)

```
erase(symbol):
  txn: TUPLE_REMOVE for every tuple touching the symbol, ERASE(id, pseudonym), TXN_COMMIT
  pseudonym = "erased:" + hex(sha256(salt || utf8))[0:16], salt from the snapshot SALT section
  interner maps id -> pseudonym and drops the bytes; the next snapshot stores the erased flag
  local: segments holding the SYMBOL record are trimmed once a newer snapshot exists
  chain:  objects within retention that hold the string are rewritten; a signed REDACTION lists them
  lake:   Iceberg copy-on-write of files holding the string, committed with a signed property
  snapshots holding the string are replaced and deleted; bootstrap starts after the REDACTION
```

The docs recommend opaque ids (UUIDs) for subjects.

## 12. Check evaluator (D76)

Iterative DFS over `(node, relation)` frames: a per-thread `int[]` stack and a generation-stamped
visited array (the `KHopTraversal` pattern). Zero allocation after warm-up. The direct probe (K3)
comes first, then the INDIRECT scan (K4). Depth limit 32 by default; reaching it raises
`NodusCheckDepthError`, never a silent `false`. Visited frames are skipped, so cycles terminate.

---

## 13. Error contract

C return codes and Python exceptions (all subclass `NodusError`):

| Code | Python | Meaning |
|---:|---|---|
| -1 | `NodusError` | generic failure |
| -2 | (lookup) | lookup failure |
| -3 | `NodusMemoryError` | memory limit; nothing changed |
| -4 | `NodusStaleReadError` | follower behind the token after the wait |
| -5 | `NodusCheckDepthError` | evaluation depth limit reached |
| -6 | `NodusSchemaError` | schema violation; nothing changed |
| -7 | `NodusLogBacklogError` | unshipped backlog at cap |
| -8 | `NodusWriterFencedError` | a newer tenure exists |
| -9 | `NodusCorruptLogError` | mid-log corruption below the forced mark |
| -10 | `NodusUnsupportedError` | not available in this mode or step |
| -11 | `NodusUpgradeRequiredError` | v1 directory; run `upgrade()` |
| -12 | `NodusIndeterminateError` | fsync failed; outcome decided at reopen |
| -13 | `NodusTokenLostError` | the token's write was never durable |

---

## 14. Build Step 1: sequencing

Each row is one or more commits, green on the full suite before the next starts. Refactors land
before features ("make the change easy, then make the easy change").

| # | Change | Decisions | Verify |
|---|---|---|---|
| 1.1 | Injectable `FileChannel` seam; every-prefix crash harness; golden corpus of today's v1 files | D72, D73 | harness reproduces `TornWriteRecoveryTest` |
| 1.2 | Off-heap lock-free interner, behaviour-preserving swap | D74 | oracle vs `HashMap`, concurrency stress, the 426 existing tests |
| 1.3 | `GraphKernel.apply(Txn)`; the four write methods delegate; I/O moved out of the seqlock | D67, D86 | existing suite, reader-spin test during SYNC writes |
| 1.4 | Key packing, two table pairs, `KHopTraversal` id mask | D61, D85 | property test over bit boundaries; K1 identity |
| 1.5 | v2 codec: envelope, record types, CRC32C | D62 to D64, D81 | golden v2 corpus, fuzz, round-trip |
| 1.6 | `LogStore` v2: segments, forced mark, recovery, trim, poisoning | D63, D71, D80, D87 | every-prefix crash tests; corruption below and above the mark; injected fsync failure |
| 1.7 | Snapshot v3, `FORMAT`, tripwire, `upgrade()` | D77, D78 | v1 corpus upgrades with equal digests; crash at every upgrade prefix; v1 build refuses v2 |
| 1.8 | Schema parser, id table, `SCHEMA` records, validation | D60, D81 | parser tests, id stability across edits, retire rules, cycle rejection |
| 1.9 | `TupleStore`: transactions, `(epoch, lsn)` tokens, tenures, durability levels, evaluator | D59, D63, D65, D76, D79 | differential test vs a naive recursive evaluator; token-lost after ASYNC crash |
| 1.10 | C ABI and Python API; lock-free reads per handle | D75 | parallel stress; every error code |
| 1.11 | Native perf ratio gate in CI | CEO 12A | fails when the CE 23 library is substituted |
| 1.12 | Docs: README, tracked `docs/decisions.md` (D1 to D87) | CEO 17A | |

### 14.1 Native performance gate (CEO 12A)

A CI job after the native build times 200,000 `add_edge` and 200,000 `has_edge` through the
native library and through the JVM classes on the same runner: the median of five runs after
warm-up. It fails when native is more than 5x slower per operation and prints both. The ratio
cancels runner speed. It would have failed the GraalVM CE 23 library (about 130x).

### 14.2 Expectations to measure (not claims)

- v2 records are 32 to 40 bytes against 24 in v1. CRC32C is a JDK intrinsic. Expected write
  throughput within 10 to 20% of v1.
- Seqlock write sections shrink from "until fsync" to microseconds, so reader tail latency during
  SYNC writes should drop from milliseconds to microseconds.

---

## 15. Failure modes

| Codepath | Failure | Test | Handling | Caller sees |
|---|---|---|---|---|
| append | fsync fails | injected fault | poison the log | `NodusIndeterminateError`, then refusal until reopen |
| recover | torn tail above the mark | every-prefix | truncate to the last commit point | opens; bytes reported |
| recover | corruption below the mark | bit-flip test | refuse | `NodusCorruptLogError` |
| recover | uncommitted transaction tail | every-prefix | discard | opens; count reported |
| recover | ASYNC loss, then reuse of LSNs | crash test | new tenure, handoff recorded | `NodusTokenLostError` for lost tokens |
| apply | memory limit mid-transaction | unit | reserve all first, journal nothing | `NodusMemoryError` |
| apply | unknown or retired relation | unit | rejected before journaling | `NodusSchemaError` |
| schema | edit adds a relation | unit | append-only ids | nothing; stored tuples unchanged |
| commit_ts | clock steps back | fake clock | max(clock, last + 1) | nothing |
| checkpoint then restart | symbols and schema | crash test | snapshot v3 holds them | names intact |
| upgrade | crash at any step | every-prefix | resume or refuse, never empty | `NodusUpgradeRequiredError` or resume |
| open | v2 directory, v1 build | cross-version CI | tripwire and snapshot version | v1 error, data untouched |
| read | during SYNC write | spin test | seqlock covers memory only | microsecond stall |
| check | cycle or deep chain | unit | stamped visited, depth limit | `NodusCheckDepthError` |
| check | deny on an object with 1M direct grants | perf test | INDIRECT scan only | fast deny |
| follower (Step 3) | forged object or snapshot | security test | signature and hash checks | STALLED, metric |
| follower (Step 3) | rollback of the head | security test | persisted seq | STALLED, metric |
| writer (Step 3) | lost PUT response | MinIO chaos | GET and compare | committed, no fence |
| writer (Step 3) | old writer returns after takeover | MinIO chaos | epoch HEAD, compare | old writer FENCED |

No row is silent with neither a test nor handling.

## 16. What already exists

| Need | Existing | Reused |
|---|---|---|
| Group commit, sync modes | `WalWriter` | yes, behind the v2 codec |
| Snapshot write and parallel load | `SnapshotFile` v2, D54 | yes, generalized to v3 sections |
| Lock-free reads | kernel seqlock | yes, also for the interner |
| 64-bit key sets | `NativeSparseSet`, `LowDegreeSlab` | yes, unchanged |
| Memory budget | `MemoryBudget` | yes, now also charged by the interner |
| Allocation-free traversal | `KHopTraversal` pattern | yes, for the evaluator |
| Parquet, Snappy, dictionary | `io.nodusdb.lake` | yes, Build Step 2 |
| Signatures | JDK Ed25519 | yes, no dependency |

## 17. NOT in scope (this review)

- Implementation of Build Steps 2 to 6: segment shipper and Iceberg projection, object-storage
  commits and followers, Leopard, erasure, watch and as_of. Their contracts are frozen above.
- Intersection and exclusion in the schema language (TODOS, P2).
- Automatic failover and leases (TODOS, P3).
- A writer-to-follower low-latency stream (TODOS, P3).
- The deterministic simulation harness (TODOS, P1, gate for Build Step 3).
- Decision log of checks (TODOS, P2), REST catalog commits (TODOS, P2), namespace sharding (P3).

## 18. Parallelization

| Step | Modules | Depends on |
|---|---|---|
| 1.1 harness | `kernel/wal`, tests | none |
| 1.2 interner | `kernel` (interner) | none |
| 1.3 apply(Txn) | `kernel` (GraphKernel) | none |
| 1.4 keys, pairs | `kernel` (adjacency, traversal) | 1.3 |
| 1.5 to 1.7 log, snapshot, upgrade | `kernel/wal` | 1.1, 1.4 for snapshot v3 adjacency |
| 1.8 schema | new `authz` package | none |
| 1.9 TupleStore | `authz` | 1.2 to 1.8 |
| 1.10 API | `capi`, `python` | 1.9 |
| 1.11 perf gate | `.github`, `python/benchmarks` | none |

Lanes: A = 1.1, 1.5, 1.6 (`kernel/wal`). B = 1.2, 1.3, 1.4 (`kernel`, sequential). C = 1.8
(`authz`). D = 1.11. Launch A to D in parallel worktrees. Merge B before A's snapshot work (1.7)
because both touch persistence wiring. Then 1.7, 1.9 and 1.10 in order.

## 19. Amendments to the CEO plan

| CEO decision | Status |
|---|---|
| 3A "epoch in LSN" | Replaced by dense LSN plus `EPOCH` records (D64) and `(epoch, lsn)` tokens (D79). Fencing per D82 |
| 4A Leopard | Unchanged; definitions in section 10 |
| 7A token wait | Default 1 s on followers (D66), not 50 ms |
| 8A signed commits | Refined: signed chain objects with content hashes, signed snapshot references, redaction (D83, D84) |
| 10A schema in the log | Unchanged; ids made explicit (D81) |
| 13A per-relation tables | Reversed: packed keys in two table pairs (D61, D85) |
| 1A watermark trim | Unchanged; the watermark is the shipped chain position (section 4.9) |
| 18A compaction | Largely absorbed: Iceberg commits every ~60 s; raw chain retained 7 days |
