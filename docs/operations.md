# Operations

Running a durable graph for real: what is on disk, what survives a crash, how to copy the log to a bucket, how to run followers and take over from a dead writer, and how to erase a person, watch changes and look at the past. The runnable version is [`examples/05_ship_follow_watch.py`](../examples/05_ship_follow_watch.py).

Each section starts with the problem it solves, so you can skip what you do not need. A single process with a directory needs only [Durability](#durability) and [Upgrading](#upgrading). Everything else builds on a bucket.

| You want | Read |
|---|---|
| The graph to survive a restart or a crash | [Durability](#durability) |
| A copy of every change outside the machine | [Shipping the log](#shipping-the-log) |
| Read replicas, or a new writer after a failure | [Followers, restore and takeover](#followers-restore-and-takeover) |
| To remove a person for good | [Erasure](#erasure) |
| To react when something changes | [Watching changes](#watching-changes) |
| To see the graph as it was | [Time travel](#time-travel) |
| To open a directory from 0.1.0rc1 | [Upgrading](#upgrading) |

## Durability

A durable graph lives in one directory:

- `log/` holds the write-ahead log as segments of at most 64 MiB, named by the sequence number of their first record. Records are typed and variable length, each with a CRC32C. `log/FORCED` records how far the log is known to have reached the disk.
- `snapshot.bin` is the last checkpoint: a complete image of the graph, the schema, the symbols and the epoch history, in checksummed sections.
- `FORMAT` names the directory format, and `nodus.wal` is a short marker that makes older releases refuse the directory.
- `nodus.lock` holds a file lock, so a second process or a second handle cannot open the same directory.

**Writing.** A write is validated, its memory is reserved, and it is appended to the log. In `sync` mode the call then waits until the record is on disk. Only after that does the change appear in memory, so a reader never sees a write that a crash could undo. A duplicate add or a missing remove does nothing and writes nothing. A transaction is its records followed by a commit record, and recovery applies whole transactions only.

**Checkpoint.** A checkpoint forces the log, writes the snapshot to a temporary file, forces it, renames it into place, and deletes the log segments the snapshot covers. A crash between the rename and the deletion is safe: recovery skips every record the snapshot already holds. A checkpoint pauses writes while it runs (about 9 seconds for 30.6 million edges), so checkpoints are never automatic. Call `checkpoint()` on a schedule that suits you. Closing the graph checkpoints too.

**Recovery.** On open, the process deletes half-written temporary files, loads the snapshot, and replays the log from the first record after it. Where the log goes bad decides what happens. Damage in the part of the log known to have reached the disk means corruption, and the directory refuses to open with `NodusCorruptLogError`. Damage after that point is a torn tail from a crash: the log is cut back to its last commit and the cut bytes are reported. Every open then starts a new writer tenure, which is what lets a token name a write that was lost. If forcing the log fails, the write that waited gets `NodusIndeterminateError` and the log refuses further writes until the graph is reopened, because retrying a failed fsync is not safe on Linux.

The crash tests replay every prefix of the writes and forces of a run through a simulated disk, with torn writes (every byte for writes up to 64 bytes in the synchronous run, every 13th byte otherwise), and require recovery to return a prefix of what was acknowledged.

### What survives what

| Failure | `async` | `sync` (single calls) |
|---|---|---|
| The process is killed | Everything the OS has accepted survives. A write still in the buffer is lost | Every acknowledged write survives |
| The machine loses power | Up to the last 10 ms of writes can be lost | Every acknowledged write survives |

Call `sync()` or `checkpoint()` to force a point where everything is on disk.

### Real-data numbers

These come from the SNAP soc-Pokec graph, 30,622,564 directed edges, each phase in its own process with a 6 GB heap. The ingest rows are in `bench/baseline/durability-windows-dev.txt`. The checkpoint and recovery rows were measured on 2026-10-05 after the two-section snapshot (commit `974ba2d`), and the file still holds the earlier run (checkpoint 3.7 to 5.6 s on a 250 MB snapshot, recovery 22 s). Nothing here was re-run for 0.1.0.

| Phase | Result |
|---|---|
| In-memory ingest, batches of 1,048,576 | 1,252,289 edges/s |
| Durable ingest, asynchronous log | 1,038,823 and 1,112,830 edges/s across two runs |
| Durable ingest, synchronous log, batches | 1,167,272 edges/s |
| Durable ingest, one call per edge, synchronous | 203 edges/s (median 3.6 ms, p99 34 ms per call) |
| Checkpoint, 500 MB snapshot | 8.7 to 9.1 s |
| Recovery in a fresh process, snapshot plus 500,000 log frames | 2.8 s |
| Replay of 500,000 log frames alone | 0.13 s |

After recovery the degrees, in-degrees and 3-hop results match the state before the crash. The benchmark compares a digest of all of them.

Recovery at this size is not yet sub-second, which was the goal. The graph lives in native memory, and the time that remains is the snapshot load.

## Upgrading

A directory written by 0.1.0rc1 is refused with `NodusUpgradeRequiredError` until it is converted:

```python
import nodusdb

report = nodusdb.upgrade("/data/graph")        # report.performed, report.edges, report.symbols
nodusdb.upgrade_cleanup("/data/graph")         # delete pre-v2/ once you trust the result
```

`upgrade` keeps the original files in `pre-v2/`, writes the new files, and reopens the result to compare an edge fingerprint and the symbol count with the original before it reports success. If it is interrupted, run it again. It cannot be undone: rc1 refuses an upgraded directory.

Within 0.1.x the directory format does not change, so any 0.1.x release opens a directory written by another.

## Shipping the log

**The problem.** A directory on one disk is one failure away from gone. You want every change copied somewhere else, in order, in a form you can trust and other tools can read.

A durable graph can copy its log to a bucket as it writes. The bucket then holds every change in order, signed and hash-linked, readable without the graph running.

```python
from nodusdb import Graph, Shipping, Transaction, environment_credentials, generate_signing_key

generate_signing_key("signing.pem", "signing.pub")     # once; keep signing.pem private

shipping = Shipping.s3(
    "my-bucket", "eu-west-1", prefix="graphs/prod",
    key_file="signing.pem", key_id=1, public_key_file="signing.pub",
    credentials=environment_credentials(),             # or file_credentials(), static_credentials(...)
    iceberg=True,                                       # also project the log into an Iceberg table
)
with Graph(path="graph", sync_mode="sync", shipping=shipping) as g:
    g.apply_schema(SCHEMA)
    token = g.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
    print(g.stats()["shipping"]["lag_lsn"])             # 0 once the bucket holds everything
```

`Shipping.directory(path, ...)` ships into a local directory instead, which is what the tests and examples use. Any S3-compatible server works with `endpoint=` and `path_style=True`.

**What lands in the bucket.** Under the prefix, `_nodus/chain/` holds numbered objects. Each one carries a run of log records, an Ed25519 signature, and the SHA-256 of the object before it, so a reader can detect a gap, a reordering or a forged object. `_nodus/snapshots/` holds a copy of each checkpoint, and a snapshot reference in the chain says which one. `_nodus/epoch/` holds one small claim per writer tenure. With `iceberg=True`, `iceberg/` holds an Iceberg v2 table `nodus_log`, with one row per tuple change, names instead of ids, partitioned by day. pyiceberg, DuckDB and Spark read it.

NodusDB never trusts a key stored in the bucket. Every reader takes the public key from its own configuration. The bucket needs conditional writes (`If-None-Match`), which S3 supports, because the chain's safety rests on them.

**Durability `lake`.** A local write returns once the log has it. A `lake` write also waits until the object is in the bucket, 30 seconds at most (`timeout=`). On a timeout the write is still applied and logged locally, and `NodusShipTimeoutError.token` names it, so `wait_shipped(token)` can finish the wait later. Other threads keep using the graph during the wait.

**Opening.** The open checks the bucket before it takes any write. An empty bucket starts a chain with a snapshot of the graph. A bucket whose chain this directory can continue gets a new epoch, and the graph keeps shipping where the chain ends. A directory that is behind the chain, or that lost records the chain never received, is refused with `NodusWriterFencedError`. A store that ignores conditional writes is refused with `NodusUnsupportedError`. A second writer that claims a newer epoch fences the first: its writes fail with `NodusWriterFencedError` from then on.

**Outages.** A bucket that cannot be reached does not stop local writes at first. The unshipped part of the log grows, local trimming waits for it, and `stats()` reports `RETRYING` with the last error. At half of `backlog_cap_bytes` (1 GiB by default) the `nodusdb` logger warns. At the cap writes fail with `NodusLogBacklogError` until shipping catches up, then they resume by themselves.

**Operating it.** `stats()` returns a dictionary with the shipper phase, the shipped and applied positions, the lag, the backlog, the age of the last commit and snapshot reference, retention counters and the projection state. New events go to the `nodusdb` logger once each. Retention deletes chain objects and snapshots older than `retention_days` (7 by default), but never the part a restore or the projection still needs.

**Cost.** The shipper uploads at most once per `interval_ms` (100 ms by default) and only when there is something to send, so a busy writer makes about 864,000 PUT requests a day and an idle one makes none. A larger interval means fewer requests and a longer `lake` wait. A local write costs about what it did without shipping: `python/benchmarks/shipping_ratio.py` measures `add_tuple` with and without shipping in one run, and CI fails above a ratio of 3. The gate that shipping adds to one log append costs about 37 ns against a local write of about 42,000 ns through Python (`bench/baseline/MACHINE.md`).

## Followers, restore and takeover

**The problem.** One writer is a single point of failure, and you may want read replicas near your users. The chain in the bucket holds everything a graph needs, so another process can rebuild the graph from it, follow it, or continue it as the writer. All of it verifies every object against a public key from its own configuration.

```python
from nodusdb import Follower, Graph, environment_credentials, restore, salvage

follower = Follower.s3(
    "my-bucket", "eu-west-1", prefix="graphs/prod",
    public_key_file="signing.pub", key_id=1,
    credentials=environment_credentials(),
    read_wait_ms=1000,          # how long a check with a token waits for the follower to catch up
    max_staleness_ms=30_000,    # optional: refuse reads once the head was not confirmed for this long
    state_directory="/var/lib/nodus/follower",   # optional: remembers the furthest position it saw
)

with Graph(follow=follower) as replica:             # a read-only copy, rebuilt from the bucket
    replica.wait_until_current()
    replica.check("document:a", "view", "user:alice", at_least=token)   # waits for the write the token names
    print(replica.stats()["follower"]["phase"])      # CURRENT, CATCHING_UP, LAGGING, STALLED

restore(follower, "recovered-graph")                 # a durable graph directory, ready for Graph(path=...)
```

**A follower** downloads the newest snapshot it can verify, loads it, and then applies the chain object by object. Each transaction becomes visible whole. Writes raise `NodusUnsupportedError`. Reads raise `NodusStaleReadError` until the snapshot has loaded. By default a follower keeps answering from its last verified state when the store is down, and `stats()` says why it is behind. With `max_staleness_ms` it refuses instead. A check with `at_least=` waits up to `read_wait_ms` for the follower to reach the token, then raises `NodusStaleReadError`, or `NodusTokenLostError` when the write was acknowledged but a takeover dropped it.

**What stops a follower.** A bad signature, an unknown signer, a broken link, a fork or a bucket that went backwards stops it with the reason in `stats()["follower"]["stall_reason"]`, and `restore()` and a takeover raise `NodusChainTrustError`. A follower that falls so far behind that retention deleted the objects it needs stops too ("fell behind retention; restart to rebuild"). It keeps serving what it has, and a restart rebuilds it from the newest snapshot. With `state_directory`, the follower writes a small checksummed file of the furthest object it has seen, so a restart against a rolled-back bucket is refused. A damaged file stops the start with `NodusCorruptLogError` naming it, and deleting it accepts the loss.

**Restore.** `restore()` writes the directory into a sibling `.restore-tmp` and renames it into place when it is complete, so a crash never leaves something that looks like a graph. The target must not exist or must be empty.

**Takeover.** To replace a dead writer, open a new directory with `Graph(path="new-graph", shipping=shipping, takeover=True)`. It claims the next epoch, which fences the old writer on its next commit, restores into the new directory, opens it as the writer, and waits until the chain holds its first object. `graph.takeover_report` has the epochs and the last LSN carried over. A `lake` write that returned before the takeover is in the new graph. A `local` write the old writer had not shipped is not, and `salvage()` lists it.

**Salvage.** `salvage(follower, "old-graph")` reads the old writer's directory without writing to it, finds the last LSN the chain keeps, and returns the transactions above it with the names of what they changed. It refuses a directory another process has open. The handoff is `provisional` until a later writer has taken over. A checkpoint can have trimmed part of the range from the log, and `unavailable_through_lsn` says so.

Nothing decides by itself that the writer is dead. Failover is manual: you run the takeover.

**Cost.** A follower makes one GET per `poll_interval_ms` (100 ms by default) when idle and one LIST per second. On the development laptop it applies about 2.5 million tuples a second on one core (`FollowerApplyBench`), and the check path pays nothing measurable for seeing a schema migration (`CheckModelBench`).

## Erasure

**The problem.** A privacy request says to delete a person. In a system that keeps a log, a snapshot, a chain of signed objects and a table of history, "delete" has to reach every copy, and a signed chain cannot be edited.

`erase` removes a subject or an object for good: every tuple that names it, and the name itself everywhere NodusDB keeps it.

```python
token = g.erase("user:alice")          # memory is clean when this returns
g.wait_erased(token, timeout=300)      # the directory, the bucket and the Iceberg table are clean
print(g.stats()["erasure"])            # per layer: configured, covered_lsn, pending
```

The name is replaced by a pseudonym, `erased:` and the first 16 hex digits of a salted SHA-256 of the name. The salt is stored in the graph's snapshot. The erasure is a transaction in the log, so every follower applies it too. A name that does not exist returns the current token. A type or relation name used by the schema is refused with `NodusSchemaError`, and an integer-keyed graph or a follower raises `NodusUnsupportedError`. The same name written later is a new subject that starts with no grants.

A subject with many tuples is removed in transactions of at most 30,000 tuples, followed by the erasure itself. If the process dies in between, the name is still there with some tuples gone, and calling `erase` again finishes the job. Erasing a subject with 1,000 grants took 2.8 ms on the development laptop, and one with 100,000 grants took 51 ms (`EraseBench`).

**The copies on disk** are scrubbed in the background:

- **The graph directory.** A checkpoint writes the erased symbol as its pseudonym, and the log segments before it are deleted. Erasures are collected for `erasure_checkpoint_delay_s` (60 seconds by default, set on `Shipping`) so a burst shares one checkpoint. `wait_erased` checkpoints at once.
- **The bucket.** Chain objects are signed and linked, so they are never edited. Once a snapshot taken after the erasure is in the bucket, the writer commits a signed `REDACTION` entry and deletes every chain object and snapshot from before that point. A transaction that erases a name always starts a new chain object, so nothing the bucket keeps still holds the name. Followers bootstrap from the newest snapshot as before. One that was behind the deleted range stops with "fell behind retention" and rebuilds on restart.
- **The Iceberg table.** The projector rewrites every data file that holds a row naming the erased subject or object, commits one overwrite snapshot with a signed `nodus.redaction.v1` record, expires every older snapshot, and deletes the replaced files and manifests. Time travel therefore reaches back only to the erasure. The rows stay, with the pseudonym in place of the name.

`wait_erased` raises `NodusErasurePendingError` (code -16) when the time runs out. It names the layers still pending and carries the token. The erasure itself is already done and the background work continues. After a restart or a takeover the writer compares what each layer covers with the number of erased symbols and finishes the rest. The tests check the result byte for byte: after `wait_erased`, no file in the graph directory, the bucket or the Iceberg table contains the erased name.

**What erasure cannot reach.** Copies made outside NodusDB are out of its hands: a backup or copy of the directory, a salvaged old directory, `pre-v2/` until `upgrade_cleanup()` runs, object versions or replicas kept by the bucket's own settings, and disk blocks the file system freed but did not overwrite. Anyone who can read a snapshot has the salt and can test a guessed name against a pseudonym, so a short or predictable name is only as private as the snapshot. Prefer opaque ids, such as UUIDs, for subjects. In the Iceberg table a name written again after its erasure keeps its new rows. After a restart the writer knows when such a name was erased only up to the snapshot it loaded, so new rows written before that snapshot can also show the pseudonym.

## Watching changes

**The problem.** Something should happen when the graph changes: a session should close when its user loses access, a cache should drop an entry, a job should start.

`changes(after)` returns what was committed after a token, in log order and in whole transactions, with the cursor to pass next. `watch()` does the same in a loop and yields each batch.

```python
cursor = g.token                                   # remember where you are
batch = g.changes(cursor, timeout=1.0)             # waits up to a second for the next commit
for change in batch.changes:
    print(change.kind, change.object, change.relation, change.subject, change.token)
cursor = batch.next                                # store it, and resume from it after a restart

for batch in g.watch(cursor):                      # the same, forever, until the graph closes
    ...
```

A change is an `add`, a `remove`, a `schema` (with its version), an `epoch` (a writer started or took over) or an `erase` (with the pseudonym). `change.token` is the token its write returned. Names are looked up when the batch is read, so an erased name always reads as its pseudonym, also in changes from before the erasure. A change inside a transaction of several writes can name a tuple that was already there: the feed is the log, as written.

`AccessWatch` turns the feed into gained and lost access for checks you register:

```python
from nodusdb import AccessWatch

access = AccessWatch(follower_or_graph, [("document:plan", "view", "user:alice")])
for change in access.poll(timeout=1.0):           # an AccessChange per check whose answer flipped
    if not change.allowed:
        disconnect(change.subject, change.object)
```

Each check is answered once when the watch starts, and again after every batch, at that batch's token. It reports the net change per batch, so access granted and revoked inside one batch is not reported, and each poll costs one check per registered triple.

**Where old cursors are read from.** Recent changes come from a ring in memory (`Graph(watch_buffer_changes=...)`, 1,048,576 changes and 24 MiB by default), which is created on the first watch call and is fed by every write and, on a follower, by every applied transaction. A cursor older than the ring is read from the writer's local log and then from the bucket's signed chain, which a follower uses too. A reader that is catching up stays open between calls, so a long catch-up does not search again on every call. When no source holds the changes after a cursor, the call raises `NodusHistoryExpiredError` (code -17); read the current state and start again from `g.token`. A cursor naming a write that was lost when the writer restarted raises `NodusTokenLostError`. A clean close checkpoints and trims the log, so a graph without shipping keeps no watch history across a restart.

**Cost.** On an in-memory kernel, a commit with a watcher attached takes 7.7% longer than one on the previous release, about 19 ns. Reading a change costs 126 ns from the ring and 1.78 µs from the log (`bench/baseline/watch-windows-dev.txt`).

## Time travel

**The problem.** Someone asks who could see a document last Tuesday, or you want to replay a decision with the permissions as they were.

`as_of` opens a read-only copy of the graph as it stood at a token or at a moment:

```python
from datetime import datetime, timedelta, timezone

with g.as_of(datetime.now(timezone.utc) - timedelta(hours=3)) as then:
    print(then.token, then.commit_time)            # the last commit at or before that moment
    print(then.check("document:plan", "view", "user:alice"))
    then.advance(datetime.now(timezone.utc) - timedelta(hours=1))   # forward only

with g.as_of(token_of_a_write) as then:            # exactly the state that write produced
    ...
```

The copy is rebuilt from the bucket: the newest snapshot at or before the target, then the chain replayed up to the target's last commit, all verified as a follower verifies them. It works on the writer and on followers, and a graph without shipping raises `NodusUnsupportedError`. On the writer the call first waits until its current writes are shipped, so an instant in the last second is exact; a follower answers from the chain it can read, and `then.token` says which commit it stands on. Every read works on it (`check`, `has_edge`, `khop`, `stats`), writes raise `NodusUnsupportedError`, and nested groups answer at full speed from the start, because the reachability index is rebuilt before the view is returned.

A view holds a full copy of the graph. `max_memory_mb` limits one view (by default the graph's own limit), and `history_max_memory_mb` on `Shipping` or `Follower` limits all open views of a graph together. A view that does not fit raises `NodusMemoryError` and leaves nothing behind. `stats()["history"]` shows the open views and their memory, and a view's own `stats()["as_of"]` shows its snapshot, what it replayed and how long the build took.

**What it costs.** The build replays every chain object since the newest snapshot before the target, so it is as fast as your checkpoints are frequent. On the development laptop a view with 10,000 grants builds in 95.6 ms from a snapshot, a view with 100,000 grants in 260 ms from a snapshot and in 373 ms by replay (`bench/baseline/watch-windows-dev.txt`). Checkpoint on a schedule if you use time travel often.

**How far back.** As far as the bucket keeps history (7 days by default, `retention_days`), and never before the newest erasure, because an erasure deletes the bucket history before it. A target outside that range raises `NodusHistoryExpiredError`. A view also forgets a name erased after it was opened, and it refuses to move forward past such an erasure. The Iceberg table keeps every row, with erased names as pseudonyms, for audits that need to reach further back with SQL.

## Limits worth knowing

- **One writer.** The kernel allows one writer and many readers. Writes on one handle run one at a time.
- **Failover is manual.** A follower, a restore and a takeover read the chain back, but nothing decides by itself that the writer is dead. A directory that was written without shipping while a chain already existed, and has trimmed those records, is refused when shipping is turned back on.
- **Checkpoints pause writes.** See [Durability](#durability).
- **Transactions are bounded.** A transaction must fit one write of 1 MiB. Split larger loads.
- **The chain and Iceberg formats are not covered by the 0.1.x promise yet.** They carry their own format versions.
- **Watching is per transaction, not per permission.** `AccessWatch` re-checks the triples it is given. Listing every object a subject gained or lost access to needs lookup APIs that are not built yet.
